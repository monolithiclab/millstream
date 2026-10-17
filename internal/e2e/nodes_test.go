//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	osexec "os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/nodewire"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
)

// The node chaos suite (spec sections 5.10 and 16): serve processes on one Postgres database without an embedded
// node, and remote node processes joining them with the node token, run the builtins fixture and its importer as
// the failover suite does. Nodes are killed mid-delivery, cut off by a proxy for longer than the lease, revoked;
// the token is rotated; the active engine fails over while nodes hold leases. Each time the store must end exactly
// as the clean run's, with the crash suite's invariants.

// withNodeToken is an engine's node.token variable.
const withNodeToken = "MILLSTREAM_NODE_TOKEN=" + nodeToken

const (
	nodeToken    = "node-token-0123456789abcdef-01234"
	slowCrawlVar = "MILLSTREAM_VAR_CRAWL_DELAY_MS=3000"
	rotatedToken = "rotated-token-0123456789abcdef-01"
	nodeJoined   = `"msg":"node joined"`
)

// newNodeDeployment is a failover deployment whose engines run no embedded node and serve remote nodes.
func newNodeDeployment(t *testing.T) *failover {
	t.Helper()
	f := newFailover(t)
	if err := os.WriteFile(f.envFile, []byte("store: { postgres: { dsn: { env: MILLSTREAM_DSN } } }\n"+
		"listen: 127.0.0.1:0\nauth: { token: { env: MILLSTREAM_TOKEN } }\n"+failoverEngine+
		"node: { embedded: false, token: { env: MILLSTREAM_NODE_TOKEN } }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

// nodeProc is a node process.
type nodeProc struct {
	*engineProc
	id string
}

// startNode starts a node joining the engines at join with token, and waits until it joined.
func (f *failover) startNode(name, token string, join ...string) *nodeProc {
	f.t.Helper()
	return f.runNode(name, token, nil, join...)
}

// startNodeCA starts a node trusting the CA at ca, joining the engines at join.
func (f *failover) startNodeCA(name, ca string, join ...string) *nodeProc {
	f.t.Helper()
	return f.runNode(name, nodeToken, []string{"--ca", ca}, join...)
}

func (f *failover) runNode(name, token string, args []string, join ...string) *nodeProc {
	f.t.Helper()
	state := filepath.Join(f.t.TempDir(), "node")
	p := &engineProc{name: "node " + name, log: newProcLog(), done: make(chan struct{})}
	p.cmd = osexec.Command(binary, append([]string{"node", "--join", strings.Join(join, ","), "--allow-exec", // #nosec G204 -- test binary
		"--slots", "4", "--state-dir", state, "--drain-timeout", "5s"}, args...)...)
	p.cmd.Env, p.cmd.Dir = f.environ(f.db, "MILLSTREAM_NODE_TOKEN="+token), f.t.TempDir()
	p.cmd.Stdout, p.cmd.Stderr = p.log, p.log
	p.cmd.WaitDelay = 10 * time.Second
	if err := p.cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	go func() {
		err := p.cmd.Wait()
		p.code = exitCode(f.t, err)
		close(p.done)
	}()
	f.t.Cleanup(func() {
		_ = p.cmd.Process.Kill()
		<-p.done
		if f.t.Failed() {
			f.t.Logf("%s:\n%s", p.name, lastLines(p.log.String(), 200))
		}
	})
	p.waitLine(f.t, nodeJoined, time.Minute)
	id, err := os.ReadFile(filepath.Join(state, "node-id"))
	if err != nil {
		f.t.Fatal(err)
	}
	return &nodeProc{engineProc: p, id: strings.TrimSpace(string(id))}
}

// waitExit waits until the process exited on its own, and returns its exit code.
func (p *engineProc) waitExit(t *testing.T, timeout time.Duration) int {
	t.Helper()
	select {
	case <-p.done:
		return p.code
	case <-time.After(timeout):
		t.Fatalf("%s still runs after %s\n%s", p.name, timeout, lastLines(p.log.String(), 20))
		return 0
	}
}

// TestNodesKilled kills nodes mid-delivery: the first while it runs the crawl, the next once it has done some work.
// Their leases expire, each consuming its attempt, and another node redoes the deliveries.
func TestNodesKilled(t *testing.T) {
	t.Parallel()
	want := cleanBuiltinsRunOn(t, postgresBackend)
	f := newNodeDeployment(t)
	a := f.startEngine("a", f.db, slowCrawlVar, withNodeToken)
	a.waitLine(t, `"msg":"leading the deployment"`, time.Minute)
	n1 := f.startNode("1", nodeToken, a.url)
	f.fireRun([]*engineProc{a})
	a.waitLine(t, `"msg":"first dispatch"`, time.Minute)
	n1.kill() // mid-crawl
	n2 := f.startNode("2", nodeToken, a.url)
	a.waitLine(t, `"msg":"lease expired"`, time.Minute)
	a.waitDone(t, 5)
	n2.kill()
	f.startNode("3", nodeToken, a.url)
	f.waitSettled()
	a.stop(t)
	if n := a.log.count(`"msg":"lease expired"`); n < 2 {
		t.Errorf("%d leases expired, want one per killed node at least", n)
	}
	wantSame(t, readSnapshot(t, f.db), want)
}

// TestNodePartitioned cuts a node off from the engine by a proxy for longer than its lease while it runs the crawl
// (Review focus 2): the crawl goes to another node; healed, the cut-off node's result is refused by fencing and it
// carries on. Its heartbeat, on its own goroutine, may learn the lease is lost before its buffered result is sent:
// the node then abandons the delivery instead of reporting it stale, the same refusal.
func TestNodePartitioned(t *testing.T) {
	t.Parallel()
	want := cleanBuiltinsRunOn(t, postgresBackend)
	f := newNodeDeployment(t)
	a := f.startEngine("a", f.db, slowCrawlVar, withNodeToken)
	a.waitLine(t, `"msg":"leading the deployment"`, time.Minute)
	proxy := newCutProxy(t, strings.TrimPrefix(a.url, "http://"))
	n1 := f.startNode("1", nodeToken, "http://"+proxy.addr())
	f.fireRun([]*engineProc{a})
	n1.waitLine(t, `"msg":"delivery started"`, time.Minute) // the crawl
	proxy.partition()
	f.startNode("2", nodeToken, a.url)
	a.waitLine(t, `"msg":"lease expired"`, 30*time.Second)
	proxy.heal()
	refused := func(l logLine) bool {
		return strings.Contains(l.text, "stale lease, result dropped") ||
			strings.Contains(l.text, "delivery abandoned, result dropped")
	}
	if _, ok := n1.log.wait(refused, 2*time.Minute, n1.done); !ok {
		t.Fatalf("node 1's result was never refused by fencing\n%s", lastLines(n1.log.String(), 20))
	}
	f.waitSettled()
	if !n1.alive() {
		t.Errorf("the partitioned node exited (%d) instead of carrying on", n1.code)
	}
	a.stop(t)
	wantSame(t, readSnapshot(t, f.db), want)
}

// TestNodeRevoked revokes a node while it holds the crawl's lease: the lease ends at once (an attempt consumed), the
// node is refused and exits, and another node finishes the run.
func TestNodeRevoked(t *testing.T) {
	t.Parallel()
	want := cleanBuiltinsRunOn(t, postgresBackend)
	f := newNodeDeployment(t)
	a := f.startEngine("a", f.db, slowCrawlVar, withNodeToken)
	a.waitLine(t, `"msg":"leading the deployment"`, time.Minute)
	n1 := f.startNode("1", nodeToken, a.url)
	f.fireRun([]*engineProc{a})
	a.waitLine(t, `"msg":"first dispatch"`, time.Minute)
	if r := f.client(a.url, "nodes", "revoke", n1.id); r.code != 0 || r.stdout != n1.id+" revoked\n" {
		t.Fatalf("revoke: %s", r)
	}
	if code := n1.waitExit(t, time.Minute); code == 0 || n1.log.count("this node was revoked") == 0 {
		t.Errorf("the revoked node exited with %d", code)
	}
	f.startNode("2", nodeToken, a.url)
	f.waitSettled()
	if r := f.client(a.url, "nodes", "list"); !strings.Contains(r.stdout, n1.id) || !strings.Contains(r.stdout, "revoked") {
		t.Errorf("nodes list: %s", r)
	}
	a.stop(t)
	wantSame(t, readSnapshot(t, f.db), want)
}

// TestNodeTokenRotated restarts the engine with a new node token, on the same address, while nodes run: they are
// refused and stop, their leases expiring; a node given the new token finishes the run.
func TestNodeTokenRotated(t *testing.T) {
	t.Parallel()
	want := cleanBuiltinsRunOn(t, postgresBackend)
	f := newNodeDeployment(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.serveArgs = []string{"--listen", ln.Addr().String()}
	_ = ln.Close()
	a := f.startEngine("a", f.db, withNodeToken)
	a.waitLine(t, `"msg":"leading the deployment"`, time.Minute)
	n1 := f.startNode("1", nodeToken, a.url)
	f.fireRun([]*engineProc{a})
	a.waitDone(t, 5)
	a.stop(t)
	b := f.startEngine("b", f.db, "MILLSTREAM_NODE_TOKEN="+rotatedToken)
	b.waitLine(t, `"msg":"leading the deployment"`, time.Minute)
	if code := n1.waitExit(t, time.Minute); code == 0 || n1.log.count("refused the node token") == 0 {
		t.Errorf("the node with the old token exited with %d", code)
	}
	f.startNode("2", rotatedToken, b.url)
	f.waitSettled()
	b.stop(t)
	wantSame(t, readSnapshot(t, f.db), want)
}

// TestNodesFollowFailover kills the active engine while the nodes hold leases (Review focus 1): the standby takes
// over, the nodes register with it, renew their leases and resend what they buffered; fencing keeps it safe.
func TestNodesFollowFailover(t *testing.T) {
	t.Parallel()
	want := cleanBuiltinsRunOn(t, postgresBackend)
	f := newNodeDeployment(t)
	a := f.startEngine("a", f.db, slowCrawlVar, withNodeToken)
	a.waitLine(t, `"msg":"leading the deployment"`, time.Minute)
	b := f.startEngine("b", f.db, slowCrawlVar, withNodeToken)
	b.waitLine(t, "waiting as the standby", time.Minute)
	// The second node joins through the standby, which names the active engine.
	n1 := f.startNode("1", nodeToken, a.url, b.url)
	n2 := f.startNode("2", nodeToken, b.url, a.url)
	f.fireRun([]*engineProc{a, b})
	a.waitDone(t, 8)
	a.kill()
	tk := measureTakeover(t, b, a.end, 30*time.Second)
	f.waitSettled()
	refuseStaleDone(t, f, b, n1.id)
	b.stop(t)
	for _, n := range []*nodeProc{n1, n2} {
		if c := n.log.count(`"msg":"registered"`); c < 2 {
			t.Errorf("%s registered %d times, want again with the new active engine", n.name, c)
		}
	}
	t.Logf("failover with remote nodes: standby took the lock after %s, dispatched after %s",
		tk.lead.Round(time.Millisecond), tk.dispatch.Round(time.Millisecond))
	// Every result reaches the store: a result held across the takeover is accepted under its lease, or, when the
	// 1 s lease expired meanwhile, refused and redone; either way the store ends as the clean run's.
	wantSame(t, readSnapshot(t, f.db), want)
}

// refuseStaleDone sends the active engine e, after a takeover, a Done from node under the lease of a delivery
// already completed: it is refused stale, never applied twice.
func refuseStaleDone(t *testing.T, f *failover, e *engineProc, node string) {
	t.Helper()
	st, err := postgres.OpenReadOnly(t.Context(), f.db, postgres.Options{}) // e holds the lock
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	ds, err := st.Deliveries(t.Context(), store.DeliveryFilter{Pipeline: pipelineName, State: store.DeliveryDone})
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(ds, func(d store.Delivery) bool { return d.FencingToken > 0 }) // leased, not a builtin's
	if i < 0 {
		t.Fatal("no delivery was completed under a lease")
	}
	d := ds[i]
	body, _ := json.Marshal(nodewire.Done{Node: node, Attempt: d.Attempt, Lease: nodewire.FromRef(store.LeaseRef{
		Pipeline: d.Pipeline, DeliveryID: d.ID, Step: d.Step, FencingToken: d.FencingToken})})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, e.url+nodewire.PathDone, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+nodeToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusConflict || !strings.Contains(string(got), nodewire.CodeStale) {
		t.Fatalf("a stale done across the takeover: %d %s", resp.StatusCode, got)
	}
}
