package nodeapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/nodeapi"
	"github.com/monolithiclab/millstream/internal/nodewire"
	"github.com/monolithiclab/millstream/internal/store"
)

func TestRemoteNode(t *testing.T) {
	d := newDeployment(t)
	n1 := d.transport("n1", "http://127.0.0.1:1", "https://a:7070") // the first engine is down: the next is tried
	run, _, err := d.e.Fire(d.ctx, "remote", "go", "k1", json.RawMessage(`{"mode":"artifact"}`))
	if err != nil {
		t.Fatal(err)
	}

	// The write step: the package is fetched for the lease, its exec action resolved in the node's cache.
	w := d.pull(n1, "n1")
	if w.Lease.Step != "write" || w.Spec.Dir == "" || !strings.HasSuffix(w.Spec.Command[0], "/actions/fake/run") ||
		strings.HasPrefix(w.Spec.Command[0], d.pkg.Dir) {
		t.Fatalf("assignment = %+v; want the action resolved in the node's own cache", w.Spec)
	}
	ref, err := n1.PutArtifact(d.ctx, w.Lease.Ref(), "report.txt", strings.NewReader("margin 18%"))
	if err != nil || ref.Size != 10 || ref.Type != "text/plain; charset=utf-8" {
		t.Fatalf("put artifact = %+v, %v", ref, err)
	}
	if again, err := n1.PutArtifact(d.ctx, w.Lease.Ref(), "copy.txt", strings.NewReader("margin 18%")); err != nil ||
		again.SHA256 != ref.SHA256 {
		t.Fatalf("put the same content again = %+v, %v", again, err)
	}
	if err := n1.Done(d.ctx, store.DoneRequest{LeaseRef: w.Lease.Ref(), Attempt: w.Lease.Attempt, Outputs: []store.Output{
		{Key: "p1", Payload: json.RawMessage(`{"mode":"artifact"}`), Artifacts: []store.ArtifactRef{ref}},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := n1.Done(d.ctx, store.DoneRequest{LeaseRef: w.Lease.Ref(), Attempt: w.Lease.Attempt}); !errors.Is(err, store.ErrStaleLease) {
		t.Fatalf("a second done: %v", err)
	}

	// The read step: its input artifact through the engine's proxy, for the lease only.
	r := d.pull(n1, "n1")
	if r.Lease.Step != "read" || len(r.Lease.Item.Artifacts) != 1 {
		t.Fatalf("read assignment = %+v", r.Lease)
	}
	n2 := d.transport("n2", "https://a:7070")
	if _, err := n2.OpenArtifact(d.ctx, r.Lease.Ref(), ref); err == nil || !strings.Contains(err.Error(), "holds no such lease") {
		t.Fatalf("another node opened the input: %v", err)
	}
	if err := n2.Done(d.ctx, store.DoneRequest{LeaseRef: r.Lease.Ref(), Attempt: r.Lease.Attempt}); !errors.Is(err, store.ErrStaleLease) {
		t.Fatalf("another node completed the delivery: %v", err)
	}
	f, err := n1.OpenArtifact(d.ctx, r.Lease.Ref(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := io.ReadAll(f); string(b) != "margin 18%" {
		t.Fatalf("input = %q", b)
	}
	_ = f.Close()

	// A failover: the new active engine does not know the node, which registers again; its lease still holds, and
	// the result it buffered is accepted under its fencing token.
	d.serve("a:7070", d.engine("engine-b"))
	if lost, err := n1.Heartbeat(d.ctx, "n1", []store.LeaseRef{r.Lease.Ref()}); err != nil || len(lost) != 0 {
		t.Fatalf("heartbeat after the failover = %v, %v", lost, err)
	}
	if reg := n1.Registered(); reg.Engine != "engine-b" {
		t.Fatalf("registered with %+v after the failover", reg)
	}
	if err := n1.Done(d.ctx, store.DoneRequest{LeaseRef: r.Lease.Ref(), Attempt: r.Lease.Attempt}); err != nil {
		t.Fatal(err)
	}
	if ok, err := d.e.Settled(d.ctx, []engine.RunRef{{Pipeline: "remote", ID: run}}); err != nil || !ok {
		t.Fatalf("settled = %v, %v", ok, err)
	}

	// A standby names the active engine, which the node follows.
	standby := nodeapi.NewStandby(token, func(context.Context) (string, string) { return "engine-b", "b:7070" })
	d.hosts.set("a:7070", standby)
	d.serve("b:7070", d.engine("engine-c"))
	if _, err := n1.Heartbeat(d.ctx, "n1", nil); err == nil {
		t.Fatal("a heartbeat to the standby succeeded")
	}
	if _, err := n1.Heartbeat(d.ctx, "n1", nil); err != nil {
		t.Fatalf("heartbeat after following the standby's answer: %v", err)
	}

	// Revoked, the node is refused for good.
	if _, err := d.st.RegisterNode(d.ctx, store.Node{ID: "n3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.st.SetNodeState(d.ctx, "n2", store.NodeRevoked, true); err != nil {
		t.Fatal(err)
	}
	n2.Heartbeat(d.ctx, "n2", nil) //nolint:errcheck // the standby's answer moves it on to b, which does not know it
	if _, err := n2.Heartbeat(d.ctx, "n2", nil); !errors.Is(err, nodeapi.ErrRevoked) {
		t.Fatalf("heartbeat of a revoked node: %v", err)
	}
	select {
	case <-n2.Fatal():
	default:
		t.Fatal("revocation is not fatal")
	}
}

// The relay sends the node's lines, with their attributes and groups, to the engine's log; a failure is reported
// under its lease; leaving ends what the node holds.
func TestRelayFailAndLeave(t *testing.T) {
	d := newDeployment(t)
	var relayed strings.Builder
	h, err := nodeapi.New(nodeapi.Options{Engine: d.e, Token: token, Artifacts: d.arts,
		Relay: slog.New(slog.NewTextHandler(&relayed, nil))})
	if err != nil {
		t.Fatal(err)
	}
	d.hosts.set("a:7070", h)
	n := d.transport("n1", "https://a:7070")
	var local strings.Builder
	log := slog.New(n.LogHandler(slog.NewTextHandler(&local, nil))).With("deliveryId", "dl_1").WithGroup("run")
	log.Warn("slow source", "attempt", 2, "since", time.Second, slog.Group("cost", "usd", 0.5))
	ctx, cancel := context.WithCancel(d.ctx)
	cancel()
	n.RunRelay(ctx) // ended: one last flush
	for _, want := range []string{"slow source", "nodeId=n1", "deliveryId=dl_1", "run.attempt=2", "run.since=1s",
		"run.cost.usd=0.5"} {
		if !strings.Contains(relayed.String(), want) {
			t.Errorf("relayed %q, want %s", relayed.String(), want)
		}
	}
	if !strings.Contains(local.String(), "slow source") {
		t.Errorf("local log %q", local.String())
	}

	if _, _, err := d.e.Fire(d.ctx, "remote", "go", "k1", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	a := d.pull(n, "n1")
	if err := n.Fail(d.ctx, store.FailRequest{LeaseRef: a.Lease.Ref(), Attempt: a.Lease.Attempt, Class: "action",
		Message: "source down", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	if err := n.Leave(d.ctx); err != nil {
		t.Fatal(err)
	}
	if ns, _ := d.st.Nodes(d.ctx); len(ns) != 1 || ns[0].State != store.NodeDraining {
		t.Fatalf("nodes after leaving = %+v", ns)
	}
	// The same process registering again, as with a new active engine after a failover, stays draining: it is
	// leased nothing; a new process for the id is active again.
	if _, err := n.Register(d.ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.e.Fire(d.ctx, "remote", "go", "k2", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if as, err := n.Pull(d.ctx, "n1", 1); err != nil || len(as) != 0 {
		t.Fatalf("the draining node registered again pulled %d, %v", len(as), err)
	}
	d.pull(d.transport("n1", "https://a:7070"), "n1") // a new process for the id: active, it leases k2
	if dir, _ := nodeapi.WorkRoot(func(string) (string, bool) { return shared(t, "tmp"), true }); dir == "" {
		t.Fatal("no work root")
	}
}

// Plain-text answers: an engine between terms (503) is left for the next one; one serving no remote nodes (404) is
// fatal at registration. Join URLs must be https, or http to a loopback host.
func TestAnswersAndURLs(t *testing.T) {
	d := newDeployment(t)
	between := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "the engine is starting", http.StatusServiceUnavailable)
	})
	d.hosts.set("b:7070", between)
	n := d.transport("n1", "https://b:7070", "https://a:7070")
	if got := n.HeartbeatInterval(); got != node.DefaultHeartbeatInterval {
		t.Errorf("heartbeat interval under the engine's lease: %s", got)
	}
	if got := node.HeartbeatFor(3 * time.Second); got != time.Second {
		t.Errorf("heartbeat interval under a 3 s lease: %s", got)
	}
	if _, err := n.Heartbeat(d.ctx, "n1", nil); err != nil {
		t.Fatalf("heartbeat after a 503: %v", err)
	}
	d.hosts.set("c:7070", http.NotFoundHandler())
	tr, err := nodeapi.NewClient(nodeapi.ClientOptions{Join: []string{"https://c:7070"}, Token: token,
		HTTP: &http.Client{Transport: d.hosts}, Node: nodewire.Register{Node: "n2", Slots: 1}, StateDir: shared(t, "state")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Register(d.ctx); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("register with an engine serving no nodes: %v", err)
	}
	if tr.Err() == nil {
		t.Fatal("not fatal")
	}
	for _, raw := range []string{"ftp://a:1", "https://", "http://example.com:7070", "%"} {
		if _, err := nodeapi.CheckURL(raw); err == nil {
			t.Errorf("CheckURL(%q) accepted", raw)
		}
	}
	for _, raw := range []string{"http://localhost:7070", "http://[::1]:7070", "https://e.example:7070/x?y"} {
		if u, err := nodeapi.CheckURL(raw); err != nil || u.Path != "" {
			t.Errorf("CheckURL(%q) = %v, %v", raw, u, err)
		}
	}
	for _, o := range []nodeapi.ClientOptions{{}, {Join: []string{"https://a:1"}}, {Join: []string{"https://a:1"}, Token: "t"}} {
		if _, err := nodeapi.NewClient(o); err == nil {
			t.Errorf("New(%+v) accepted", o)
		}
	}
}

// A delivery the node cannot prepare (its package cannot be cached here) is failed at once, retryable.
func TestUnpreparedDeliveryFails(t *testing.T) {
	d := newDeployment(t)
	file := filepath.Join(scratch(t), "state")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	tr, err := nodeapi.NewClient(nodeapi.ClientOptions{Join: []string{"https://a:7070"}, Token: token,
		HTTP: &http.Client{Transport: d.hosts}, Node: nodewire.Register{Node: "n1", Runtimes: []string{"exec"}, Slots: 1},
		StateDir: file, PullWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Register(d.ctx); err != nil {
		t.Fatal(err)
	}
	run, _, err := d.e.Fire(d.ctx, "remote", "go", "k1", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if as, err := tr.Pull(d.ctx, "n1", 1); err != nil || len(as) != 0 {
		t.Fatalf("pull = %v, %v", as, err)
	}
	ds, _ := d.st.Deliveries(d.ctx, store.DeliveryFilter{Pipeline: "remote", RunID: run})
	if len(ds) != 1 || ds[0].State != store.DeliveryPending || ds[0].ErrorClass != nodeapi.ClassTransport {
		t.Fatalf("deliveries = %+v", ds)
	}
}

func (d *deployment) pull(tr *nodeapi.Client, id string) node.Assignment {
	d.t.Helper()
	as, err := tr.Pull(d.ctx, id, 1)
	if err != nil || len(as) != 1 {
		d.t.Fatalf("pull: %d, %v", len(as), err)
	}
	return as[0]
}
