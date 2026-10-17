package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	osexec "os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/nodeapi"
	"github.com/monolithiclab/millstream/internal/nodewire"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// NodeTokenEnv holds the deployment's node token a remote node joins with (node.token on the engines).
const NodeTokenEnv = "MILLSTREAM_NODE_TOKEN" // #nosec G101 -- the variable's name, not a credential

// NodeCmd is "millstream node": "node --join <url>" runs a remote node, "node drain" drains the one running here.
type NodeCmd struct {
	Run   NodeRunCmd   `cmd:"" default:"withargs" hidden:"" help:"Join the engine with the node token and run deliveries."`
	Drain NodeDrainCmd `cmd:"" help:"Drain the node running from the state directory: it stops pulling, finishes or releases its deliveries within its drain timeout, and exits."`
}

// NodeRunCmd is "millstream node --join <url>[,<url>…]" (spec section 5.10).
type NodeRunCmd struct {
	Join         []string          `required:"" placeholder:"URL[,URL…]" help:"The engines' URLs, the active one and its standbys: https, or http to a loopback engine. The node follows the active one across a failover."`
	Labels       map[string]string `mapsep:"," placeholder:"K=V[,K=V…]" help:"Labels steps place their deliveries by (placement.labels)."`
	AllowExec    bool              `name:"allow-exec" help:"Run exec actions, and unsandboxed agents, as children with this node's privileges: trusted code only (spec section 15.3)."`
	Slots        int               `default:"4" help:"Deliveries the node runs at once."`
	CA           string            `name:"ca" type:"existingfile" placeholder:"FILE" help:"PEM bundle of the certificate authority the engine's certificate is checked against, besides the system's."`
	DrainTimeout time.Duration     `name:"drain-timeout" default:"30s" help:"On drain or SIGTERM, how long in-flight deliveries may run before the node releases them."`
	StateDir     string            `name:"state-dir" type:"path" default:".millstream/node" help:"The node's id, its package cache and its pid file."`
}

// Files of the state directory.
const (
	nodeIDFile  = "node-id"
	nodePIDFile = "node.pid"
)

// Run joins the engine and runs deliveries until drained (node drain, SIGTERM, SIGINT), revoked, or refused its
// token. It registers its id, version, labels and capacity: exec with --allow-exec, container when the runtime the
// environment file selects is installed, and the agent harnesses it can run (agents key). Logs are JSON lines on
// stderr, relayed to the engine's log and live tail.
func (c NodeRunCmd) Run(rt *Runtime) error {
	ctx, stop := signalContext(rt.Ctx)
	defer stop()
	env, err := environment(rt)
	if err != nil {
		return err
	}
	token, _ := rt.Env(NodeTokenEnv)
	if token == "" {
		return usageError("node: set " + NodeTokenEnv + " to the deployment's node token (node.token)")
	}
	if err := pipeline.CheckToken(NodeTokenEnv, token); err != nil {
		return usageError(err.Error())
	}
	if c.Slots <= 0 || c.DrainTimeout <= 0 {
		return usageError("--slots and --drain-timeout must be positive")
	}
	for k := range c.Labels {
		if k == "" {
			return usageError("--labels: want k=v pairs")
		}
	}
	client := rt.HTTPClient // tests reach an engine in memory
	if client == nil {
		var err error
		if client, err = c.httpClient(); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(c.StateDir, 0o700); err != nil {
		return err
	}
	id, err := nodeID(c.StateDir)
	if err != nil {
		return err
	}
	release, err := lockStateDir(c.StateDir)
	if err != nil {
		return err
	}
	defer release()

	launch := launchers(rt, env)
	ag := agents(rt, env).Agents
	if !c.AllowExec {
		launch = maps.Clone(launch)
		launch[pipeline.KindExec] = nil // refused by the runtime too, whatever the engine sends
		ag.AllowUnsandboxed = false
	}
	kinds := slices.DeleteFunc(sortedKeys(launch), func(k string) bool { return launch[k] == nil })
	runtimes := node.Runtimes(kinds, ag, func(p string) bool {
		_, err := osexec.LookPath(p)
		return err == nil
	})
	workRoot, tmpfs := nodeapi.WorkRoot(rt.Env)
	base := slog.NewJSONHandler(rt.IO.Err, nil)
	t, err := nodeapi.NewClient(nodeapi.ClientOptions{
		Join: c.Join, Token: token, HTTP: client, StateDir: c.StateDir, TmpDir: workRoot, Env: rt.Env,
		Agents: ag, Logger: slog.New(base).With("node", id),
		Node: nodewire.Register{Node: id, Version: rt.Version, Labels: c.Labels, Runtimes: runtimes, Slots: c.Slots},
	})
	if err != nil {
		return usageError(err.Error())
	}
	log := slog.New(t.LogHandler(base))
	if !tmpfs {
		log.Warn("no tmpfs for the deliveries' working directories: they are created 0700 in the temporary "+
			"directory and removed after each process", "dir", workRoot)
	}
	reg, err := t.Register(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	ttl := time.Duration(reg.LeaseTTLMs) * time.Millisecond
	pool := exec.NewPool(exec.PoolOptions{Options: exec.Options{Logger: log, Launchers: launch, WorkRoot: workRoot}})
	defer func() { _ = pool.Close() }()
	n, err := node.New(t, node.Options{
		Owner: id, Slots: c.Slots, Pool: pool, Logger: log, DrainTimeout: c.DrainTimeout, IdleHeartbeat: true,
		HeartbeatInterval: t.HeartbeatInterval(), Heartbeat: t.HeartbeatInterval, ReportTimeout: max(2*ttl, time.Minute),
	})
	if err != nil {
		return err
	}
	log.Info("node joined", "runtimes", runtimes, "labels", c.Labels, "slots", c.Slots, "engine", reg.Engine,
		"workDir", workRoot)
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	relayCtx, stopRelay := context.WithCancel(context.WithoutCancel(ctx))
	relayed := make(chan struct{})
	go func() {
		defer close(relayed)
		t.RunRelay(relayCtx)
	}()
	go func() {
		select {
		case <-t.Fatal():
			cancel(node.ErrAbandon) // revoked or refused: nothing it holds can be reported
		case <-runCtx.Done():
		}
	}()
	runErr := n.Run(runCtx)
	if fatal := t.Err(); fatal != nil {
		log.Error("node stopped", "error", fatal.Error())
		stopRelay()
		<-relayed
		return fatal
	}
	// Released: what the drain timeout abandoned ends at once rather than when its lease expires.
	leave, cancelLeave := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	if err := t.Leave(leave); err != nil {
		log.Warn("leave", "error", err.Error())
	}
	cancelLeave()
	log.Info("node drained")
	stopRelay()
	<-relayed
	return runErr
}

// httpClient is the node's client: HTTP/2 over TLS, checking the engine's certificate against the system's
// authorities and --ca's, with pings that detect a dead connection.
func (c NodeRunCmd) httpClient() (*http.Client, error) {
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if c.CA != "" {
		pem, err := os.ReadFile(c.CA)
		if err != nil {
			return nil, fmt.Errorf("--ca: %w", err)
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, usageError("--ca " + c.CA + ": no PEM certificate in it")
		}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	tr.ForceAttemptHTTP2 = true
	tr.HTTP2 = &http.HTTP2Config{SendPingTimeout: 5 * time.Second, PingTimeout: 5 * time.Second}
	return &http.Client{Transport: tr}, nil
}

// nodeID returns the node's id from the state directory, made the first time from the host name and a random
// suffix: the id outlives restarts, so a revocation does too.
func nodeID(dir string) (string, error) {
	path := filepath.Join(dir, nodeIDFile)
	b, err := os.ReadFile(path) // #nosec G304 -- the operator's state directory
	if err == nil && strings.TrimSpace(string(b)) != "" {
		return strings.TrimSpace(string(b)), nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	id := newEngineID()
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", err
	}
	return id, nil
}

// lockStateDir records this process's pid in the state directory, refusing to start while another live node runs
// from it, and returns what removes the record.
func lockStateDir(dir string) (func(), error) {
	path := filepath.Join(dir, nodePIDFile)
	if pid, ok := runningNode(dir); ok {
		return nil, fmt.Errorf("node: another node (pid %d) runs from %s", pid, dir)
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		return nil, err
	}
	return func() { _ = os.Remove(path) }, nil
}

// signalProcess sends sig to the process pid: tests replace it.
var signalProcess = func(pid int, sig os.Signal) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Signal(sig)
}

// runningNode returns the pid of the node running from the state directory, if one does.
func runningNode(dir string) (int, bool) {
	b, err := os.ReadFile(filepath.Join(dir, nodePIDFile)) // #nosec G304 -- the operator's state directory
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0, false
	}
	if signalProcess(pid, syscall.Signal(0)) != nil {
		return 0, false
	}
	return pid, true
}

// NodeDrainCmd is "millstream node drain".
type NodeDrainCmd struct {
	StateDir string        `name:"state-dir" type:"path" default:".millstream/node" help:"The state directory of the node to drain."`
	Wait     time.Duration `default:"2m" help:"How long to wait for the node to exit."`
}

// Run sends SIGTERM to the node running from the state directory and waits for it to exit: it stops pulling,
// finishes its deliveries within its drain timeout, releases what remains and exits, for a rolling upgrade.
func (c NodeDrainCmd) Run(rt *Runtime) error {
	pid, ok := runningNode(c.StateDir)
	if !ok {
		return fmt.Errorf("node drain: no node runs from %s", c.StateDir)
	}
	if err := signalProcess(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("node drain: signal pid %d: %w", pid, err)
	}
	fmt.Fprintf(rt.IO.Out, "draining node %d\n", pid)
	deadline := time.Now().Add(c.Wait)
	for time.Now().Before(deadline) {
		if _, ok := runningNode(c.StateDir); !ok {
			_, err := fmt.Fprintln(rt.IO.Out, "drained")
			return err
		}
		select {
		case <-rt.Ctx.Done():
			return rt.Ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("node drain: node %d still runs after %s", pid, c.Wait)
}
