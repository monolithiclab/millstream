package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

const e2ePipeline = `
pipeline: listings
vars: { count: 3 }
triggers:
  adhoc: { manual: {} }
steps:
  crawl:
    action: ./actions/fake
    in: trigger.adhoc
    out: [listings]
    config: { mode: keyed, n: "${vars.count}" }
  score:
    action: ./actions/fake
    in: listings
    concurrency: 2
    config: { mode: echo }
`

// TestEndToEnd runs a pipeline of the real fake action through a real node, on the real clocks, until the run
// succeeds.
func TestEndToEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "actions", "fake"), 0o750); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"pipeline.yaml":            e2ePipeline,
		"actions/fake/action.yaml": "name: fake\nversion: 1.0.0\nkind: exec\nruntime: { exec: { command: [\"./fakeaction\"] } }\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	linkFakeAction(t, dir, "actions/fake/fakeaction")
	pkg, err := pipeline.Load(dir)
	if err != nil {
		t.Fatal(err)
	}

	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	logs := &syncBuffer{}
	e, err := New(Options{
		Store: st, NoCron: true, TickInterval: 20 * time.Millisecond, Logger: newLogger(logs),
		Env: os.LookupEnv,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := e.Apply(ctx, pkg); err != nil {
		t.Fatal(err)
	}
	runID, _, err := e.Fire(ctx, "listings", "adhoc", "e2e", nil)
	if err != nil {
		t.Fatal(err)
	}

	pool := exec.NewPool(exec.PoolOptions{Options: exec.Options{ShutdownTimeout: 300 * time.Millisecond}})
	t.Cleanup(func() { _ = pool.Close() })
	n, err := node.New(e, node.Options{Owner: "node-a", Slots: 4, Pool: pool, PullInterval: 20 * time.Millisecond,
		Logger: newLogger(logs)})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	engineDone, nodeDone := make(chan error, 1), make(chan error, 1)
	go func() { engineDone <- e.Run(runCtx) }()
	go func() { nodeDone <- n.Run(runCtx) }()

	if err := e.WaitSettled(ctx, []RunRef{{Pipeline: "listings", ID: runID}}); err != nil {
		t.Fatalf("WaitSettled: %v\nlogs:\n%s", err, logs.String())
	}
	stop()
	if err := <-nodeDone; err != nil {
		t.Fatalf("node: %v", err)
	}
	if err := <-engineDone; err != nil {
		t.Fatalf("engine: %v", err)
	}

	r, err := st.Run(ctx, "listings", runID)
	if err != nil || r.State != store.RunSucceeded {
		t.Fatalf("run = %+v, %v\nlogs:\n%s", r, err, logs.String())
	}
	items, err := st.Items(ctx, store.ItemFilter{Pipeline: "listings", Stream: "listings"})
	if err != nil || len(items) != 3 {
		t.Fatalf("listings = %d items, %v", len(items), err)
	}
	for _, line := range logs.lines(t) {
		if line["msg"] == "delivery done" {
			for _, k := range []string{"pipeline", "runId", "step", "itemId", "deliveryId", "attempt"} {
				if _, ok := line[k]; !ok {
					t.Fatalf("log line without %s: %v", k, line)
				}
			}
		}
	}
	if strings.Contains(logs.String(), `"in":`) {
		t.Fatal("a payload reached the logs")
	}
}

func TestWaitSettledHonoursContext(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(writePackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc }\n"), "a"))
	id := v.fire("p", "adhoc", "k")
	ctx, cancel := context.WithCancel(v.ctx)
	cancel()
	if err := v.e.WaitSettled(ctx, []RunRef{{Pipeline: "p", ID: id}}); err == nil {
		t.Fatal("WaitSettled with a cancelled context succeeded")
	}
	if _, err := v.e.Settled(v.ctx, []RunRef{{Pipeline: "p", ID: "nope"}}); err == nil {
		t.Fatal("Settled of an unknown run succeeded")
	}
}

// TestRunExpiresLeases runs the engine loop: a lease that expires by the store clock returns to pending and is
// leased again.
func TestRunExpiresLeases(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{LeaseTTL: time.Minute, TickInterval: 5 * time.Millisecond})
	v.apply(writePackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc }\n"), "a"))
	id := v.fire("p", "adhoc", "k")
	if got := v.pull(1); len(got) != 1 {
		t.Fatalf("pull = %v", got)
	}
	ctx, cancel := context.WithCancel(v.ctx)
	done := make(chan error, 1)
	go func() { done <- v.e.Run(ctx) }()
	v.clock.Advance(2 * time.Minute)
	deadline := time.Now().Add(10 * time.Second)
	for v.deliveries(id)[0].State != store.DeliveryPending {
		if time.Now().After(deadline) {
			t.Fatal("the lease did not expire")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := v.pull(1); len(got) != 1 {
		t.Fatalf("pull after expiry = %v", got)
	}
}

func TestRunReturnsWhenCancelledBeforeStart(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	ctx, cancel := context.WithCancel(v.ctx)
	cancel()
	if err := v.e.Run(ctx); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if err := v.st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.e.Run(v.ctx); err == nil {
		t.Fatal("Run on a closed store succeeded")
	}
}
