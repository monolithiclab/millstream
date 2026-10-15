//go:build e2e

// The engine's integration scenarios: a real node on the real clocks, the projector loop, and the twins of store
// behaviours (round-robin leasing, rate limits, the dedupe window, dedupe within a replay's generation) seen through the engine.
// The default build tests the engine's own decisions; these run in make test-e2e.

package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest/fakeexec"
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
		Store: st, NoCron: true, TickInterval: 5 * time.Millisecond, Logger: newLogger(logs),
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

	pool := fakeexec.NewPool(t, exec.Options{ShutdownTimeout: 300 * time.Millisecond})
	n, err := node.New(e, node.Options{Owner: "node-a", Slots: 4, Pool: pool, PullInterval: 5 * time.Millisecond,
		Logger: newLogger(logs)})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	engineDone, nodeDone := make(chan error, 1), make(chan error, 1)
	go func() { engineDone <- e.Run(runCtx) }()
	<-e.Ready()
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

// Run runs the projectors on their ticker and stops them before it returns.
func TestRunProjects(t *testing.T) {
	t.Parallel()
	v := stubbedEnv(t, Options{ProjectInterval: time.Millisecond})
	v.fire("p", "adhoc", "r1")
	v.pull(10)
	projected := make(chan struct{}, 1)
	v.e.projected = func() {
		select {
		case projected <- struct{}{}:
		default:
		}
	}
	ctx, cancel := context.WithCancel(v.ctx)
	done := make(chan error, 1)
	go func() { done <- v.e.Run(ctx) }()
	<-projected
	if len(v.insights()) == 0 {
		t.Fatal("the projectors ran without effect")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestReplayThroughDedupe is review focus 2: a replay of the step upstream of a dedupe dedupes only within its own
// generation, so items the original run let through pass again, and duplicates within the replay are filtered.
func TestReplayThroughDedupe(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(newPackage(t, builtinPipeline, "a"))
	id := v.fire("p", "adhoc", "r1")
	v.emit(output("a", `{"n":7}`), output("b", `{"n":7}`))
	v.pull(10)
	states := func(runID string) []store.DeliveryState {
		var out []store.DeliveryState
		for _, d := range v.byStep(runID, "unique") {
			out = append(out, d.State)
		}
		return out
	}
	want := []store.DeliveryState{store.DeliveryDone, store.DeliveryFiltered}
	if got := states(id); !slices.Equal(got, want) {
		t.Fatalf("original unique = %v", got)
	}
	for _, key := range []string{"rp1", "rp2"} {
		rp := v.replay(ReplayRequest{Step: "gate", RunID: id, RequestID: key})
		got := v.pull(10)
		if got := states(rp); !slices.Equal(got, want) {
			t.Fatalf("replay %s unique = %v, want %v", key, got, want)
		}
		if !slices.Equal(got, []string{"p/sink"}) {
			t.Fatalf("replay %s pulled %v, want its one sink delivery", key, got)
		}
		if d := v.byStep(rp, "sink")[0]; d.Generation != v.run("p", rp).Generation || d.Generation == 0 {
			t.Fatalf("sink delivery of replay %s in generation %d", key, d.Generation)
		}
	}
}

// linkFakeAction places the fake action in the package directory dir at rel: a stub the in-process launcher
// ignores in the default build, the built binary in the e2e build.
func linkFakeAction(t *testing.T, dir, rel string) {
	t.Helper()
	if err := fakeexec.Place(filepath.Join(dir, filepath.FromSlash(rel))); err != nil {
		t.Fatal(err)
	}
}

// lines decodes the JSON log lines.
func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range bytes.Split([]byte(b.String()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestRoundRobinAcrossPipelines(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	for _, name := range []string{"p1", "p2"} {
		v.apply(newPackage(t, manualPipeline(name, `
  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 10 }
`), "a"))
		fireN(v, name, 3)
	}
	for i := range 3 {
		got := v.pull(2)
		slices.Sort(got)
		if !slices.Equal(got, []string{"p1/a", "p2/a"}) {
			t.Fatalf("pull %d = %v, want one delivery of each pipeline", i, got)
		}
	}
}

func TestBuiltinDedupeWindowWithTheEngineClock(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(newPackage(t, builtinPipeline, "a"))
	states := func(runID string) store.DeliveryState {
		v.t.Helper()
		v.emit(output("", `{"n":7}`))
		v.pull(10)
		return v.byStep(runID, "unique")[0].State
	}
	wantState := func(what string, got, want store.DeliveryState) {
		t.Helper()
		if got != want {
			t.Fatalf("%s: %s, want %s", what, got, want)
		}
	}
	wantState("first", states(v.fire("p", "adhoc", "r1")), store.DeliveryDone)
	v.clock.Advance(29 * 24 * time.Hour)
	wantState("29 days later", states(v.fire("p", "adhoc", "r2")), store.DeliveryFiltered)
	v.clock.Advance(24*time.Hour + time.Second)
	wantState("30 days and 1 second after the first", states(v.fire("p", "adhoc", "r3")), store.DeliveryDone)
}

func TestRateLimit(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(newPackage(t, manualPipeline("p", `
  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 10, rate: 2/min }
`), "a"))
	fireN(v, "p", 6)
	steps := []struct {
		advance time.Duration
		want    int
	}{
		{0, 2}, {0, 0}, {30 * time.Second, 1}, {10 * time.Second, 0}, {5 * time.Minute, 2},
	}
	for i, s := range steps {
		v.clock.Advance(s.advance)
		if got := v.pull(10); len(got) != s.want {
			t.Fatalf("step %d: pulled %d, want %d", i, len(got), s.want)
		}
	}
}

func TestInvalidRateIsNotLimited(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	pkg := newPackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 5 }\n"), "a")
	s := pkg.Pipeline.Steps["a"]
	s.Rate = "often"
	pkg.Pipeline.Steps["a"] = s
	v.apply(pkg)
	fireN(v, "p", 3)
	if got := v.pull(5); len(got) != 3 || !strings.Contains(v.logs.String(), "invalid step rate") {
		t.Fatalf("pull = %v; logs:\n%s", got, v.logs.String())
	}
}
