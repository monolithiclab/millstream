//go:build e2e

package backtest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// recordHistory runs the fixture twice (three listings scored 40, 50 and 60 each time: the first filtered out) in
// a store and returns it with the revision's package hash.
func recordHistory(t *testing.T) (store.Store, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	pkg, err := pipeline.Materialize(t.TempDir(), fixtureFiles())
	if err != nil {
		t.Fatal(err)
	}
	e, err := engine.New(engine.Options{Store: st, NoCron: true, Artifacts: artifacts.New(t.TempDir())})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Apply(ctx, pkg); err != nil {
		t.Fatal(err)
	}
	var refs []engine.RunRef
	for _, key := range []string{"day1", "day2"} {
		id, _, err := e.Fire(ctx, "bt", "adhoc", key, json.RawMessage(`{"n":3,"base":40}`))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, engine.RunRef{Pipeline: "bt", ID: id})
	}
	pool := exec.NewPool(exec.PoolOptions{Options: exec.Options{Launchers: launchers()}})
	defer func() { _ = pool.Close() }()
	n, err := node.New(e, node.Options{Owner: "history", Slots: 4, Pool: pool, PullInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- n.Run(runCtx) }()
	werr := e.WaitSettled(ctx, refs)
	stop()
	if err := <-done; err != nil || werr != nil {
		t.Fatal(err, werr)
	}
	crmCalls.Store(0)
	return st, pkg.Hash
}

// newBacktester returns a Backtester over st running the fixture's actions in-process.
func newBacktester(st store.Store, improve pipeline.Improve) *Backtester {
	return New(Options{Store: st, Launchers: launchers(), Improve: improve, Timeout: time.Minute})
}

func stepReport(t *testing.T, rep *Report, step string) StepReport {
	t.Helper()
	for _, s := range rep.Steps {
		if s.Step == step {
			return s
		}
	}
	t.Fatalf("no report for step %s in %+v", step, rep.Steps)
	return StepReport{}
}

// TestConfigChange: a bonus on score changes every score: the base replay agrees with history and the patched one
// does not; the listing scored 40 now passes keep, so enrich runs on it (newly reached), and notify, external,
// never runs: it publishes nothing for an input the history has no outcome for.
func TestConfigChange(t *testing.T) {
	st, hash := recordHistory(t)
	_, data := edit(t, hash, fixtureFiles(), "mode: score, bonus: 0", "mode: score, bonus: 15")
	rep, err := newBacktester(st, pipeline.Improve{}).Run(context.Background(),
		Request{Pipeline: "bt", BaseRevision: hash, Patch: data})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Source != SourceLocal || rep.Runs != 2 || len(rep.ChangedSteps) != 1 || rep.ChangedSteps[0] != "score" {
		t.Fatalf("report = %+v", rep)
	}
	score := stepReport(t, rep, "score")
	if score.Before.Agreement != 1 || score.After.Agreement != 0 || score.After.Ran != 6 || score.After.Compared != 6 {
		t.Errorf("score: before %+v, after %+v", score.Before, score.After)
	}
	if rep.Before.Agreement != 1 || rep.After.Agreement >= 1 || rep.After.NewlyReached == 0 {
		t.Errorf("overall: before %+v, after %+v", rep.Before, rep.After)
	}
	if n := crmCalls.Load(); n != 0 {
		t.Errorf("the external action ran %d times", n)
	}
	if len(rep.Divergence) != 0 || rep.PatchedRevision == hash || rep.PatchHash == "" {
		t.Errorf("report = %+v", rep)
	}
}

// TestTopologyRerun: lowering the threshold admits the listings keep filtered out: enrich, downstream and
// unchanged, runs for them, and notify does not.
func TestTopologyRerun(t *testing.T) {
	st, hash := recordHistory(t)
	_, data := edit(t, hash, fixtureFiles(), "minScore: 50", "minScore: 30")
	rep, err := newBacktester(st, pipeline.Improve{BacktestSample: 50}).Run(context.Background(),
		Request{Pipeline: "bt", BaseRevision: hash, Patch: data})
	if err != nil {
		t.Fatal(err)
	}
	keep := stepReport(t, rep, "keep")
	if keep.Before.Agreement != 1 || keep.After.Agreed != 4 || keep.After.Compared != 6 {
		t.Errorf("keep: before %+v, after %+v", keep.Before, keep.After)
	}
	if rep.After.NewlyReached < 2 || rep.After.Ran <= rep.Before.Ran {
		t.Errorf("overall: before %+v, after %+v", rep.Before, rep.After)
	}
	if n := crmCalls.Load(); n != 0 {
		t.Errorf("the external action ran %d times", n)
	}
}

// TestBudgetReached: a replay stops once what it spends passes what is left of the day's improve budget, and the
// ledger is charged what the backtest spent.
func TestBudgetReached(t *testing.T) {
	st, hash := recordHistory(t)
	_, data := edit(t, hash, fixtureFiles(), "mode: score, bonus: 0", "mode: score, bonus: 15, cost: 0.01")
	ledger := &Ledger{}
	b := New(Options{Store: st, Launchers: launchers(), Improve: pipeline.Improve{BudgetDayUSD: 0.025},
		Budget: ledger, Timeout: time.Minute})
	_, err := b.Run(context.Background(), Request{Pipeline: "bt", BaseRevision: hash, Patch: data})
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("Run = %v, want ErrBudget", err)
	}
	if spent := ledger.Spent(time.Now()); spent < 0.025 || spent > 0.065 {
		t.Errorf("charged %.3f", spent)
	}
	if _, err := b.Prepare(Request{Pipeline: "bt", BaseRevision: hash, Patch: data}); !errors.Is(err, ErrBudget) {
		t.Errorf("Prepare once spent = %v", err)
	}
}
