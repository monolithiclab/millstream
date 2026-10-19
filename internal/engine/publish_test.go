package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/proposals"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// The apply publish action applies an accepted proposal on its base only, never in production, and records it
// applied and validated; a revision applied with the proposal's trailer over another revision records it not
// validated in that combination, is validated against the revision before it, and raises a regression insight
// when its backtest does not hold (spec section 11.3).
func TestPublishApply(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	bt := &verdict{passed: true}
	e, err := New(Options{Store: st, PackageDir: testdir.New(t), Proposals: ProposalOptions{Backtest: bt}})
	if err != nil {
		t.Fatal(err)
	}
	files := func(extra, expr string) map[string][]byte {
		return map[string][]byte{"pipeline.yaml": fmt.Appendf(nil, proposalSrc, extra, expr)}
	}
	apply := func(f map[string][]byte, trailers ...string) {
		t.Helper()
		pkg, err := e.PackageFromFiles(f)
		if err == nil {
			_, _, err = e.ApplyPackages(ctx, []*pipeline.Package{pkg},
				ApplyOptions{Proposals: map[string][]string{"extra": trailers}})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	apply(files("", "true"))
	diff := "--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -4 +4 @@\n" +
		"-  pass: { action: filter, in: trigger.adhoc, config: { expr: \"true\" } }\n" +
		"+  pass: { action: filter, in: trigger.adhoc, config: { expr: \"false\" } }\n"
	p, err := e.CreateProposal(ctx, ProposalRequest{Pipeline: "extra", Kind: store.ProposalConfig, Patch: diff})
	if err == nil {
		p, err = e.AcceptProposal(ctx, p.ID, p.PatchSHA256, "ana")
	}
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := patch.Parse([]byte(diff))
	if err != nil {
		t.Fatal(err)
	}
	patched, err := parsed.Apply(files("", "true"))
	if err != nil {
		t.Fatal(err)
	}

	e.opts.Mode = pipeline.ModeProduction
	if _, err := e.ApplyProposal(ctx, p.ID, "extra", p.BaseRevision, patched); !errors.Is(err, pipeline.ErrProduction) {
		t.Fatalf("apply in production: %v", err)
	}
	e.opts.Mode = ""
	if _, err := e.ApplyProposal(ctx, p.ID, "extra", strings.Repeat("0", 64), patched); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("apply on another base: %v", err)
	}
	rev, err := e.ApplyProposal(ctx, p.ID, "extra", p.BaseRevision, patched)
	if err != nil || rev != 2 {
		t.Fatalf("ApplyProposal = %d, %v", rev, err)
	}

	// Merged elsewhere over another revision: applied with its trailer, never validated so.
	bt.passed, bt.requests = false, nil
	apply(files("description: moved\n", "false"), p.ID, "pr_unknown")
	evs, err := st.PipelineEvents(ctx, store.EventFilter{Pipeline: "extra", Kinds: []string{store.EventProposalApplied}})
	if err != nil || len(evs) != 2 {
		t.Fatalf("proposalApplied events = %d, %v", len(evs), err)
	}
	for i, want := range []bool{true, false} {
		var data struct{ Validated bool }
		if err := json.Unmarshal(evs[i].Data, &data); err != nil || data.Validated != want {
			t.Fatalf("event %d = %s, want validated %v", i, evs[i].Data, want)
		}
	}
	ins, err := st.Insights(ctx, store.InsightFilter{Pipeline: "extra"})
	if err != nil || len(ins) != 1 || ins[0].Kind != pipeline.InsightRegression ||
		!strings.Contains(string(ins[0].Evidence), p.ID) {
		t.Fatalf("insights = %+v, %v", ins, err)
	}
	if len(bt.requests) != 1 || bt.requests[0].BaseRevision == p.BaseRevision {
		t.Fatalf("backtests %+v: want one, against the revision before", bt.requests)
	}
}

// verdict is a backtester answering passed, recording its requests.
type verdict struct {
	passed   bool
	requests []proposals.BacktestRequest
}

func (v *verdict) Backtest(_ context.Context, req proposals.BacktestRequest) (proposals.BacktestResult, error) {
	v.requests = append(v.requests, req)
	return proposals.BacktestResult{Passed: v.passed, Report: json.RawMessage(`{}`)}, nil
}
