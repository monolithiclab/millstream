package backtest

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/proposals"
)

// Tolerance is how far a proposal's backtest may lower an agreement before it fails (spec section 11.3): two points,
// so that one delivery of a small sample replaying differently does not hold a proposal back.
const Tolerance = 0.02

// Holds reports whether a backtest lets a proposal through (spec section 11.3), comparing the patched replay with
// the base revision's over the whole sample: agreement with the historical outputs and agreement with the reviewers'
// decisions each fall by at most Tolerance (an agreement with nothing to compare does not count), and the failure
// rate does not rise at all.
func (r *Report) Holds() bool {
	b, a := r.Before, r.After
	switch {
	case b.Compared > 0 && a.Agreement < b.Agreement-Tolerance:
		return false
	case b.Reviewed > 0 && a.ReviewAgreement < b.ReviewAgreement-Tolerance:
		return false
	}
	return a.FailureRate <= b.FailureRate
}

// ForProposals adapts a Runner to the proposals' validation: it backtests the proposal's patch on its base revision,
// with the steps it touches besides those the backtest derives, and passes it when the report Holds. A pipeline
// whose history never reaches the changed steps has nothing to regress (ErrNoHistory): its report samples nothing
// and says so, and holds. Any other refusal (a spent budget, a production deployment, proposed exec code) is the
// validation's failure.
func ForProposals(r Runner) proposals.Backtester { return proposalBacktester{r} }

type proposalBacktester struct{ r Runner }

func (b proposalBacktester) Backtest(ctx context.Context, req proposals.BacktestRequest) (proposals.BacktestResult,
	error,
) {
	rep, err := b.r.Run(ctx, Request{Pipeline: req.Pipeline, BaseRevision: req.BaseRevision, Patch: []byte(req.Patch),
		Steps: req.Steps})
	switch {
	case errors.Is(err, ErrNoHistory):
		rep = &Report{Pipeline: req.Pipeline, BaseRevision: req.BaseRevision, PatchHash: patch.Hash([]byte(req.Patch)),
			Strata: map[string]int{}, ChangedSteps: []string{}, Steps: []StepReport{}, Warnings: []string{err.Error()}}
	case err != nil:
		return proposals.BacktestResult{}, err
	}
	data, err := json.Marshal(rep)
	if err != nil {
		return proposals.BacktestResult{}, err
	}
	return proposals.BacktestResult{Passed: rep.Holds(), Report: data}, nil
}
