package backtest

import (
	"cmp"
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
// the base revision's over the whole sample: agreement with the reviewers' decisions falls by at most Tolerance;
// agreement with the historical outputs of the lineages no review lies below falls by at most Tolerance; on the
// reviewed lineages it may fall further only by the share of deliveries that moved toward the reviewers' truth
// (TowardReviewersShare: what a proposal correcting outputs as reviewers did is for), so fixing one reviewed item
// never pays for wrecking the outputs nobody reviewed; an agreement with nothing to compare does not count. The
// failure rate does not rise at all, and the patched replay drops at most maxFalseDrop of the sample's approvals,
// items a person approved that no longer reach their review (improve.filter.maxFalseDrop, spec section 11.6).
func (r *Report) Holds(maxFalseDrop float64) bool {
	b, a := r.Before, r.After
	switch {
	case a.Approved > 0 && float64(a.ApprovalsDropped-b.ApprovalsDropped) > maxFalseDrop*float64(a.Approved):
		return false
	case b.Reviewed > 0 && a.ReviewAgreement < b.ReviewAgreement-Tolerance:
		return false
	case b.UnreviewedCompared > 0 && a.UnreviewedAgreement < b.UnreviewedAgreement-Tolerance:
		return false
	case b.ReviewedCompared > 0 && a.ReviewedAgreement < b.ReviewedAgreement-a.TowardReviewersShare-Tolerance:
		return false
	}
	return a.FailureRate <= b.FailureRate
}

// ForProposals adapts a Runner to the proposals' validation: it backtests the proposal's patch on its base revision,
// with the steps it touches besides those the backtest derives, and passes it when the report Holds. A pipeline
// whose history never reaches the changed steps has nothing to regress (ErrNoHistory): its report samples nothing
// and says so, and holds. Any other refusal (a spent budget, a production deployment, proposed exec code) is the
// validation's failure.
// maxFalseDrop is improve.filter.maxFalseDrop, DefaultMaxFalseDrop when zero.
func ForProposals(r Runner, maxFalseDrop float64) proposals.Backtester {
	return proposalBacktester{r: r, maxFalseDrop: cmp.Or(maxFalseDrop, DefaultMaxFalseDrop)}
}

// DefaultMaxFalseDrop is improve.filter.maxFalseDrop's default.
const DefaultMaxFalseDrop = 0.01

type proposalBacktester struct {
	r            Runner
	maxFalseDrop float64
}

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
	return proposals.BacktestResult{Passed: rep.Holds(b.maxFalseDrop), Report: data}, nil
}
