package backtest

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/proposals"
)

// TestHolds: the proposals' pass rule, after against before over the whole sample: 40 deliveries no review lies
// below, 10 whose outputs lead to a review.
func TestHolds(t *testing.T) {
	base := Metrics{Compared: 50, Agreement: 0.9, UnreviewedCompared: 40, UnreviewedAgreement: 0.9,
		ReviewedCompared: 10, ReviewedAgreement: 0.9, Reviewed: 10, ReviewAgreement: 0.8, Ran: 50, FailureRate: 0.1}
	for name, tt := range map[string]struct {
		after func(m *Metrics)
		want  bool
	}{
		"unchanged":                             {func(*Metrics) {}, true},
		"unreviewed agreement within tolerance": {func(m *Metrics) { m.UnreviewedAgreement = 0.885 }, true},
		"unreviewed agreement regressed":        {func(m *Metrics) { m.UnreviewedAgreement = 0.87 }, false},
		"reviews regressed":                     {func(m *Metrics) { m.ReviewAgreement = 0.75 }, false},
		"reviewed outputs changed as the reviewers corrected them": {func(m *Metrics) {
			m.ReviewedAgreement, m.TowardReviewers, m.TowardReviewersShare, m.ReviewAgreement = 0.4, 5, 0.5, 1
		}, true},
		"reviewed outputs changed beyond the corrections": {func(m *Metrics) {
			m.ReviewedAgreement, m.TowardReviewers, m.TowardReviewersShare, m.ReviewAgreement = 0.3, 5, 0.5, 1
		}, false},
		"reviewed outputs changed, reviews unchanged": {func(m *Metrics) { m.ReviewedAgreement = 0.6 }, false},
		// The finer rule's reason: one reviewed item fixed, the outputs nobody reviewed wrecked.
		"one reviewed item fixed, unreviewed outputs wrecked": {func(m *Metrics) {
			m.ReviewedAgreement, m.TowardReviewers, m.TowardReviewersShare, m.ReviewAgreement = 0.8, 1, 0.1, 0.9
			m.UnreviewedAgreement, m.Agreement = 0.3, 0.4
		}, false},
		// What a lessons change is for: what the reviewers did to their items, done to the unreviewed ones.
		"reviewers' corrections generalised to unreviewed outputs": {func(m *Metrics) {
			m.ReviewedAgreement, m.TowardReviewers, m.TowardReviewersShare, m.ReviewAgreement = 0.4, 5, 0.5, 1
			m.UnreviewedAgreement = 0.5
		}, true},
		"unreviewed outputs changed beyond the corrections": {func(m *Metrics) {
			m.ReviewedAgreement, m.TowardReviewers, m.TowardReviewersShare, m.ReviewAgreement = 0.4, 5, 0.5, 1
			m.UnreviewedAgreement = 0.35
		}, false},
		"approvals dropped within maxFalseDrop": {func(m *Metrics) { m.Approved, m.ApprovalsDropped = 10, 1 }, true},
		"approvals dropped over maxFalseDrop":   {func(m *Metrics) { m.Approved, m.ApprovalsDropped = 10, 2 }, false},
		"failure rate up":                       {func(m *Metrics) { m.FailureRate = 0.11 }, false},
		"failure rate down":                     {func(m *Metrics) { m.FailureRate = 0 }, true},
	} {
		after := base
		tt.after(&after)
		if got := (&Report{Before: base, After: after}).Holds(0.1); got != tt.want {
			t.Errorf("%s: Holds = %v, want %v", name, got, tt.want)
		}
	}
	// Nothing compared or reviewed before: agreement cannot regress.
	if !(&Report{After: Metrics{Agreement: 0, ReviewAgreement: 0}}).Holds(0.1) {
		t.Error("an empty sample does not hold")
	}
}

// TestForProposals: a proposal's validation backtests through the real runner on the seeded history, in a scratch
// store: a lessons file the exec step ignores changes no outcome, and passes. A pipeline without history to replay
// passes, its report saying so. (A regression failing it is TestHolds; the replay's own measures are TestConfigChange,
// under the e2e tag.)
func TestForProposals(t *testing.T) {
	ctx := context.Background()
	files := fixtureFiles()
	lessons := map[string][]byte{"lessons/score.yaml": []byte("rules:\n  - text: Score 0 without a surface.\n")}
	maps.Copy(lessons, files)
	harmless, err := patch.Format(historyHash, files, lessons)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ForProposals(New(Options{Store: seeded, Launchers: launchers(), Timeout: time.Minute,
		Improve: pipeline.Improve{Backtest: pipeline.BacktestConfig{Sample: 1}}}), 0).Backtest(ctx, proposals.BacktestRequest{Pipeline: "bt",
		BaseRevision: historyHash, Patch: string(harmless), Steps: []string{"score"}})
	var rep Report
	if err != nil || !res.Passed || json.Unmarshal(res.Report, &rep) != nil || rep.Sampled != 1 ||
		rep.After.Agreement != rep.Before.Agreement {
		t.Errorf("Backtest = %+v (%s), %v", res, res.Report, err)
	}
	_, data := edit(t, fixtureHash, files, "mode: score, bonus: 0", "mode: score, bonus: 15")
	res, err = ForProposals(New(Options{Store: revisionOnly, Launchers: launchers()}), 0).Backtest(ctx,
		proposals.BacktestRequest{Pipeline: "bt", BaseRevision: fixtureHash, Patch: string(data)})
	rep = Report{}
	if err != nil || !res.Passed || json.Unmarshal(res.Report, &rep) != nil || rep.Sampled != 0 || len(rep.Warnings) != 1 {
		t.Errorf("without history: %+v (%s), %v", res, res.Report, err)
	}
}

// TestReserve: a running backtest holds what is left of the day's budget, so a concurrent one is refused instead of
// spending it again; released, what it did not spend is free again.
func TestReserve(t *testing.T) {
	now := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	ledger := &Ledger{}
	ledger.Charge(now, 1)
	b := New(Options{Budget: ledger, Now: func() time.Time { return now }, Improve: pipeline.Improve{Budget: pipeline.BudgetConfig{DayUSD: 5}}})
	left, limited, release, err := b.reserve()
	if err != nil || !limited || left != 4 {
		t.Fatalf("reserve = %v, %v, %v", left, limited, err)
	}
	if _, _, _, err := b.reserve(); !errors.Is(err, ErrBudget) {
		t.Fatalf("a concurrent reservation: %v", err)
	}
	if left, _ := b.remaining(); left > 0 {
		t.Fatalf("%v left while the budget is held: Prepare would start another backtest", left)
	}
	ledger.Charge(now, 1.5)
	release()
	if left, _, release, err := b.reserve(); err != nil || left != 2.5 {
		t.Fatalf("after release: %v, %v", left, err)
	} else {
		release()
	}
	if _, limited, _, err := New(Options{}).reserve(); err != nil || limited {
		t.Fatalf("without a budget: %v, %v", limited, err)
	}
}
