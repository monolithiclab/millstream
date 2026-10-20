package backtest

import (
	"context"
	"encoding/json"
	"maps"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/proposals"
)

// TestHolds: the proposals' pass rule, after against before over the whole sample.
func TestHolds(t *testing.T) {
	base := Metrics{Compared: 50, Agreement: 0.9, Reviewed: 10, ReviewAgreement: 0.8, Ran: 50, FailureRate: 0.1}
	for name, tt := range map[string]struct {
		after func(m *Metrics)
		want  bool
	}{
		"unchanged":                  {func(*Metrics) {}, true},
		"agreement within tolerance": {func(m *Metrics) { m.Agreement = 0.885 }, true},
		"agreement regressed":        {func(m *Metrics) { m.Agreement = 0.87 }, false},
		"reviews regressed":          {func(m *Metrics) { m.ReviewAgreement = 0.75 }, false},
		"outputs changed as the reviewers corrected them": {func(m *Metrics) { m.Agreement, m.ReviewAgreement = 0.6, 1 },
			true},
		"outputs changed, reviews unchanged":    {func(m *Metrics) { m.Agreement = 0.6 }, false},
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
		Improve: pipeline.Improve{BacktestSample: 1}}), 0).Backtest(ctx, proposals.BacktestRequest{Pipeline: "bt",
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
