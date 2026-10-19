//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/monolithiclab/millstream/examples/m2-fixture/pool"
	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
)

// TestM2Backtest backtests the score step's second lessons version, as a patch, against a day the M2 fixture ran
// with the first: millstream backtest replays the day's listings through score from the recordings of both prompts.
// The base replay agrees with history everywhere; the patched one changes exactly the scores the edited rule
// concerns, and the shortlist with them. Both replays price the agent's calls.
func TestM2Backtest(t *testing.T) {
	t.Parallel()
	db := dbPath(t)
	env := m2Env(m2Recordings)
	const day = 3
	mustExit(t, 0, db, env, m2Run(fixtureM2, fmt.Sprintf("day-%d", day), day)...)

	base, err := pipeline.Load(fixtureM2)
	if err != nil {
		t.Fatal(err)
	}
	edited, err := pipeline.Load(fixtureM2Lessons)
	if err != nil {
		t.Fatal(err)
	}
	data, err := patch.Format(base.Hash, base.Files, edited.Files)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "lessons.patch")
	if err := os.WriteFile(file, data, 0o600); err != nil {
		t.Fatal(err)
	}
	r := mustExit(t, 0, db, env, "--env-file", envFile, "backtest", file, "--json")
	var rep backtest.Report
	if err := json.Unmarshal([]byte(r.stdout), &rep); err != nil {
		t.Fatalf("%v\n%s", err, r.stdout)
	}
	if len(rep.ChangedSteps) != 1 || rep.ChangedSteps[0] != "score" || rep.Source != backtest.SourceLocal ||
		rep.Runs != 1 || len(rep.Divergence) != 0 {
		t.Fatalf("report:\n%s", r.stdout)
	}
	listings := pool.Day(day)
	wantChanged := 0
	for _, l := range listings {
		if pool.Score(l, lessonsV1Penalty) != pool.Score(l, lessonsV2Penalty) {
			wantChanged++
		}
	}
	var score backtest.StepReport
	for _, s := range rep.Steps {
		if s.Step == "score" {
			score = s
		}
	}
	if score.Before.Compared != len(listings) || score.Before.Agreement != 1 ||
		score.After.Compared != len(listings) || score.After.Compared-score.After.Agreed != wantChanged ||
		wantChanged == 0 {
		t.Fatalf("score: before %+v, after %+v; want %d of %d changed", score.Before, score.After, wantChanged,
			len(listings))
	}
	if score.Before.CostUSD <= 0 || score.After.CostUSD <= 0 || rep.CostUSD != rep.Before.CostUSD+rep.After.CostUSD {
		t.Errorf("costs: before %v, after %v, total %v", score.Before.CostUSD, score.After.CostUSD, rep.CostUSD)
	}
	shortlistBefore := m2Counts(listings, lessonsV1Penalty)["listings.shortlist"]
	shortlistAfter := m2Counts(listings, lessonsV2Penalty)["listings.shortlist"]
	if rep.Before.Agreement != 1 || rep.After.Agreement == 1 ||
		shortlistAfter < shortlistBefore && rep.After.NoLongerReached == 0 {
		t.Errorf("overall: before %+v, after %+v (shortlist %d then %d)", rep.Before, rep.After, shortlistBefore,
			shortlistAfter)
	}
	t.Logf("strata %v; score agreement %.2f -> %.2f", rep.Strata, score.Before.Agreement,
		score.After.Agreement)
}
