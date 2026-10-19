package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// scoreErrorPatch makes the run fixture's scorer fail: its scoreMode variable's default becomes error. It states no
// base, so it applies to the pipeline's active revision.
const scoreErrorPatch = `--- a/pipeline.yaml
+++ b/pipeline.yaml
@@ -2 +2 @@
-vars: { count: 3, scoreMode: echo }
+vars: { count: 3, scoreMode: error }
`

// writeFile writes a file in a directory of the test's own and returns its path.
func writeFile(t *testing.T, name, body string) string {
	t.Helper()
	p := filepath.Join(testdir.New(t), name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestBacktestReport prints a report: the sample by stratum, before and after overall and per step, the cost and
// the warnings; and the command refuses a patch naming an unknown base, one that does not parse, and an unknown
// pipeline (TestBacktestCommand, under the e2e tag, runs a backtest through it).
func TestBacktestReport(t *testing.T) {
	t.Parallel()
	rep := &backtest.Report{Pipeline: "listings", PatchHash: strings.Repeat("a", 64), Source: backtest.SourceLocal,
		ChangedSteps: []string{"score"}, Sampled: 3, Strata: map[string]int{"done": 2, "dead": 1}, Runs: 1,
		After: backtest.Metrics{Ran: 3, Failed: 3, FailureRate: 1, Compared: 3},
		Steps: []backtest.StepReport{{Step: "score"}}, Warnings: []string{"diverged"}, CostUSD: 0.5}
	var text bytes.Buffer
	if err := printBacktest(&text, rep); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"backtest of listings: patch aaaaaaaaaaaa", "3 deliveries sampled from local history",
		"(done 2, dead 1)", "changed: score", "failure rate", "100% of 3", "cost 0.5000 USD", "warning: diverged"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("text report lacks %q:\n%s", want, text.String())
		}
	}
	s := newState(t, false)
	file := writeFile(t, "score.patch", scoreErrorPatch)
	s.checkRefusals(t, nil, []refusal{
		{cmd: BacktestCmd{Patch: writeFile(t, "unknown.patch", "Millstream-Base: "+strings.Repeat("ab", 32)+"\n"+
			scoreErrorPatch)}, want: "no pipeline has revision abababababab"},
		{cmd: BacktestCmd{Patch: writeFile(t, "bad.patch", "nothing here\n")}, want: "invalid patch"},
		{cmd: BacktestCmd{Patch: file, Pipeline: "nope"}, want: "nope"},
	})
}
