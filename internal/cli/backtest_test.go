package cli

import (
	"bytes"
	"encoding/json"
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

// TestBacktestCommand: a patch making the scorer fail replays run r1's three listings through score, which fails
// them all after and none before; the report prints as text or JSON, and a patch naming an unknown base or not
// parsing is refused.
func TestBacktestCommand(t *testing.T) {
	t.Parallel()
	s := newState(t, false)
	dir := testdir.New(t)
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	file := write("score.patch", scoreErrorPatch)
	out, _ := s.must(0, "backtest", file, "--json")
	var rep backtest.Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if strings.Join(rep.ChangedSteps, ",") != "score" || rep.Sampled != 3 || rep.Runs != 1 ||
		rep.Before.FailureRate != 0 || rep.After.FailureRate != 1 || rep.After.Failed != 3 {
		t.Fatalf("report:\n%s", out)
	}
	var text bytes.Buffer
	if err := printBacktest(&text, &rep); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"backtest of listings", "3 deliveries sampled from local history", "changed: score",
		"failure rate", "100% of 3"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("text report lacks %q:\n%s", want, text.String())
		}
	}
	unknown := write("unknown.patch", "Millstream-Base: "+strings.Repeat("ab", 32)+"\n"+scoreErrorPatch)
	s.checkRefusals(t, nil, []refusal{
		{cmd: BacktestCmd{Patch: unknown}, want: "no pipeline has revision abababababab"},
		{cmd: BacktestCmd{Patch: write("bad.patch", "nothing here\n")}, want: "invalid patch"},
		{cmd: BacktestCmd{Patch: file, Pipeline: "nope"}, want: "nope"},
	})
}
