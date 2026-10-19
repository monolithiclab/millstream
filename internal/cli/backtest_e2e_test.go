//go:build e2e

package cli

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/backtest"
)

// TestBacktestCommand: a patch making the scorer fail replays run r1's three listings through score, which fails
// them all after and none before. Two replays: in the e2e build.
func TestBacktestCommand(t *testing.T) {
	t.Parallel()
	s := newState(t, false)
	file := writeFile(t, "score.patch", scoreErrorPatch)
	out, _ := s.must(0, "backtest", file, "--json")
	var rep backtest.Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if strings.Join(rep.ChangedSteps, ",") != "score" || rep.Sampled != 3 || rep.Runs != 1 ||
		rep.Before.FailureRate != 0 || rep.After.FailureRate != 1 || rep.After.Failed != 3 {
		t.Fatalf("report:\n%s", out)
	}
	if out, _ := s.must(0, "backtest", file, "--sample", "1"); !strings.Contains(out, "1 deliveries sampled") {
		t.Errorf("text report:\n%s", out)
	}
}
