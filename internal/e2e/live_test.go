//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/examples/m2-fixture/pool"
	"github.com/monolithiclab/millstream/internal/agent"
)

// liveRuntime is the score action's runtime for the live smoke test: the claude-code harness on the CLI's own login,
// with a per-delivery budget.
const liveRuntime = `runtime:
  agent:
    harness: claude-code
    model: %s
    maxCostUsd: 0.5
`

// TestLiveScore is the live smoke test, skipped unless MILLSTREAM_LIVE=1: it evaluates the M2 fixture's score action
// with the claude-code harness against the real claude CLI (logged in, on PATH) on two pool listings, recording into
// a temporary directory, and checks each answer is a valid scored listing. MILLSTREAM_LIVE_MODEL picks the model
// (haiku by default). It spends real money, a few cents, and is never part of make ci.
func TestLiveScore(t *testing.T) {
	if os.Getenv("MILLSTREAM_LIVE") != "1" {
		t.Skip("live smoke test: set MILLSTREAM_LIVE=1 to run it against the real claude CLI")
	}
	if _, err := osexec.LookPath("claude"); err != nil {
		t.Skip("live smoke test: the claude CLI is not on PATH")
	}
	model := os.Getenv("MILLSTREAM_LIVE_MODEL")
	if model == "" {
		model = "haiku"
	}
	pkg := filepath.Join(t.TempDir(), "m2")
	if err := os.CopyFS(pkg, os.DirFS(fixtureM2)); err != nil {
		t.Fatal(err)
	}
	action := filepath.Join(pkg, "actions", "score")
	manifest := filepath.Join(action, "action.yaml")
	data, err := os.ReadFile(manifest) // #nosec G304 -- a fixture copy
	if err != nil {
		t.Fatal(err)
	}
	const apiRuntime = "runtime:\n  agent:\n    harness: api\n"
	i := strings.Index(string(data), apiRuntime)
	if i < 0 {
		t.Fatalf("%s: no api runtime to replace", manifest)
	}
	if err := os.WriteFile(manifest, []byte(string(data[:i])+fmt.Sprintf(liveRuntime, model)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(action, "evals"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, l := range pool.Day(1)[:2] {
		input, _ := json.Marshal(l)
		c := fmt.Sprintf("input: %s\nassert:\n  - output.id == input.id\n  - output.score >= 0 && output.score <= 100\n"+
			"  - output.reason != ''\n", input)
		if err := os.WriteFile(filepath.Join(action, "evals", l.ID+".yaml"), []byte(c), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	recordings := filepath.Join(t.TempDir(), "recordings")
	r := mustExit(t, 0, dbPath(t), []string{"MILLSTREAM_SECRET_SCORER_KEY=unused"}, "eval", action,
		"--harness-mode", agent.ModeRecord, "--harness-dir", recordings)
	t.Logf("eval:\n%s", r.stdout)
	files, _ := filepath.Glob(filepath.Join(recordings, "*.json"))
	if len(files) != 2 {
		t.Fatalf("%d recordings, want one per listing", len(files))
	}
}
