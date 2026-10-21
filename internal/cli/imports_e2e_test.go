//go:build e2e

package cli

import (
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
)

// Two pipelines end to end through run (spec section 4.12): deals imports listings' stream, so the listings run
// opens a run in deals holding a copy of each listing, waited on and reported by run --once; then a back-fill
// replays the producer's retained items into a deals replay run, which the next run --once waits for.
func Test_runImports(t *testing.T) {
	t.Parallel()
	src, deals, db := runPackage(t, "run"), runPackage(t, "imports"), migratedDB(t)
	out, _ := mustRun(t, 0, "--db", dsn(db), "run", "--once", "--request-id", "r1", src, deals)
	importRun := ids.ImportRun("approved", "run_r1_adhoc")
	for _, want := range []string{
		"listings run_r1_adhoc succeeded\n",
		"deals " + importRun + " succeeded\n  approved 3\n",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("summary lacks %q:\n%s", want, out)
		}
	}
	out, _ = mustRun(t, 0, "--db", dsn(db), "replay", "--pipeline", "deals", "--stream", "approved", "--since", "7d",
		"--request-id", "bf")
	if out != "run_replay_bf\n" {
		t.Fatalf("replay --stream stdout = %q", out)
	}
	out, _ = mustRun(t, 0, "--db", dsn(db), "run", "--once", "--request-id", "r1", src, deals)
	if !strings.Contains(out, "deals run_replay_bf succeeded\n") {
		t.Fatalf("summary after the back-fill:\n%s", out)
	}
}
