//go:build e2e

package cli

import (
	"context"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// Test_initListings: the listings example scaffolds under the name given, passes check and runs --once offline with
// its own environment file: the crawl's Python action reads the bundled listings, the score agent replays the
// package's recordings with no model and no key, and the shortlist parks in the inbox, where run --once stops waiting
// (exit 1, the run waits on reviews).
func Test_initListings(t *testing.T) {
	t.Parallel()
	if _, err := osexec.LookPath("python3"); err != nil {
		t.Skip("python3 is not on PATH: the example's crawl action needs it")
	}
	parent := testdir.New(t)
	out, _ := mustRun(t, 0, "init", "pipeline", "houses", "--from", "listings", "--dir", parent)
	pkg := filepath.Join(parent, "houses")
	if !strings.HasPrefix(out, "created "+pkg+", the listings example\n") {
		t.Fatalf("init: %q", out)
	}
	if _, errOut := mustRun(t, 0, "check", pkg); !strings.Contains(errOut, "houses: 0 errors") {
		t.Fatalf("check: %s", errOut)
	}
	db := dsn(migratedDB(t))
	env := map[string]string{agent.EnvHarnessMode: agent.ModeReplay, agent.EnvHarnessDir: filepath.Join(pkg, "recordings")}
	code, out, errOut := runCLICtx(t, context.Background(), env, "--db", db, "--env-file",
		filepath.Join(pkg, "millstream.yaml"), "run", "--once", "--request-id", "r1", pkg)
	want := "houses run_r1_adhoc waitingReview\n  listings.raw 6\n  listings.scored 6\n  listings.shortlist 3\n" +
		"  listings.unique 6\n  trigger.adhoc 1\n"
	if code != 1 || out != want {
		t.Fatalf("run --once: code %d\nstdout:\n%s\nstderr:\n%s", code, out, errOut)
	}
	out, _ = mustRun(t, 0, "--db", db, "inbox")
	if lines := splitLines(out); len(lines) != 4 || !strings.Contains(out, "L1004: 45 m² in Lille at 150000 EUR, "+
		"score 84") {
		t.Fatalf("inbox:\n%s", out)
	}
}
