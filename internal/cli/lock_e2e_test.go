//go:build e2e

package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// The tests here run containerSetup's fake docker, a shell script: they resolve images through it.

const rebuiltID = "sha256:2222222222222222222222222222222222222222222222222222222222222222"

func TestLockCommand(t *testing.T) {
	t.Parallel()
	pkg, envFile, env := containerSetup(t, reportID)
	ctx := context.Background()

	code, out, _ := runCLICtx(t, ctx, env, "--env-file", envFile, "check", pkg)
	if code != 0 || !strings.Contains(out, "warning steps.report.action: image report:dev ("+reportID+") is not pinned") ||
		!strings.Contains(out, "warning steps.score.action: image ghcr.io/lab/actions/score:1.2.0") {
		t.Fatalf("check before lock: %d\n%s", code, out)
	}

	code, out, errOut := runCLICtx(t, ctx, env, "--env-file", envFile, "lock", pkg)
	if code != 0 {
		t.Fatalf("lock: %d\n%s%s", code, out, errOut)
	}
	if want := "./actions/report report:dev " + reportID + "\nlab/score@1 ghcr.io/lab/actions/score:1.2.0 " + scoreID +
		"\n"; out != want {
		t.Fatalf("lock printed:\n%s\nwant:\n%s", out, want)
	}
	data, err := os.ReadFile(filepath.Join(pkg, pipeline.LockFile)) // #nosec G304 -- the test's package
	if err != nil {
		t.Fatal(err)
	}
	l, err := pipeline.ParseLock(data)
	if err != nil {
		t.Fatal(err)
	}
	if l.Actions["lab/score@1"] != (pipeline.LockEntry{Version: "1.2.0", Image: "ghcr.io/lab/actions/score:1.2.0",
		Digest: scoreID}) || l.Actions["./actions/report"] != (pipeline.LockEntry{Image: "report:dev", Digest: reportID}) ||
		len(l.Actions) != 2 {
		t.Fatalf("lock file %+v", l.Actions)
	}

	code, out, _ = runCLICtx(t, ctx, env, "--env-file", envFile, "check", pkg)
	if code != 0 || strings.Contains(out, "not pinned") {
		t.Fatalf("check after lock: %d\n%s", code, out)
	}
}

// TestLockMismatch: once the image is rebuilt, check and run refuse the package until it is locked again.
func TestLockMismatch(t *testing.T) {
	t.Parallel()
	pkg, envFile, env := containerSetup(t, rebuiltID)
	lock := "actions:\n  ./actions/report: { image: \"report:dev\", digest: \"" + reportID + "\" }\n"
	if err := os.WriteFile(filepath.Join(pkg, pipeline.LockFile), []byte(lock), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	code, out, _ := runCLICtx(t, ctx, env, "--env-file", envFile, "check", pkg)
	want := "error steps.report.action: action ./actions/report: image report:dev is " + rebuiltID +
		" but millstream.lock pins report:dev (" + reportID + ")"
	if code != 1 || !strings.Contains(out, want) {
		t.Fatalf("check: %d\n%s", code, out)
	}
	code, _, errOut := runCLICtx(t, ctx, env, "--env-file", envFile, "--db", dbPath(t), "run", "--once", pkg)
	if code != 1 || !strings.Contains(errOut, "millstream.lock pins") {
		t.Fatalf("run: %d\n%s", code, errOut)
	}
	if code, _, _ := runCLICtx(t, ctx, env, "--env-file", envFile, "lock", pkg); code != 0 {
		t.Fatalf("relock: %d", code)
	}
	if code, out, _ := runCLICtx(t, ctx, env, "--env-file", envFile, "check", pkg); code != 0 {
		t.Fatalf("check after relocking: %d\n%s", code, out)
	}
}
