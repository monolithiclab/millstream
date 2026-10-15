//go:build e2e

package cli

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"
)

// More run flows through the engine, beyond Test_runOnce: a generated request id and a failing run, a cron
// trigger, and a run until interrupted.

func Test_runOnceGeneratesRequestIDAndFailsOnErrors(t *testing.T) {
	t.Parallel()
	pkg, db := runPackage(t, "run"), migratedDB(t)
	out, _ := mustRun(t, 1, "--db", db, "run", "--once", "--var", "scoreMode=error", pkg)
	if !regexp.MustCompile(`^listings run_\d{8}T\d{6}Z-[0-9a-f]{8}_adhoc completedWithErrors\n`).MatchString(out) ||
		!strings.Contains(out, "  score.dlq 3\n") {
		t.Fatalf("summary:\n%s", out)
	}
}

func Test_runOnceCron(t *testing.T) {
	t.Parallel()
	pkg, db := runPackage(t, "cron"), migratedDB(t)
	out, _ := mustRun(t, 0, "--db", db, "run", "--once", pkg)
	if !regexp.MustCompile(`^nightly run_\d{4}-\d\d-\d\dT\d\d:\d\dZ_daily succeeded\n  trigger.daily 1\n$`).
		MatchString(out) {
		t.Fatalf("summary:\n%s", out)
	}
}

func Test_runUntilInterrupted(t *testing.T) {
	t.Parallel()
	pkg, db := runPackage(t, "run"), migratedDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out bytes.Buffer
	errOut := newWatchBuffer(func(s string) bool { return strings.Contains(s, "revision applied") })
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, []string{"--db", db, "run", "--lease-ttl", "3s", pkg},
			IO{In: strings.NewReader(""), Out: &out, Err: errOut}, "dev", func(rt *Runtime) { rt.Launchers = testLaunchers() })
	}()
	select {
	case <-errOut.Done:
	case code := <-done:
		t.Fatalf("run exited with %d before applying a revision:\n%s", code, errOut.String())
	}
	cancel()
	if code := <-done; code != 0 || out.Len() != 0 {
		t.Fatalf("code=%d out=%q err=%s", code, out.String(), errOut.String())
	}
	status, _ := mustRun(t, 0, "--db", db, "status")
	if !regexp.MustCompile(`listings\s+active\s+1\s`).MatchString(status) {
		t.Fatalf("status:\n%s", status)
	}
}
