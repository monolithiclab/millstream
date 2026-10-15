package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

func Test_runOnce(t *testing.T) {
	t.Parallel()
	pkg, db := runPackage(t, "run"), dbPath(t)
	out, errOut := mustRun(t, 0, "--db", db, "run", "--once", "--request-id", "r1", pkg)
	want := "listings run_r1_adhoc succeeded\n  listings 3\n  trigger.adhoc 1\n"
	if out != want {
		t.Fatalf("summary:\n%s\nwant:\n%s", out, want)
	}
	logged := false
	for _, line := range splitLines(errOut) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("stderr line is not JSON: %q", line)
		}
		if m["msg"] == "delivery done" {
			logged = true
			for _, k := range []string{"pipeline", "runId", "step", "itemId", "deliveryId", "attempt"} {
				if _, ok := m[k]; !ok {
					t.Fatalf("log line without %s: %s", k, line)
				}
			}
		}
	}
	if !logged {
		t.Fatalf("no delivery logged:\n%s", errOut)
	}

	// A rerun with the same request id resumes the same run instead of firing a new one.
	out, _ = mustRun(t, 0, "--db", db, "run", "--once", "--request-id", "r1", pkg)
	if out != want {
		t.Fatalf("rerun summary:\n%s", out)
	}
	status, _ := mustRun(t, 0, "--db", db, "status")
	if strings.Count(status, "run_") != 1 {
		t.Fatalf("status after the rerun lists other runs:\n%s", status)
	}
}

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

func Test_runRejects(t *testing.T) {
	t.Parallel()
	pkg := runPackage(t, "run")
	tests := []struct {
		name    string
		argv    []string
		code    int
		wantErr string
	}{
		{"check errors", []string{"run", "--once", fixture("unknown-stream")}, 1,
			`"path":"steps.b.in[0]","message":"stream listing is read but no step writes it"`},
		{"no package", []string{"run", "--once", fixture("nope")}, 1, "load package"},
		{"undeclared var", []string{"run", "--once", "--var", "nope=1", pkg}, 2,
			"--var nope: no pipeline declares this variable"},
		{"bad slots", []string{"run", "--slots", "0", pkg}, 2, "--slots and --lease-ttl must be positive"},
		{"unresolved var", []string{"run", "--once", "--var", "count=many", pkg}, 1, "variable count from flag"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			code, _, errOut := runCLICtx(t, context.Background(), nil, append([]string{"--db", dbPath(t)}, tt.argv...)...)
			if code != tt.code || !strings.Contains(errOut, tt.wantErr) {
				t.Fatalf("code=%d, want %d; stderr:\n%s\nwant %q", code, tt.code, errOut, tt.wantErr)
			}
		})
	}
}
