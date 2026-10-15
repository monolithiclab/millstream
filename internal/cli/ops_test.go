package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// appliedDB returns a database where the run fixture is applied and one run settled.
func appliedDB(t *testing.T, extra ...string) (db, pkg string) {
	t.Helper()
	pkg, db = runPackage(t, "run"), dbPath(t)
	argv := append([]string{"--db", db, "run", "--once", "--request-id", "r1"}, extra...)
	code, _, errOut := runCLICtx(t, context.Background(), nil, append(argv, pkg)...)
	if code > 1 {
		t.Fatalf("run: code %d\n%s", code, errOut)
	}
	return db, pkg
}

func Test_triggerCommand(t *testing.T) {
	t.Parallel()
	db, _ := appliedDB(t)
	out, _ := mustRun(t, 0, "--db", db, "trigger", "listings", "adhoc", "--payload", `{"city":"Lyon"}`,
		"--request-id", "t1", "--var", "count=5")
	if out != "run_t1_adhoc\n" {
		t.Fatalf("stdout = %q", out)
	}
	out, _ = mustRun(t, 0, "--db", db, "trigger", "listings", "adhoc", "--request-id", "t1")
	if out != "run_t1_adhoc (already fired)\n" {
		t.Fatalf("refire stdout = %q", out)
	}
	out, _ = mustRun(t, 0, "--db", db, "trigger", "listings", "adhoc")
	if !regexp.MustCompile(`^run_\d{8}T\d{6}Z-[0-9a-f]{8}_adhoc\n$`).MatchString(out) {
		t.Fatalf("generated id stdout = %q", out)
	}
	tests := []struct {
		argv    []string
		code    int
		wantErr string
	}{
		{[]string{"trigger", "listings", "adhoc", "--payload", "{"}, 2, "--payload: not valid JSON"},
		{[]string{"trigger", "listings", "adhoc", "--var", "nope=1"}, 2, "declares no such variable"},
		{[]string{"trigger", "other", "adhoc", "--var", "nope=1"}, 1, "pipeline other: not found"},
		{[]string{"trigger", "listings", "weekly"}, 1, `has no trigger "weekly"`},
	}
	for _, tt := range tests {
		code, _, errOut := runCLICtx(t, context.Background(), nil, append([]string{"--db", db}, tt.argv...)...)
		if code != tt.code || !strings.Contains(errOut, tt.wantErr) {
			t.Fatalf("%v: code %d, stderr %q; want %d, %q", tt.argv, code, errOut, tt.code, tt.wantErr)
		}
	}
	status, _ := mustRun(t, 0, "--db", db, "status", "--pipeline", "listings", "--limit", "5")
	if !regexp.MustCompile(`run_t1_adhoc\s+listings\s+running\s+\S+\s+1\s+0`).MatchString(status) {
		t.Fatalf("status:\n%s", status)
	}
}

func Test_cancelCommand(t *testing.T) {
	t.Parallel()
	db, _ := appliedDB(t)
	mustRun(t, 0, "--db", db, "trigger", "listings", "adhoc", "--request-id", "c1")
	out, _ := mustRun(t, 0, "--db", db, "cancel", "listings", "run_c1_adhoc")
	if out != "run_c1_adhoc cancelled\n" {
		t.Fatalf("stdout = %q", out)
	}
	_, errOut := mustRun(t, 1, "--db", db, "cancel", "listings", "run_r1_adhoc")
	if !strings.Contains(errOut, "conflict") {
		t.Fatalf("cancelling a settled run: %q", errOut)
	}
	mustRun(t, 1, "--db", db, "cancel", "listings", "nope")
}

func Test_pauseAndResumeCommands(t *testing.T) {
	t.Parallel()
	db, _ := appliedDB(t)
	steps := []struct {
		argv []string
		out  string
		want *regexp.Regexp
	}{
		{[]string{"pause", "listings"}, "listings paused\n", regexp.MustCompile(`listings\s+paused\s+1\s+-`)},
		{[]string{"resume", "listings"}, "listings resumed\n", regexp.MustCompile(`listings\s+active\s+1\s+-`)},
		{[]string{"pause", "listings", "score"}, "listings.score paused\n", regexp.MustCompile(`listings\s+active\s+1\s+score`)},
		{[]string{"resume", "listings", "score"}, "listings.score resumed\n", regexp.MustCompile(`listings\s+active\s+1\s+-`)},
	}
	for _, s := range steps {
		out, _ := mustRun(t, 0, append([]string{"--db", db}, s.argv...)...)
		if out != s.out {
			t.Fatalf("%v: stdout %q, want %q", s.argv, out, s.out)
		}
		status, _ := mustRun(t, 0, "--db", db, "status")
		if !s.want.MatchString(status) {
			t.Fatalf("%v: status:\n%s", s.argv, status)
		}
	}
	mustRun(t, 1, "--db", db, "pause", "nope")
}

func Test_dlqAndReplayCommands(t *testing.T) {
	t.Parallel()
	db, pkg := appliedDB(t, "--var", "scoreMode=error")
	out, _ := mustRun(t, 0, "--db", db, "dlq", "list", "--pipeline", "listings", "--run", "run_r1_adhoc")
	lines := splitLines(out)
	if len(lines) != 4 || !strings.HasPrefix(lines[0], "ID") || !strings.Contains(lines[1], "score") ||
		!strings.Contains(lines[1], "portal answered 503") {
		t.Fatalf("dlq list:\n%s", out)
	}
	ids := make([]string, 0, 3)
	for _, l := range lines[1:] {
		ids = append(ids, strings.Fields(l)[0])
	}
	out, _ = mustRun(t, 0, "--db", db, "dlq", "discard", "--pipeline", "listings", ids[0])
	if out != "1 dead letter discarded\n" {
		t.Fatalf("discard stdout = %q", out)
	}
	mustRun(t, 1, "--db", db, "dlq", "discard", "--pipeline", "listings", ids[0])
	out, _ = mustRun(t, 0, "--db", db, "dlq", "list", "--state", "all")
	if len(splitLines(out)) != 4 || !strings.Contains(out, "discarded") {
		t.Fatalf("dlq list --state all:\n%s", out)
	}
	out, _ = mustRun(t, 0, "--db", db, "replay", "--from", "dlq", "--pipeline", "listings", "--run", "run_r1_adhoc",
		"--migrate", ids[1])
	if out != "1 dead letter replayed\n" {
		t.Fatalf("replay stdout = %q", out)
	}
	status, _ := mustRun(t, 0, "--db", db, "status")
	if !regexp.MustCompile(`run_r1_adhoc\s+listings\s+running`).MatchString(status) {
		t.Fatalf("status after replay:\n%s", status)
	}
	// The rerun waits for the reopened run, whose stored vars still fail the step.
	out, _ = mustRun(t, 1, "--db", db, "run", "--once", "--request-id", "r1", pkg)
	if !strings.HasPrefix(out, "listings run_r1_adhoc completedWithErrors\n") {
		t.Fatalf("rerun summary:\n%s", out)
	}
	out, _ = mustRun(t, 0, "--db", db, "replay", "--from", "dlq", "--pipeline", "listings", "--run", "run_r1_adhoc")
	if out != "2 dead letters replayed\n" {
		t.Fatalf("replay all stdout = %q", out)
	}
	mustRun(t, 1, "--db", db, "replay", "--from", "dlq", "--pipeline", "listings", "--run", "nope")
}

func Test_replayStepCommand(t *testing.T) {
	t.Parallel()
	db, pkg := appliedDB(t)
	replay := func(want int, args ...string) (string, string) {
		t.Helper()
		return mustRun(t, want, append([]string{"--db", db, "replay", "--pipeline", "listings"}, args...)...)
	}
	out, _ := replay(0, "--step", "score", "--run", "run_r1_adhoc", "--since", "7d", "--revision", "1",
		"--where", "envelope.key != ''", "--request-id", "s1", "--var", "count=3")
	if out != "run_replay_s1\n" {
		t.Fatalf("replay stdout = %q", out)
	}
	if out, _ = replay(0, "--step", "score", "--request-id", "s1"); out != "run_replay_s1 (already started)\n" {
		t.Fatalf("replay again stdout = %q", out)
	}
	out, _ = replay(0, "--step", "score")
	if !regexp.MustCompile(`^run_replay_\d{8}T\d{6}Z-[0-9a-f]{8}\n$`).MatchString(out) {
		t.Fatalf("generated id stdout = %q", out)
	}
	// run --once waits for the replay runs too, and reports them.
	out, _ = mustRun(t, 0, "--db", db, "run", "--once", "--request-id", "r1", pkg)
	if !strings.Contains(out, "listings run_replay_s1 succeeded\n") {
		t.Fatalf("summary:\n%s", out)
	}
	tests := []struct {
		args    []string
		code    int
		wantErr string
	}{
		{[]string{"--from", "dlq", "--step", "score"}, 2, "--from and --step are mutually exclusive"},
		{nil, 2, "replay needs --from dlq or --step"},
		{[]string{"--from", "later"}, 2, "--from later: only dlq is supported"},
		{[]string{"--from", "dlq"}, 2, "--from dlq needs --run"},
		{[]string{"--from", "dlq", "--run", "run_r1_adhoc", "--since", "7d"}, 2, "go with --step"},
		{[]string{"--step", "score", "--migrate"}, 2, "go with --from dlq"},
		{[]string{"--step", "score", "it_x"}, 2, "go with --from dlq"},
		{[]string{"--step", "score", "--since", "soon"}, 2, "--since: invalid duration"},
		{[]string{"--step", "score", "--var", "nope=1"}, 2, "declares no such variable"},
		{[]string{"--step", "score", "--where", "item.n >"}, 1, "--where"},
		{[]string{"--step", "nope"}, 1, `has no step "nope"`},
		{[]string{"--step", "score", "--run", "nope"}, 1, "selects no item"},
	}
	for _, tt := range tests {
		code, _, errOut := runCLICtx(t, context.Background(), nil,
			append([]string{"--db", db, "replay", "--pipeline", "listings"}, tt.args...)...)
		if code != tt.code || !strings.Contains(errOut, tt.wantErr) {
			t.Errorf("%v: code %d, stderr %q; want %d, %q", tt.args, code, errOut, tt.code, tt.wantErr)
		}
	}
}

func Test_tailCommand(t *testing.T) {
	t.Parallel()
	db, _ := appliedDB(t)
	out, _ := mustRun(t, 0, "--db", db, "tail")
	lines := splitLines(out)
	if len(lines) < 5 {
		t.Fatalf("tail:\n%s", out)
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first["seq"] != float64(1) || first["kind"] != "revisionApplied" || first["pipeline"] != "listings" {
		t.Fatalf("first event = %v", first)
	}
	out, _ = mustRun(t, 0, "--db", db, "tail", "--from", "2")
	if len(splitLines(out)) != len(lines)-2 {
		t.Fatalf("tail --from 2 printed %d lines of %d", len(splitLines(out)), len(lines))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	code, out, _ := runCLICtx(t, ctx, nil, "--db", db, "tail", "--follow", "--interval", "20ms",
		"--from", "1")
	if code != 0 || len(splitLines(out)) != len(lines)-1 {
		t.Fatalf("tail --follow: code %d, %d lines", code, len(splitLines(out)))
	}
}

func Test_storeCommandsFailOnUnopenableDB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mustRun(t, 1, "--db", dir, "status") // a directory is not a database
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, errOut := mustRun(t, 1, "--db", filepath.Join(file, "x.db"), "tail") // the directory cannot be created
	if !strings.Contains(errOut, "create database directory") {
		t.Fatalf("stderr = %q", errOut)
	}
}

func Test_oneLine(t *testing.T) {
	t.Parallel()
	tests := []struct{ in, want string }{
		{"short", "short"},
		{"two\nlines  here", "two lines here"},
		{strings.Repeat("é", 25), strings.Repeat("é", 19) + "…"},
	}
	for _, tt := range tests {
		if got := oneLine(tt.in, 20); got != tt.want {
			t.Errorf("oneLine(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
