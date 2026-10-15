package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func Test_triggerCommand(t *testing.T) {
	t.Parallel()
	s := newState(t, false)
	out, _ := s.must(0, "trigger", "listings", "adhoc", "--payload", `{"city":"Lyon"}`, "--request-id", "t1",
		"--var", "count=5")
	if out != "run_t1_adhoc\n" {
		t.Fatalf("stdout = %q", out)
	}
	out, _ = s.must(0, "trigger", "listings", "adhoc", "--request-id", "t1")
	if out != "run_t1_adhoc (already fired)\n" {
		t.Fatalf("refire stdout = %q", out)
	}
	out, _ = s.must(0, "trigger", "listings", "adhoc")
	if !regexp.MustCompile(`^run_\d{8}T\d{6}Z-[0-9a-f]{8}_adhoc\n$`).MatchString(out) {
		t.Fatalf("generated id stdout = %q", out)
	}
	s.checkRefusals(t, nil, []refusal{
		{TriggerCmd{Pipeline: "listings", Trigger: "adhoc", Payload: "{"}, true, "--payload: not valid JSON"},
		{TriggerCmd{Pipeline: "listings", Trigger: "adhoc", Payload: "{}", Var: map[string]string{"nope": "1"}}, true,
			"declares no such variable"},
		{TriggerCmd{Pipeline: "other", Trigger: "adhoc", Payload: "{}", Var: map[string]string{"nope": "1"}}, false,
			"pipeline other: not found"},
		{TriggerCmd{Pipeline: "listings", Trigger: "weekly", Payload: "{}"}, false, `has no trigger "weekly"`},
	})
	status, _ := s.must(0, "status", "--pipeline", "listings", "--limit", "5")
	if !regexp.MustCompile(`run_t1_adhoc\s+listings\s+running\s+\S+\s+1\s+0`).MatchString(status) {
		t.Fatalf("status:\n%s", status)
	}
}

func Test_cancelCommand(t *testing.T) {
	t.Parallel()
	s := newState(t, false)
	s.must(0, "trigger", "listings", "adhoc", "--request-id", "c1")
	out, _ := s.must(0, "cancel", "listings", "run_c1_adhoc")
	if out != "run_c1_adhoc cancelled\n" {
		t.Fatalf("stdout = %q", out)
	}
	_, errOut := s.must(1, "cancel", "listings", "run_r1_adhoc")
	if !strings.Contains(errOut, "conflict") {
		t.Fatalf("cancelling a settled run: %q", errOut)
	}
	s.must(1, "cancel", "listings", "nope")
}

func Test_pauseAndResumeCommands(t *testing.T) {
	t.Parallel()
	s := newState(t, false)
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
	for _, step := range steps {
		out, _ := s.must(0, step.argv...)
		if out != step.out {
			t.Fatalf("%v: stdout %q, want %q", step.argv, out, step.out)
		}
		status := s.status()
		if !step.want.MatchString(status) {
			t.Fatalf("%v: status:\n%s", step.argv, status)
		}
	}
	s.must(1, "pause", "nope")
	if !e2eBuild {
		return
	}

	// A budget pauses the agent steps with its cause (store tests cover how); status shows it and resume lifts it,
	// leaving a step a user paused as it is. The test writes the cause into the database file: e2e build only.
	sqldb, err := sql.Open("sqlite", s.db)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sqldb.Exec(`INSERT OR REPLACE INTO steps_state (pipeline, step, paused, cause) VALUES ('listings', 'score', 1,
		'budget.day'), ('listings', 'crawl', 1, '')`)
	if cerr := sqldb.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatal(err)
	}
	status := s.status()
	if !regexp.MustCompile(`listings\s+active\s+1\s+crawl,score \(budget\.day\)`).MatchString(status) {
		t.Fatalf("status:\n%s", status)
	}
	s.must(0, "resume", "listings")
	status = s.status()
	if !regexp.MustCompile(`listings\s+active\s+1\s+crawl\n`).MatchString(status) {
		t.Fatalf("status after resume:\n%s", status)
	}
}

func Test_dlqAndReplayCommands(t *testing.T) {
	t.Parallel()
	s := newState(t, true)
	out, _ := s.must(0, "dlq", "list", "--pipeline", "listings", "--run", "run_r1_adhoc")
	lines := splitLines(out)
	if len(lines) != 4 || !strings.HasPrefix(lines[0], "ID") || !strings.Contains(lines[1], "score") ||
		!strings.Contains(lines[1], "portal answered 503") {
		t.Fatalf("dlq list:\n%s", out)
	}
	ids := make([]string, 0, 3)
	for _, l := range lines[1:] {
		ids = append(ids, strings.Fields(l)[0])
	}
	out, _ = s.must(0, "dlq", "discard", "--pipeline", "listings", ids[0])
	if out != "1 dead letter discarded\n" {
		t.Fatalf("discard stdout = %q", out)
	}
	s.checkRefusals(t, nil, []refusal{{DLQDiscardCmd{Pipeline: "listings", IDs: ids[:1]}, false, "not found"}})
	out, _ = s.must(0, "dlq", "list", "--state", "all")
	if len(splitLines(out)) != 4 || !strings.Contains(out, "discarded") {
		t.Fatalf("dlq list --state all:\n%s", out)
	}
	out, _ = s.must(0, "replay", "--from", "dlq", "--pipeline", "listings", "--run", "run_r1_adhoc", "--migrate", ids[1])
	if out != "1 dead letter replayed\n" {
		t.Fatalf("replay stdout = %q", out)
	}
	status := s.status()
	if !regexp.MustCompile(`run_r1_adhoc\s+listings\s+running`).MatchString(status) {
		t.Fatalf("status after replay:\n%s", status)
	}
	replayed := "1 dead letter replayed\n"
	if e2eBuild {
		// The rerun waits for the reopened run, whose stored vars still fail the step.
		out, _ = s.must(1, "run", "--once", "--request-id", "r1", s.pkg)
		if !strings.HasPrefix(out, "listings run_r1_adhoc completedWithErrors\n") {
			t.Fatalf("rerun summary:\n%s", out)
		}
		replayed = "2 dead letters replayed\n"
	}
	out, _ = s.must(0, "replay", "--from", "dlq", "--pipeline", "listings", "--run", "run_r1_adhoc")
	if out != replayed {
		t.Fatalf("replay all stdout = %q, want %q", out, replayed)
	}
	s.checkRefusals(t, nil, []refusal{
		{ReplayCmd{From: "dlq", Pipeline: "listings", RunID: "nope"}, false, "run nope: not found"},
	})
}

func Test_replayStepCommand(t *testing.T) {
	t.Parallel()
	s := newState(t, false)
	replay := func(want int, args ...string) (string, string) {
		t.Helper()
		return s.must(want, append([]string{"replay", "--pipeline", "listings"}, args...)...)
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
	if e2eBuild {
		// run --once waits for the replay runs too, and reports them.
		out, _ = s.must(0, "run", "--once", "--request-id", "r1", s.pkg)
		if !strings.Contains(out, "listings run_replay_s1 succeeded\n") {
			t.Fatalf("summary:\n%s", out)
		}
	}
	// Through the parser: a usage error exits 2, a refusal of the engine 1.
	if code, _, errOut := s.run("replay", "--pipeline", "listings", "--from", "dlq", "--step", "score"); code != 2 ||
		!strings.Contains(errOut, "--from and --step are mutually exclusive") {
		t.Errorf("--from and --step: code %d, stderr %q", code, errOut)
	}
	if code, _, errOut := s.run("replay", "--pipeline", "listings", "--step", "nope"); code != 1 ||
		!strings.Contains(errOut, `has no step "nope"`) {
		t.Errorf("--step nope: code %d, stderr %q", code, errOut)
	}
	replayCmd := func(c ReplayCmd) ReplayCmd { c.Pipeline = "listings"; return c }
	s.checkRefusals(t, nil, []refusal{
		{replayCmd(ReplayCmd{}), true, "replay needs --from dlq or --step"},
		{replayCmd(ReplayCmd{From: "later"}), true, "--from later: only dlq is supported"},
		{replayCmd(ReplayCmd{From: "dlq"}), true, "--from dlq needs --run"},
		{replayCmd(ReplayCmd{From: "dlq", RunID: "run_r1_adhoc", Since: "7d"}), true, "go with --step"},
		{replayCmd(ReplayCmd{Step: "score", Migrate: true}), true, "go with --from dlq"},
		{replayCmd(ReplayCmd{Step: "score", IDs: []string{"it_x"}}), true, "go with --from dlq"},
		{replayCmd(ReplayCmd{Step: "score", Since: "soon"}), true, "--since: invalid duration"},
		{replayCmd(ReplayCmd{Step: "score", Var: map[string]string{"nope": "1"}}), true, "declares no such variable"},
		{replayCmd(ReplayCmd{Step: "score", Where: "item.n >"}), false, "--where"},
		{replayCmd(ReplayCmd{Step: "score", RunID: "nope"}), false, "selects no item"},
	})
}

func Test_tailCommand(t *testing.T) {
	t.Parallel()
	s := newState(t, false)
	out, _ := s.must(0, "tail")
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
	out, _ = s.must(0, "tail", "--from", "2")
	if len(splitLines(out)) != len(lines)-2 {
		t.Fatalf("tail --from 2 printed %d lines of %d", len(splitLines(out)), len(lines))
	}
	// --follow prints the events from --from, then waits --interval and polls again, until interrupted: the test
	// interrupts it once it has printed them all, during its first wait.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	follow := newWatchBuffer(func(s string) bool { return len(splitLines(s)) >= len(lines)-1 })
	go func() { <-follow.Done; cancel() }()
	code, _ := s.exec(ctx, nil, follow, "tail", "--follow", "--interval", "1h", "--from", "1")
	if code != 0 || len(splitLines(follow.String())) != len(lines)-1 {
		t.Fatalf("tail --follow: code %d, %d lines", code, len(splitLines(follow.String())))
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
