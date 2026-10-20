package cli

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

// runOnceSummary is what run --once of testdata/run prints.
const runOnceSummary = "listings run_r1_adhoc succeeded\n  listings 3\n  trigger.adhoc 1\n"

// Test_runOnce runs testdata/run named by its directory, twice: the e2e build. The default build runs it from its
// directory instead (Test_runOnceCurrentDir), with the same checks.
func Test_runOnce(t *testing.T) {
	if !e2eBuild {
		t.Skip("Test_runOnceCurrentDir runs the package in the default build")
	}
	t.Parallel()
	pkg, s := sharedPackage(t, "run"), newEmptyState(t)
	out, errOut := s.must(0, "run", "--once", "--request-id", "r1", pkg)
	if out != runOnceSummary {
		t.Fatalf("summary:\n%s\nwant:\n%s", out, runOnceSummary)
	}
	checkRunLogs(t, errOut)

	// A rerun with the same request id resumes the same run instead of firing a new one: a second engine run.
	out, _ = s.must(0, "run", "--once", "--request-id", "r1", pkg)
	if out != runOnceSummary {
		t.Fatalf("rerun summary:\n%s", out)
	}
	status, _ := s.must(0, "status")
	if strings.Count(status, "run_") != 1 {
		t.Fatalf("status after the rerun lists other runs:\n%s", status)
	}
}

// checkRunLogs checks run's stderr: JSON lines, a first dispatch under the engine id, and deliveries logged with
// their ids.
func checkRunLogs(t *testing.T, errOut string) {
	t.Helper()
	logged, dispatched := false, false
	for _, line := range splitLines(errOut) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("stderr line is not JSON: %q", line)
		}
		if m["msg"] == "first dispatch" {
			// The engine logs under the id its embedded node is named after.
			dispatched = true
			if id, _ := m["engine"].(string); id == "" {
				t.Fatalf("first dispatch without the engine id: %s", line)
			}
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
	if !logged || !dispatched {
		t.Fatalf("no delivery or no first dispatch logged:\n%s", errOut)
	}
}

func Test_runRejects(t *testing.T) {
	t.Parallel()
	pkg, s := sharedPackage(t, "run"), newEmptyState(t)
	if code, errOut := s.exec(t.Context(), map[string]string{"USER": "ana"}, io.Discard, "run", "--slots", "0",
		pkg); code != 2 || !strings.Contains(errOut, "--slots and --lease-ttl must be positive") {
		t.Fatalf("--slots 0: code %d, stderr %q", code, errOut)
	}
	// Serving the API needs someone to record the decisions of requests naming nobody under: it fails at once.
	if code, _, errOut := s.run("run", pkg); code != 2 || !strings.Contains(errOut, "no user to record review decisions") {
		t.Fatalf("no user: code %d, stderr %q", code, errOut)
	}
	// run serves the API on a loopback address only (serve listens elsewhere with auth.token); run --once serves
	// nothing.
	for _, tt := range []struct{ argv, want []string }{
		{[]string{"run", "--listen", "0.0.0.0:7070", pkg}, []string{"not a loopback address", "without auth.token"}},
		{[]string{"run", "--listen", "7070", pkg}, []string{"want host:port"}},
		{[]string{"run", "--once", "--listen", "127.0.0.1:7070", pkg}, []string{"run --once does not serve"}},
	} {
		code, _, errOut := s.run(tt.argv...)
		for _, w := range tt.want {
			if code != 2 || !strings.Contains(errOut, w) {
				t.Errorf("%v: code %d, stderr %q, want %q", tt.argv, code, errOut, w)
			}
		}
	}
	run := func(c RunPackagesCmd) RunPackagesCmd {
		c.Once, c.Slots, c.LeaseTTL = true, 4, time.Minute
		return c
	}
	s.checkRefusals(t, nil, []refusal{
		{run(RunPackagesCmd{Dirs: []string{fixture("unknown-stream")}}), false,
			`"path":"steps.b.in[0]","message":"stream listing is read but no step writes it"`},
		{run(RunPackagesCmd{Dirs: []string{fixture("nope")}}), false, "load package"},
		{run(RunPackagesCmd{Dirs: []string{pkg}, Var: map[string]string{"nope": "1"}}), true,
			"--var nope: no pipeline declares this variable"},
		{run(RunPackagesCmd{Dirs: []string{pkg}, Var: map[string]string{"count": "many"}}), false,
			"variable count from flag"},
	})
}
