package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Test_testActionFake: test action runs the package's fake action in-process (testLaunchers) against fixtures that
// pass and fail each way: outputs compared by key and payload with a diff, error expectations compared by
// retryability, and done where an error was expected and the reverse. The e2e build runs the same fixtures against
// the fake action binary, and Test_testAction against a built SDK action.
func Test_testActionFake(t *testing.T) {
	t.Parallel()
	action := filepath.Join(sharedPackage(t, "testaction"), "actions", "fake")
	if e2eBuild { // the passing fixtures alone exit 0; the default build runs them all once, below
		passing := filepath.Join(runPackage(t, "testaction"), "actions", "fake")
		for _, name := range []string{"wrong", "err", "fails", "retry"} {
			if err := os.Remove(filepath.Join(passing, "tests", name+".yaml")); err != nil {
				t.Fatal(err)
			}
		}
		if out, errOut := mustRun(t, 0, "test", "action", passing); out != "PASS down\nPASS keyed\n" ||
			!strings.Contains(errOut, "fake: 2 passed, 0 failed") {
			t.Fatalf("stdout %q, stderr %q", out, errOut)
		}
	}
	out, errOut := mustRun(t, 1, "test", "action", action)
	for _, want := range []string{
		"PASS down\n",
		"FAIL err\n  want an error (retryable false), got done with 1 output(s)\n",
		"FAIL fails\n  want done, got an error (retryable false): portal answered 503",
		"PASS keyed\n",
		"FAIL retry\n  want an error with retryable true, got retryable false: portal answered 503",
		"FAIL wrong\n  outputs differ (- want, + got):\n",
		"-     \"key\": \"k\",\n", "+     \"key\": \"k0\",\n", "-     \"payload\": 1\n", "+     \"payload\": 0\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout lacks %q:\n%s", want, out)
		}
	}
	if !strings.Contains(errOut, "fake: 2 passed, 4 failed") {
		t.Errorf("stderr %q", errOut)
	}
}

// Test_testActionUsage: what test action cannot run exits 2, an unreadable fixture 1.
func Test_testActionUsage(t *testing.T) {
	t.Parallel()
	fake := filepath.Join(sharedPackage(t, "run"), "actions", "fake")
	bad := fixture("testaction-bad")
	if _, errOut := mustRun(t, 2, "test", "action", fake, "--harness-mode", "dry"); !strings.Contains(errOut,
		"--harness-mode") {
		t.Errorf("--harness-mode dry: stderr %q", errOut)
	}
	if _, errOut := mustRun(t, 1, "test", "action", bad); !strings.Contains(errOut,
		"expect needs outputs or error, not both") {
		t.Errorf("bad fixture: stderr %q", errOut)
	}
	testAction := func(c TestActionCmd) TestActionCmd { c.HarnessMode, c.Timeout = "replay", 5*time.Second; return c }
	(&state{t: t}).checkRefusals(t, nil, []refusal{
		{testAction(TestActionCmd{Dir: fake}), true, "no test case in"},
		{testAction(TestActionCmd{Dir: fake, Package: bad}), true, "is not inside the package"},
		{testAction(TestActionCmd{Dir: filepath.Join(bad, "tests")}), false, "action.yaml in the package"},
	})
}
