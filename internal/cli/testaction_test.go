package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFiles writes files (path relative to dir: content) under dir.
func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// Test_testActionFake: test action runs the package's fake action in-process (testLaunchers) against fixtures that
// pass and fail each way: outputs compared by key and payload with a diff, error expectations compared by
// retryability, and done where an error was expected and the reverse. The e2e build runs the same fixtures against
// the fake action binary, and Test_testAction against a built SDK action.
func Test_testActionFake(t *testing.T) {
	t.Parallel()
	pkg := runPackage(t, "run")
	action := filepath.Join(pkg, "actions", "fake")
	writeFiles(t, action, map[string]string{
		"tests/keyed.yaml": "config: { mode: keyed, n: 2 }\n" +
			"expect: { outputs: [{ key: k0, payload: 0 }, { key: k1, payload: 1 }] }\n",
		"tests/down.yaml": "config: { mode: error, retryable: true }\nexpect: { error: { retryable: true } }\n",
	})
	if out, errOut := mustRun(t, 0, "test", "action", action); out != "PASS down\nPASS keyed\n" ||
		!strings.Contains(errOut, "fake: 2 passed, 0 failed") {
		t.Fatalf("stdout %q, stderr %q", out, errOut)
	}

	writeFiles(t, action, map[string]string{
		"tests/wrong.yaml": "config: { mode: keyed, n: 1 }\nexpect: { outputs: [{ key: k, payload: 1 }] }\n",
		"tests/err.yaml":   "config: { mode: keyed, n: 1 }\nexpect: { error: { retryable: false } }\n",
		"tests/fails.yaml": "config: { mode: error, retryable: false }\nexpect: { outputs: [] }\n",
		"tests/retry.yaml": "config: { mode: error, retryable: false }\nexpect: { error: { retryable: true } }\n",
	})
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
	pkg := runPackage(t, "run")
	fake := filepath.Join(pkg, "actions", "fake")
	bad := t.TempDir()
	writeFiles(t, bad, map[string]string{
		"action.yaml":    "name: bad\nkind: exec\nruntime: { exec: { command: [./bad] } }\n",
		"bad":            "",
		"tests/x.yaml":   "input: 1\nexpect: { outputs: [], error: { retryable: true } }\n",
		"tests/y.yaml":   "nope: 1\n",
		"tests/z.yaml.b": "",
	})
	tests := []struct {
		argv []string
		code int
		want string
	}{
		{[]string{"test", "action", fake}, 2, "no test case in"},
		{[]string{"test", "action", fake, "--package", t.TempDir()}, 2, "is not inside the package"},
		{[]string{"test", "action", fake, "--harness-mode", "dry"}, 2, "--harness-mode"},
		{[]string{"test", "action", bad}, 1, "expect needs outputs or error, not both"},
		{[]string{"test", "action", t.TempDir()}, 1, "action.yaml in the package"},
	}
	for _, tt := range tests {
		if _, errOut := mustRun(t, tt.code, tt.argv...); !strings.Contains(errOut, tt.want) {
			t.Errorf("%v: stderr %q, want %q", tt.argv, errOut, tt.want)
		}
	}
}
