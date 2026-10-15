package cli

import (
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/scaffold"
)

// builtActions builds, once, the SDK actions the tests share: the Go action millstream init scaffolds, against
// this repository's SDK (no network: GOPROXY=off), and the M1 fixture's score action in a copy of its package.
var builtActions = sync.OnceValues(func() (map[string]string, error) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return nil, err
	}
	platform := runtime.GOOS + "-" + runtime.GOARCH
	greeter, err := scaffold.Action(testDir, "greeter", scaffold.LangGo)
	if err != nil {
		return nil, err
	}
	m1 := filepath.Join(testDir, "m1-fixture")
	if err := os.CopyFS(m1, os.DirFS(filepath.Join(root, "examples", "m1-fixture"))); err != nil {
		return nil, err
	}
	for _, c := range []struct {
		dir  string
		args []string
	}{
		{greeter, []string{"mod", "edit", "-replace", "github.com/monolithiclab/millstream/sdk/go=" +
			filepath.Join(root, "sdk", "go")}},
		{greeter, []string{"build", "-o", filepath.Join("bin", platform, "greeter"), "."}},
		{root, []string{"build", "-o", filepath.Join(m1, "actions", "score", "bin", platform, "score"),
			"./examples/m1-fixture/actions/score"}},
	} {
		cmd := osexec.Command("go", c.args...) // #nosec G204 -- fixed test arguments
		cmd.Dir = c.dir
		cmd.Env = append(os.Environ(), "GOPROXY=off", "GOWORK=off", "GOFLAGS=")
		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("go %v: %w\n%s", c.args, err, out)
		}
	}
	return map[string]string{"greeter": greeter, "score": filepath.Join(m1, "actions", "score")}, nil
})

// sdkAction returns a private copy of the scaffolded Go action, built.
func sdkAction(t *testing.T) string {
	t.Helper()
	built, err := builtActions()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "greeter")
	if err := os.CopyFS(dir, os.DirFS(built["greeter"])); err != nil {
		t.Fatal(err)
	}
	return dir
}

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

// Test_testAction: the scaffolded fixture passes on the lone action; a wrong golden output fails with a diff, error
// expectations compare retryability, and a package's action is found from its directory.
func Test_testAction(t *testing.T) {
	t.Parallel()
	dir := sdkAction(t)
	out, errOut := mustRun(t, 0, "test", "action", dir)
	if out != "PASS greets by name\n" || !strings.Contains(errOut, "greeter: 1 passed, 0 failed") {
		t.Fatalf("stdout %q, stderr %q", out, errOut)
	}

	writeFiles(t, dir, map[string]string{
		"tests/wrong.yaml": "input: { name: Ada }\nexpect: { outputs: [{ key: k, payload: { greeting: \"Hi, Ada!\" } }] }\n",
		"tests/bad-config.yaml": "name: config of the wrong type\ninput: { name: Ada }\nconfig: { greeting: 3 }\n" +
			"expect: { error: { retryable: false } }\n",
		"tests/retry.yaml": "input: { name: Ada }\nconfig: { greeting: 3 }\nexpect: { error: { retryable: true } }\n",
		"tests/none.yaml":  "input: { name: Ada }\nexpect: { outputs: [] }\n",
		"tests/fails.yaml": "input: {}\nexpect: { outputs: [] }\n",
		"tests/err.yaml":   "input: { name: Ada }\nexpect: { error: { retryable: false } }\n",
	})
	out, errOut = mustRun(t, 1, "test", "action", dir)
	for _, want := range []string{
		"PASS config of the wrong type\n",
		"FAIL err\n  want an error (retryable false), got done with 1 output(s)\n",
		"FAIL fails\n  want done, got an error (retryable false): the item has no name\n",
		"PASS greets by name\n",
		"FAIL none\n  outputs differ (- want, + got):\n    - []\n    + [\n",
		"FAIL retry\n  want an error with retryable true, got retryable false: decode config:",
		"FAIL wrong\n  outputs differ (- want, + got):\n      [\n        {\n    -     \"key\": \"k\",\n" +
			"    +     \"key\": \"\",\n          \"payload\": {\n    -       \"greeting\": \"Hi, Ada!\"\n" +
			"    +       \"greeting\": \"Hello, Ada!\"\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout lacks %q:\n%s", want, out)
		}
	}
	if !strings.Contains(errOut, "greeter: 2 passed, 5 failed") {
		t.Errorf("stderr %q", errOut)
	}

	pkg := runPackage(t, "run")
	writeFiles(t, filepath.Join(pkg, "actions", "fake"), map[string]string{
		"tests/keyed.yaml": "config: { mode: keyed, n: 2 }\n" +
			"expect: { outputs: [{ key: k0, payload: 0 }, { key: k1, payload: 1 }] }\n",
		"tests/down.yaml": "config: { mode: error, retryable: true }\nexpect: { error: { retryable: true } }\n",
	})
	if out, _ := mustRun(t, 0, "test", "action", filepath.Join(pkg, "actions", "fake")); out != "PASS down\nPASS keyed\n" {
		t.Fatalf("package action: %q", out)
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

// Test_testActionConformance: SDK actions, the M1 fixture's score action (no fixture: an input made from its schema)
// and the raw Python scaffold conform; the fake action's broken modes fail, naming the checks.
func Test_testActionConformance(t *testing.T) {
	t.Parallel()
	built, err := builtActions()
	if err != nil {
		t.Fatal(err)
	}
	conformant := "PASS ready: ready in"
	t.Run("sdk", func(t *testing.T) {
		t.Parallel()
		out, errOut := mustRun(t, 0, "test", "action", sdkAction(t), "--conformance")
		for _, want := range []string{conformant, "PASS unknown-keys:", "PASS framing:", "PASS item-answer: done with 1",
			"SKIP keyed-outputs:", "PASS idle-silence:", "PASS malformed-config: null config: done",
			"config of unexpected types: error (retryable false): decode config", "PASS shutdown: exited with status 0"} {
			if !strings.Contains(out, want) {
				t.Errorf("stdout lacks %q:\n%s", want, out)
			}
		}
		if !strings.Contains(errOut, "greeter: conformant") {
			t.Errorf("stderr %q", errOut)
		}
	})
	t.Run("m1 score", func(t *testing.T) {
		t.Parallel()
		out, _ := mustRun(t, 0, "test", "action", built["score"], "--conformance")
		if !strings.Contains(out, "PASS item-answer: done with 1 output(s)") {
			t.Errorf("stdout:\n%s", out)
		}
	})
	t.Run("raw python", func(t *testing.T) {
		t.Parallel()
		if _, err := osexec.LookPath("python3"); err != nil {
			t.Skip("python3 is not on PATH: the raw scaffold's conformance is not checked")
		}
		parent := t.TempDir()
		mustRun(t, 0, "init", "action", "greeter", "--lang", "raw", "--dir", parent)
		dir := filepath.Join(parent, "greeter")
		if out, _ := mustRun(t, 0, "test", "action", dir); out != "PASS greets by name\n" {
			t.Fatalf("fixtures: %q", out)
		}
		out, _ := mustRun(t, 0, "test", "action", dir, "--conformance")
		if !strings.Contains(out, "PASS malformed-config: null config: done with 1 output(s) and 0 metric(s); "+
			"config of unexpected types: error (retryable false): config.greeting must be a string") {
			t.Errorf("stdout:\n%s", out)
		}
	})

	tests := []struct {
		mode string
		want []string
	}{
		{"keyed", []string{conformant, "PASS keyed-outputs: 3 outputs with distinct keys", "PASS idle-silence:"}},
		{"garbage", []string{conformant, "FAIL framing: main: protocol violation: malformed line",
			`(line "this is not json")`, "FAIL item-answer: protocol violation", "SKIP idle-silence: needs item-answer",
			"PASS shutdown:"}},
		{"noReady", []string{"FAIL ready: no ready within 300ms", "SKIP item-answer: needs ready",
			"FAIL unknown-keys: hello with an unknown key: no ready"}},
		{"hang", []string{conformant, "FAIL item-answer: no done or error for the item within 300ms",
			"FAIL shutdown: did not exit within 300ms of shutdown"}},
		{"emitAfterDone", []string{"FAIL idle-silence: wrote emit between deliveries"}},
		{"wrongId", []string{`FAIL item-answer: wrote emit for delivery "dl_other", want "dl_conformance_1"`}},
		{"crash", []string{"FAIL item-answer: exited (exit status 3) before done or error for the item"}},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			t.Parallel()
			pkg := runPackage(t, "run")
			action := filepath.Join(pkg, "actions", "fake")
			writeFiles(t, action, map[string]string{
				"tests/a.yaml": fmt.Sprintf("config: { mode: %s, n: 3 }\nexpect: { outputs: [] }\n", tt.mode),
			})
			code := 1
			if tt.mode == "keyed" {
				code = 0
			}
			out, errOut := mustRun(t, code, "test", "action", action, "--conformance", "--timeout", "300ms")
			for _, want := range tt.want {
				if !strings.Contains(out, want) {
					t.Errorf("stdout lacks %q:\n%s", want, out)
				}
			}
			if code == 1 && !strings.Contains(errOut, "fake: not conformant") {
				t.Errorf("stderr %q", errOut)
			}
		})
	}
}
