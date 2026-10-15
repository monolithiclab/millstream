package cli

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/eval"
)

// writeEvals writes eval cases under the fake action of a copy of the run fixture and returns the action directory.
func writeEvals(t *testing.T, pkg string, cases map[string]string) string {
	t.Helper()
	action := filepath.Join(pkg, "actions", "fake")
	for name, body := range cases {
		p := filepath.Join(action, "evals", name)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return action
}

// Test_evalCommandUsage: what eval cannot run exits 2 with why.
func Test_evalCommandUsage(t *testing.T) {
	t.Parallel()
	pkg := runPackage(t, "run")
	action := writeEvals(t, pkg, map[string]string{"keyed.yaml": "config: { mode: keyed, n: 1 }\nassert: [output == 0]\n"})
	lone := t.TempDir()
	writeEvals(t, lone, map[string]string{"x.yaml": "assert: [\"true\"]\n"})
	if code, _, errOut := runCLICtx(t, context.Background(), nil, "eval", action, "--harness-mode", "dry"); code != 2 ||
		!strings.Contains(errOut, "--harness-mode") {
		t.Errorf("--harness-mode dry: code %d, stderr %q", code, errOut)
	}
	if code, _, errOut := runCLICtx(t, context.Background(), nil, "eval", action, "--case", "nope"); code != 2 ||
		!strings.Contains(errOut, "no eval case to run") {
		t.Errorf("--case nope: code %d, stderr %q", code, errOut)
	}
	evalCmd := func(c EvalCmd) EvalCmd { c.HarnessMode = "replay"; return c }
	(&state{t: t}).checkRefusals(t, nil, []refusal{
		{evalCmd(EvalCmd{Action: filepath.Join(pkg, "actions")}), true, "no eval case to run"},
		{evalCmd(EvalCmd{Action: action, Package: t.TempDir()}), true, "is not inside the package"},
		{evalCmd(EvalCmd{Action: filepath.Join(lone, "actions", "fake")}), true, "name the package with --package"},
		{evalCmd(EvalCmd{Action: filepath.Join(lone, "actions", "fake"), Package: lone}), false, "no pipeline.yaml"},
	})
}

// printEval shows a case's cost to four decimals, not at full float precision.
func Test_printEvalCost(t *testing.T) {
	t.Parallel()
	var out strings.Builder
	rt := &Runtime{IO: IO{Out: &out}}
	if err := printEval(rt, []eval.Result{{Case: "c", Passed: 1, Total: 1, CostUSD: 0.1 + 0.2, Score: 1}}, nil); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`c\s+1/1\s+-\s+\$0\.3000\s+1\.00\n`).MatchString(out.String()) {
		t.Fatalf("output:\n%s", out.String())
	}
}
