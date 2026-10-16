package cli

import (
	"context"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/eval"
)

// Test_evalCommandFake: eval scores the fake action's case in-process and, with --db, records the score and shows
// the previous one; what eval cannot run exits 2 with why. Test_evalCommand (e2e) adds a judge and failing cases.
func Test_evalCommandFake(t *testing.T) {
	t.Parallel()
	pkg, lone := sharedPackage(t, "eval"), fixture("eval-lone")
	action := filepath.Join(pkg, "actions", "fake")
	s := newEmptyState(t)
	for i, want := range []string{`keyed\s+1/1\s+-\s+\$0\.0000\s+1\.00\s+-\n`, `keyed\s+1/1\s+-\s+\$0\.0000\s+1\.00\s+1\.00\n`} {
		if out, _ := s.must(0, "eval", action); !regexp.MustCompile(want).MatchString(out) {
			t.Fatalf("eval %d:\n%s", i, out)
		}
	}
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
		{evalCmd(EvalCmd{Action: action, Package: lone}), true, "is not inside the package"},
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
