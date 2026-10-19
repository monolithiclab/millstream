package cli

import (
	"strings"
	"testing"
)

// Proposals from the command's own process, without the parser: created and validated, listed and shown, one
// accepted (the overlapping one goes stale and is not revalidated on the same base), rejected with its reason; a
// patch outside its kind's paths is refused; the proposals-only token is minted. Test_proposalsLifecycle (e2e) runs
// the same through the parser.
func Test_proposalsCommands(t *testing.T) {
	s := newEmptyState(t)
	s.must(0, "apply", fixture(lifecycleV1))
	env := map[string]string{"USER": "ana"}
	call := func(cmd runner, want string) string {
		t.Helper()
		out, errOut, err := s.call(env, cmd)
		if err != nil || !strings.Contains(out, want) {
			t.Fatalf("%T: %q, %q, %v; want %q", cmd, out, errOut, err, want)
		}
		return out
	}
	create := func(patch string) string {
		t.Helper()
		out := call(ProposalsCreateCmd{Patch: fixture("proposals/" + patch), Pipeline: "extra", Kind: "config",
			Rationale: "tighter"}, "validation passed: check passed, evals 0/0 passed, backtest passed")
		return strings.Fields(out)[0]
	}
	first, second := create("false.diff"), create("n.diff")
	call(ProposalsListCmd{State: "ready"}, second)
	call(ProposalsListCmd{State: "all", JSON: true}, `"id":"`+first+`"`)
	call(ProposalsShowCmd{ID: first}, "rationale: tighter")
	call(ProposalsShowCmd{ID: first, JSON: true}, `"createdBy":"ana"`)
	call(ProposalsAcceptCmd{ID: first}, first+" config accepted")
	call(ProposalsListCmd{State: "stale"}, second)
	if _, _, err := s.call(env, ProposalsRevalidateCmd{ID: second}); err == nil ||
		!strings.Contains(err.Error(), "comes first") {
		t.Fatalf("revalidate on the same base: %v", err)
	}
	call(ProposalsRejectCmd{ID: second, Reason: "one change at a time"}, second+" config rejected")
	call(ProposalsListCmd{State: "rejected"}, "one change at a time")
	call(ProposalsShowCmd{ID: second}, "decided by ana: one change at a time")
	if _, _, err := s.call(env, ProposalsCreateCmd{Patch: fixture("proposals/ci.diff"), Pipeline: "extra",
		Kind: "topology"}); err == nil || !strings.Contains(err.Error(), "may not add .github/workflows/ci.yaml") {
		t.Fatalf("a path outside the kind's: %v", err)
	}
	call(ProposalsTokenCmd{}, "mspr_")
}

// validationSummary sums a report up in a line, whatever it holds.
func Test_validationSummary(t *testing.T) {
	for _, tt := range []struct{ report, want string }{
		{"null", "not validated"},
		{`{"passed":false,"error":"disk full"}`, "validation failed to run: disk full"},
		{`{"passed":false,"check":{"passed":false}}`, "validation failed: check failed"},
		{`{"passed":true,"check":{"passed":true},"evals":[{"passed":true}],"newEvals":[{"passed":true}],` +
			`"backtest":{"passed":true,"skipped":"no runner"}}`,
			"validation passed: check passed, evals 1/1 passed, 1 new eval, backtest passed (no runner)"},
	} {
		if got := validationSummary([]byte(tt.report)); got != tt.want {
			t.Errorf("%s: %q, want %q", tt.report, got, tt.want)
		}
	}
}
