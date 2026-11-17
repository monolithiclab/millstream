package cli

import (
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// Proposals from the command's own process, without the parser: created and validated, listed and shown, one
// accepted (the overlapping one goes stale and is not revalidated on the same base), rejected with its reason; a
// patch outside its kind's paths is refused; the proposals-only token is minted. Test_proposalsLifecycle (e2e) runs
// the same through the parser.
func Test_proposalsCommands(t *testing.T) {
	t.Parallel()
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
		Kind: "topology"}); err == nil || !strings.Contains(err.Error(), "may not create .github/workflows/ci.yaml") {
		t.Fatalf("a path outside the kind's: %v", err)
	}
	call(ProposalsTokenCmd{}, "mspr_")

	// The accepted proposal's patch (what patch-file publishes) applied to the package on disk, then applied:
	// recorded applied, by its hash alone.
	st := s.st
	if st == nil { // the e2e build's commands open --db
		var err error
		if st, err = sqlite.Open(s.db, sqlite.Options{}); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = st.Close() }()
	}
	prop, err := st.Proposal(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	base, err := pipeline.Load(fixture(lifecycleV1))
	if err != nil {
		t.Fatal(err)
	}
	p, err := patch.Parse([]byte(prop.Patch))
	if err != nil {
		t.Fatal(err)
	}
	files, err := p.Apply(base.Files)
	if err != nil {
		t.Fatal(err)
	}
	patched, err := pipeline.Materialize(t.TempDir(), files)
	if err != nil {
		t.Fatal(err)
	}
	s.must(0, "apply", patched.Dir)
	evs, err := st.PipelineEvents(t.Context(), store.EventFilter{Pipeline: "extra",
		Kinds: []string{store.EventProposalApplied}})
	if err != nil || len(evs) != 1 || !strings.Contains(string(evs[0].Data), `"id":"`+first+`"`) {
		t.Fatalf("proposalApplied events = %v, %v", evs, err)
	}
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

// printProposal tells how to build the action a distill proposal publishes, and says nothing of it for other kinds.
func Test_printProposalBuild(t *testing.T) {
	t.Parallel()
	patchText := "--- /dev/null\n+++ b/actions/score-rules/main.go\n@@ -0,0 +1 @@\n+x\n"
	var out strings.Builder
	if err := printProposal(&out, api.Proposal{ID: "pr_1", Kind: "distill", Patch: patchText}, true); err != nil {
		t.Fatal(err)
	}
	want := "build: cd actions/score-rules && go build -o bin/$(go env GOOS)-$(go env GOARCH)/score-rules .\n"
	if !strings.Contains(out.String(), want) {
		t.Errorf("distill proposal shows %q, want %q", out.String(), want)
	}
	out.Reset()
	if err := printProposal(&out, api.Proposal{ID: "pr_2", Kind: "config", Patch: patchText}, true); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "build:") {
		t.Errorf("config proposal shows a build line: %q", out.String())
	}
}
