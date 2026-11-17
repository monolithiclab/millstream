package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// publishEngine is a development engine on a store of its own, with apply, which applies a package's files, and
// applied, which checks how many proposalApplied events pipeline extra has.
func publishEngine(t *testing.T, opts ProposalOptions) (e *Engine, apply func(map[string][]byte), applied func(int)) {
	t.Helper()
	ctx := context.Background()
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if e, err = New(Options{Store: st, PackageDir: testdir.New(t), Proposals: opts}); err != nil {
		t.Fatal(err)
	}
	apply = func(f map[string][]byte) {
		t.Helper()
		pkg, err := e.PackageFromFiles(f)
		if err == nil {
			_, _, err = e.ApplyPackages(ctx, []*pipeline.Package{pkg}, ApplyOptions{})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	applied = func(want int) {
		t.Helper()
		evs, err := st.PipelineEvents(ctx, store.EventFilter{Pipeline: "extra",
			Kinds: []string{store.EventProposalApplied}})
		if err != nil || len(evs) != want {
			t.Fatalf("proposalApplied events = %d, %v; want %d", len(evs), err, want)
		}
	}
	return e, apply, applied
}

// The apply publish action applies an accepted proposal on its base only, never in production, and records it
// applied; so does applying its patched package any other way, while a package that is not exactly its patch over
// its base records nothing (spec section 11.3).
func TestPublishApply(t *testing.T) {
	ctx := context.Background()
	e, apply, applied := publishEngine(t, ProposalOptions{Backtest: passing{}})
	files := func(extra, expr string) map[string][]byte {
		return map[string][]byte{"pipeline.yaml": fmt.Appendf(nil, proposalSrc, extra, expr)}
	}
	apply(files("", "true"))
	diff := "--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -4 +4 @@\n" +
		"-  pass: { action: filter, in: trigger.adhoc, config: { expr: \"true\" } }\n" +
		"+  pass: { action: filter, in: trigger.adhoc, config: { expr: \"false\" } }\n"
	p, err := e.CreateProposal(ctx, ProposalRequest{Pipeline: "extra", Kind: store.ProposalConfig, Patch: diff})
	if err == nil {
		p, err = e.AcceptProposal(ctx, p.ID, p.PatchSHA256, "ana")
	}
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := patch.Parse([]byte(diff))
	if err != nil {
		t.Fatal(err)
	}
	patched, err := parsed.Apply(files("", "true"))
	if err != nil {
		t.Fatal(err)
	}

	e.opts.Mode = pipeline.ModeProduction
	if _, err := e.ApplyProposal(ctx, p.ID, "extra", p.BaseRevision, patched); !errors.Is(err, pipeline.ErrProduction) {
		t.Fatalf("apply in production: %v", err)
	}
	e.opts.Mode = ""
	if _, err := e.ApplyProposal(ctx, p.ID, "extra", strings.Repeat("0", 64), patched); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("apply on another base: %v", err)
	}
	apply(files("description: moved\n", "false")) // its change, but not its patch over its base
	applied(0)
	apply(files("", "true"))
	rev, err := e.ApplyProposal(ctx, p.ID, "extra", p.BaseRevision, patched)
	if err != nil || rev != 4 {
		t.Fatalf("ApplyProposal = %d, %v", rev, err)
	}
	applied(1)
}

// An accepted distill proposal's patch, applied with its new action built into bin/ (and the base's own action
// rebuilt), records the proposal applied: compiled actions are left out of the match; any other file is not
// (spec section 11.5).
func TestPublishApplyBuiltDistill(t *testing.T) {
	ctx := context.Background()
	e, apply, applied := publishEngine(t, ProposalOptions{})
	st := e.st
	base := map[string][]byte{
		"pipeline.yaml": []byte("pipeline: extra\ntriggers: { adhoc: { manual: {} } }\nsteps:\n" +
			"  score:\n    action: ./actions/scorer\n    in: trigger.adhoc\n"),
		"actions/scorer/action.yaml": []byte("name: scorer\nversion: 1.0.0\nkind: exec\n" +
			"runtime: { exec: { command: [scorer] } }\n"),
		"actions/scorer/bin/linux-amd64/scorer": []byte("scorer v1"),
	}
	apply(base)
	diff := "--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -5,1 +5,2 @@\n-    action: ./actions/scorer\n" +
		"+    action: ./actions/coded\n+    fallback: { action: ./actions/scorer, when: [abstain] }\n" +
		"--- /dev/null\n+++ b/actions/coded/action.yaml\n@@ -0,0 +1,4 @@\n+name: coded\n+version: 1.0.0\n" +
		"+kind: exec\n+runtime: { exec: { command: [coded] } }\n"
	parsed, err := patch.Parse([]byte(diff))
	if err != nil {
		t.Fatal(err)
	}
	patched, err := parsed.Apply(base)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := e.activeRevision(ctx, "extra")
	if err != nil {
		t.Fatal(err)
	}
	p, err := st.CreateProposal(ctx, store.NewProposal{ID: "pr_distill", Pipeline: "extra",
		Kind: store.ProposalDistill, BaseRevision: rev.PackageHash, Patch: diff, PatchSHA256: patch.Hash([]byte(diff)),
		Paths: []string{"actions/coded/action.yaml", "pipeline.yaml"}, Steps: []string{"score"},
		Fingerprint: "distill-score"})
	if err == nil {
		_, err = st.StartValidation(ctx, p.ID, time.Time{})
	}
	if err == nil {
		_, err = st.RecordValidation(ctx, store.Validation{ID: p.ID, Report: json.RawMessage(`{}`), Passed: true})
	}
	if err == nil {
		_, err = st.AcceptProposal(ctx, p.ID, p.PatchSHA256, "ana")
	}
	if err != nil {
		t.Fatal(err)
	}
	built := maps.Clone(patched)
	built["actions/coded/bin/linux-amd64/coded"] = []byte("coded")
	built["actions/scorer/bin/linux-amd64/scorer"] = []byte("scorer v2")

	edited := maps.Clone(built)
	edited["actions/coded/README.md"] = []byte("not in the patch")
	apply(edited)
	applied(0)
	apply(base)
	apply(built)
	applied(1)
}
