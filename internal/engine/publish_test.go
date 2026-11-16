package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// The apply publish action applies an accepted proposal on its base only, never in production, and records it
// applied; so does applying its patched package any other way, while a package that is not exactly its patch over
// its base records nothing (spec section 11.3).
func TestPublishApply(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	e, err := New(Options{Store: st, PackageDir: testdir.New(t), Proposals: ProposalOptions{Backtest: passing{}}})
	if err != nil {
		t.Fatal(err)
	}
	files := func(extra, expr string) map[string][]byte {
		return map[string][]byte{"pipeline.yaml": fmt.Appendf(nil, proposalSrc, extra, expr)}
	}
	apply := func(f map[string][]byte) {
		t.Helper()
		pkg, err := e.PackageFromFiles(f)
		if err == nil {
			_, _, err = e.ApplyPackages(ctx, []*pipeline.Package{pkg}, ApplyOptions{})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	applied := func(want int) {
		t.Helper()
		evs, err := st.PipelineEvents(ctx, store.EventFilter{Pipeline: "extra",
			Kinds: []string{store.EventProposalApplied}})
		if err != nil || len(evs) != want {
			t.Fatalf("proposalApplied events = %d, %v; want %d", len(evs), err, want)
		}
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
