package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// proposalSrc is a pipeline of builtins, which check passes without any action: line 4 holds the filter.
const proposalSrc = "pipeline: extra\n%striggers: { adhoc: { manual: {} } }\nsteps:\n" +
	"  pass: { action: filter, in: trigger.adhoc, config: { expr: \"%s\" } }\n"

// The engine's proposal rules on a store of their own: a proposal is created and validated ready, refused outside
// its kind's paths, accepted with its hash (the overlapping one goes stale), rebased when the base moves and kept
// stale when its patch no longer applies; the proposals-only token is minted, its hash stored.
func TestProposals(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	e, err := New(Options{Store: st, PackageDir: testdir.New(t)})
	if err != nil {
		t.Fatal(err)
	}
	apply := func(extra, expr string) {
		t.Helper()
		pkg, err := e.PackageFromFiles(map[string][]byte{"pipeline.yaml": fmt.Appendf(nil, proposalSrc, extra, expr)})
		if err == nil {
			_, _, err = e.ApplyPackages(ctx, []*pipeline.Package{pkg}, ApplyOptions{})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	patch := func(at int, to string) string {
		return fmt.Sprintf("--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -%d +%d @@\n"+
			"-  pass: { action: filter, in: trigger.adhoc, config: { expr: \"true\" } }\n"+
			"+  pass: { action: filter, in: trigger.adhoc, config: { expr: %q } }\n", at, at, to)
	}
	create := func(p string) store.Proposal {
		t.Helper()
		got, err := e.CreateProposal(ctx, ProposalRequest{Pipeline: "extra", Kind: store.ProposalConfig, Patch: p,
			By: "improve"})
		if err != nil || got.State != store.ProposalReady || got.Steps[0] != "pass" || len(got.Fingerprint) != 32 {
			t.Fatalf("CreateProposal = %+v, %v", got, err)
		}
		return got
	}
	apply("", "true")
	first, second := create(patch(4, "false")), create(patch(4, "item.n > 1"))
	if _, err := e.CreateProposal(ctx, ProposalRequest{Pipeline: "extra", Kind: store.ProposalLessons,
		Patch: patch(4, "false")}); !errors.Is(err, store.ErrInvalid) {
		t.Fatalf("a lessons proposal changing pipeline.yaml: %v", err)
	}
	if p, err := e.AcceptProposal(ctx, first.ID, first.PatchSHA256, "ana"); err != nil || p.State != store.ProposalAccepted {
		t.Fatalf("AcceptProposal = %+v, %v", p, err)
	}
	if _, err := e.ValidateProposal(ctx, second.ID); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("revalidating on the same base: %v", err)
	}
	apply("description: moved\n", "true")
	p, err := e.ValidateProposal(ctx, second.ID)
	if err != nil || p.State != store.ProposalReady || !strings.Contains(p.Patch, "@@ -5,1 +5,1 @@") {
		t.Fatalf("rebased = %+v, %v", p, err)
	}
	apply("description: moved\n", "false")
	if _, err := e.ValidateProposal(ctx, second.ID); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("an unrebasable proposal: %v", err)
	}
	for _, err := range []error{
		func() error {
			_, err := e.CreateProposal(ctx, ProposalRequest{Pipeline: "nope", Patch: "x"})
			return err
		}(),
		func() error { _, err := e.ValidateProposal(ctx, "nope"); return err }(),
		func() error { _, err := e.AcceptProposal(ctx, "nope", "x", "ana"); return err }(),
	} {
		if !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("an unknown pipeline or proposal: %v", err)
		}
	}
	if err := st.Unit(ctx, func(exec func(string, ...any) error) error {
		return exec(`UPDATE proposals SET patch = 'tampered' WHERE id = ?`, second.ID)
	}); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{
		func() error { _, err := e.ValidateProposal(ctx, second.ID); return err }(),
		func() error { _, err := e.AcceptProposal(ctx, second.ID, second.PatchSHA256, "ana"); return err }(),
	} {
		if !errors.Is(err, store.ErrConflict) || !strings.Contains(err.Error(), "no longer hashes") {
			t.Fatalf("a tampered patch: %v", err)
		}
	}
	token, err := e.MintProposalToken(ctx)
	if err != nil || !strings.HasPrefix(token, ProposalTokenPrefix) {
		t.Fatalf("MintProposalToken = %q, %v", token, err)
	}
	if h, err := st.ProposalTokenHash(ctx); err != nil || h != TriggerTokenHash(token) {
		t.Fatalf("stored hash %q, %v", h, err)
	}
}
