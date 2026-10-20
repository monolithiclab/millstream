package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/proposals"
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
	e, err := New(Options{Store: st, PackageDir: testdir.New(t), Proposals: ProposalOptions{Backtest: passing{}}})
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
	diff := func(at int, to string) string {
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
	first, second := create(diff(4, "false")), create(diff(4, "item.n > 1"))
	if _, err := e.CreateProposal(ctx, ProposalRequest{Pipeline: "extra", Kind: store.ProposalLessons,
		Patch: diff(4, "false")}); !errors.Is(err, store.ErrInvalid) {
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
	if err != nil || p.State != store.ProposalReady || !strings.HasPrefix(p.Patch, patch.BaseTrailer+": "+p.BaseRevision+"\n") ||
		!strings.Contains(p.Patch, "@@ -2,4 +2,4 @@") {
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
	// Production learns and never changes itself: no proposal is created, revalidated or given a token there.
	e.opts.Mode = pipeline.ModeProduction
	for name, err := range map[string]error{
		"create": func() error {
			_, err := e.CreateProposal(ctx, ProposalRequest{Pipeline: "extra", Kind: store.ProposalConfig,
				Patch: diff(4, "false")})
			return err
		}(),
		"revalidate": func() error { _, err := e.ValidateProposal(ctx, first.ID); return err }(),
		"token":      func() error { _, err := e.MintProposalToken(ctx); return err }(),
	} {
		if !errors.Is(err, pipeline.ErrProduction) || !errors.Is(err, store.ErrConflict) {
			t.Errorf("%s in production: %v", name, err)
		}
	}
	e.opts.Mode = ""
	token, err := e.MintProposalToken(ctx)
	if err != nil || !strings.HasPrefix(token, ProposalTokenPrefix) {
		t.Fatalf("MintProposalToken = %q, %v", token, err)
	}
	if h, err := st.ProposalTokenHash(ctx); err != nil || h != TriggerTokenHash(token) {
		t.Fatalf("stored hash %q, %v", h, err)
	}
}

// passing is a backtester whose backtests all pass (the real one, package backtest, drives an engine).
type passing struct{}

func (passing) Backtest(context.Context, proposals.BacktestRequest) (proposals.BacktestResult, error) {
	return proposals.BacktestResult{Passed: true, Report: json.RawMessage(`{}`)}, nil
}

// cancelling is a backtester that cancels the request which started the validation, then reports what its own
// context says.
type cancelling struct {
	cancel   context.CancelFunc
	err      error
	deadline time.Time
}

func (c *cancelling) Backtest(ctx context.Context, _ proposals.BacktestRequest) (proposals.BacktestResult, error) {
	c.cancel()
	c.err = ctx.Err()
	c.deadline, _ = ctx.Deadline()
	return proposals.BacktestResult{Passed: true, Report: json.RawMessage(`{}`)}, nil
}

// TestProposalValidationLifetime: a validation outlives the request that started it, within its timeout, and is
// recorded; a proposal left validating is taken over once older than the timeout, and an engine starting puts it
// back to drafting.
func TestProposalValidationLifetime(t *testing.T) {
	now := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	bt := &cancelling{}
	e, err := New(Options{Store: st, PackageDir: testdir.New(t), Now: func() time.Time { return now },
		Proposals: ProposalOptions{Backtest: bt, ValidationTimeout: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := e.PackageFromFiles(map[string][]byte{"pipeline.yaml": fmt.Appendf(nil, proposalSrc, "", "true")})
	if err == nil {
		_, _, err = e.ApplyPackages(context.Background(), []*pipeline.Package{pkg}, ApplyOptions{})
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	bt.cancel = cancel
	p, err := e.CreateProposal(ctx, ProposalRequest{Pipeline: "extra", Kind: store.ProposalConfig, By: "improve",
		Patch: "--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -4 +4 @@\n" +
			"-  pass: { action: filter, in: trigger.adhoc, config: { expr: \"true\" } }\n" +
			"+  pass: { action: filter, in: trigger.adhoc, config: { expr: \"false\" } }\n"})
	if err != nil || p.State != store.ProposalReady || bt.err != nil || time.Until(bt.deadline) < 59*time.Minute {
		t.Fatalf("validation of a cancelled request = %+v, %v; its context %v, deadline %s", p, err, bt.err, bt.deadline)
	}
	bt.cancel = func() {}
	// A validation that died midway leaves its proposal validating.
	if _, err := st.StartValidation(context.Background(), p.ID, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ValidateProposal(context.Background(), p.ID); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("revalidating while a validation may still run: %v", err)
	}
	now = now.Add(2 * time.Hour)
	if got, err := e.ValidateProposal(context.Background(), p.ID); err != nil || got.State != store.ProposalReady {
		t.Fatalf("taking over a lost validation = %+v, %v", got, err)
	}
	if _, err := st.StartValidation(context.Background(), p.ID, time.Time{}); err != nil {
		t.Fatal(err)
	}
	e.resetValidations(context.Background())
	if got, err := st.Proposal(context.Background(), p.ID); err != nil || got.State != store.ProposalDrafting {
		t.Fatalf("after an engine start: %+v, %v", got, err)
	}
}
