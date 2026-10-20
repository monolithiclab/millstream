package storetest

import (
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/store"
)

// proposal is a proposal of listings on base touching paths and steps, its fingerprint its id unless fp is set.
func proposal(id, base string, paths, steps []string) store.NewProposal {
	return store.NewProposal{ID: id, Pipeline: "listings", Kind: store.ProposalConfig, BaseRevision: base,
		Patch: "patch " + id, PatchSHA256: "sha-" + id, Paths: paths, Steps: steps, Fingerprint: "fp-" + id,
		CreatedBy: "improve"}
}

func (e *env) createProposal(np store.NewProposal) store.Proposal {
	e.t.Helper()
	p, err := e.s.CreateProposal(e.ctx, np)
	if err != nil {
		e.t.Fatalf("CreateProposal %s: %v", np.ID, err)
	}
	return p
}

// ready validates a proposal as passed.
func (e *env) ready(id string) {
	e.t.Helper()
	if _, err := e.s.StartValidation(e.ctx, id, time.Time{}); err != nil {
		e.t.Fatalf("StartValidation %s: %v", id, err)
	}
	p, err := e.s.RecordValidation(e.ctx, store.Validation{ID: id, Report: []byte(`{"passed":true}`), Passed: true})
	if err != nil || p.State != store.ProposalReady {
		e.t.Fatalf("RecordValidation %s = %s, %v", id, p.State, err)
	}
}

func (e *env) proposalStates() map[string]store.ProposalState {
	e.t.Helper()
	ps, err := e.s.Proposals(e.ctx, store.ProposalFilter{Pipeline: "listings"})
	if err != nil {
		e.t.Fatal(err)
	}
	out := map[string]store.ProposalState{}
	for _, p := range ps {
		out[p.ID] = p.State
	}
	return out
}

// TestProposalLifecycle: a proposal is created drafting on the active revision only, validated to ready (or back to
// drafting when it fails), and accepted with its validated hash; the acceptance makes every other open proposal
// touching one of its paths or steps stale in the same unit, and leaves the others (spec sections 5.11 and 11.3).
// A rejection needs a reason, kept for later drafts.
func TestProposalLifecycle(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	base := hashOf(fixture)
	p := e.createProposal(proposal("p1", base, []string{"pipeline.yaml"}, []string{"score"}))
	if p.State != store.ProposalDrafting || string(p.Report) != "null" || !p.CreatedAt.Equal(t0) ||
		!slices.Equal(p.Paths, []string{"pipeline.yaml"}) {
		t.Fatalf("created = %+v", p)
	}
	wantErr(t, func() error {
		_, err := e.s.CreateProposal(e.ctx, proposal("p1", base, []string{"x"}, nil))
		return err
	}(),
		store.ErrConflict)
	wantErr(t, func() error {
		_, err := e.s.CreateProposal(e.ctx, proposal("old", hashOf("other"), []string{"x"}, nil))
		return err
	}(), store.ErrConflict)
	wantErr(t, func() error { _, err := e.s.CreateProposal(e.ctx, store.NewProposal{ID: "x"}); return err }(),
		store.ErrInvalid)
	e.createProposal(proposal("samePath", base, []string{"pipeline.yaml"}, nil))
	e.createProposal(proposal("sameStep", base, []string{"lessons/score.yaml"}, []string{"score"}))
	e.createProposal(proposal("apart", base, []string{"lessons/crawl.yaml"}, []string{"crawl"}))
	e.createProposal(proposal("failing", base, []string{"actions/a/prompt.md"}, []string{"audit"}))

	wantErr(t, func() error { _, err := e.s.AcceptProposal(e.ctx, "p1", "sha-p1", "ana"); return err }(),
		store.ErrConflict) // not ready yet
	if _, err := e.s.StartValidation(e.ctx, "failing", time.Time{}); err != nil {
		t.Fatal(err)
	}
	wantErr(t, func() error { _, err := e.s.StartValidation(e.ctx, "failing", time.Time{}); return err }(), store.ErrConflict)
	// A validation started no earlier than the takeover time is still running; an older one is presumed lost.
	wantErr(t, func() error { _, err := e.s.StartValidation(e.ctx, "failing", e.clock.Now()); return err }(),
		store.ErrConflict)
	e.clock.Advance(time.Hour)
	if p, err := e.s.StartValidation(e.ctx, "failing", e.clock.Now().Add(-30*time.Minute)); err != nil ||
		p.State != store.ProposalValidating || !p.UpdatedAt.Equal(e.clock.Now()) {
		t.Fatalf("taking over a lost validation = %+v, %v", p, err)
	}
	// An engine starting resets what was validating: no validation outlives its engine.
	if ids, err := e.s.ResetValidations(e.ctx); err != nil || !slices.Equal(ids, []string{"failing"}) ||
		e.proposalStates()["failing"] != store.ProposalDrafting {
		t.Fatalf("ResetValidations = %v, %v; states %v", ids, err, e.proposalStates())
	}
	if ids, err := e.s.ResetValidations(e.ctx); err != nil || len(ids) != 0 {
		t.Fatalf("ResetValidations again = %v, %v", ids, err)
	}
	if _, err := e.s.StartValidation(e.ctx, "failing", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if f, err := e.s.RecordValidation(e.ctx, store.Validation{ID: "failing", Report: []byte(`{"passed":false}`)}); err != nil ||
		f.State != store.ProposalDrafting || string(f.Report) != `{"passed":false}` {
		t.Fatalf("failed validation = %+v, %v", f, err)
	}
	e.ready("p1")
	e.ready("sameStep")
	wantErr(t, func() error { _, err := e.s.AcceptProposal(e.ctx, "p1", "sha-other", "ana"); return err }(),
		store.ErrConflict)
	got, err := e.s.AcceptProposal(e.ctx, "p1", "sha-p1", "ana")
	if err != nil || got.State != store.ProposalAccepted || got.DecidedBy != "ana" || got.DecidedAt.IsZero() {
		t.Fatalf("AcceptProposal = %+v, %v", got, err)
	}
	want := map[string]store.ProposalState{"p1": store.ProposalAccepted, "samePath": store.ProposalStale,
		"sameStep": store.ProposalStale, "apart": store.ProposalDrafting, "failing": store.ProposalDrafting}
	if states := e.proposalStates(); !maps.Equal(states, want) {
		t.Fatalf("after the acceptance: %v, want %v", states, want)
	}
	if n := len(e.eventKinds(store.EventProposalStale)); n != 2 {
		t.Fatalf("%d proposalStale events, want 2", n)
	}

	wantErr(t, func() error { _, err := e.s.RejectProposal(e.ctx, "apart", "", "ana"); return err }(), store.ErrInvalid)
	r, err := e.s.RejectProposal(e.ctx, "apart", "not worth it", "ana")
	if err != nil || r.State != store.ProposalRejected || r.Reason != "not worth it" {
		t.Fatalf("RejectProposal = %+v, %v", r, err)
	}
	wantErr(t, func() error { _, err := e.s.RejectProposal(e.ctx, "p1", "late", "ana"); return err }(),
		store.ErrConflict)
	if _, err := e.s.RejectProposal(e.ctx, "samePath", "superseded", "ana"); err != nil {
		t.Fatalf("rejecting a stale proposal: %v", err)
	}
	if rejected, err := e.s.Proposals(e.ctx, store.ProposalFilter{State: store.ProposalRejected}); err != nil ||
		len(rejected) != 2 {
		t.Fatalf("rejected proposals = %+v, %v", rejected, err)
	}
	wantErr(t, func() error { _, err := e.s.Proposal(e.ctx, "nope"); return err }(), store.ErrNotFound)
	missing := proposal("m", base, []string{"x"}, nil)
	missing.Pipeline = "nope"
	for _, err := range []error{
		func() error { _, err := e.s.CreateProposal(e.ctx, missing); return err }(),
		func() error { _, err := e.s.StartValidation(e.ctx, "nope", time.Time{}); return err }(),
		func() error {
			_, err := e.s.RecordValidation(e.ctx, store.Validation{ID: "nope", Report: []byte(`{}`)})
			return err
		}(),
		func() error { _, err := e.s.AcceptProposal(e.ctx, "nope", "x", "ana"); return err }(),
		func() error { _, err := e.s.RejectProposal(e.ctx, "nope", "x", "ana"); return err }(),
	} {
		wantErr(t, err, store.ErrNotFound)
	}
	wantErr(t, func() error {
		_, err := e.s.RecordValidation(e.ctx, store.Validation{ID: "p1", Report: []byte(`{`)})
		return err
	}(),
		store.ErrInvalid)
	wantErr(t, func() error {
		_, err := e.s.RecordValidation(e.ctx, store.Validation{ID: "p1", Report: []byte(`{}`)})
		return err
	}(),
		store.ErrConflict) // accepted, not validating
}

// TestProposalMovedBase: a revision applied, or rolled back to, makes the open proposals on another base stale; an
// acceptance finding the base moved makes its proposal stale, committed, and answers ErrConflict; a validation
// recorded after the base moved leaves it stale, and one rebased onto the active revision makes it ready again.
func TestProposalMovedBase(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	base := hashOf(fixture)
	e.createProposal(proposal("p1", base, []string{"pipeline.yaml"}, nil))
	e.createProposal(proposal("p2", base, []string{"lessons/score.yaml"}, []string{"score"}))
	e.ready("p1")
	v2 := fixture + "\n# v2\n"
	e.apply(v2)
	if states := e.proposalStates(); states["p1"] != store.ProposalStale || states["p2"] != store.ProposalStale {
		t.Fatalf("after an apply: %v", states)
	}
	// Behind the units: p1 ready on the old base, as if the apply had not seen it.
	if err := e.s.Exec(e.ctx, `UPDATE proposals SET state = 'ready' WHERE id = 'p1'`); err != nil {
		t.Fatal(err)
	}
	got, err := e.s.AcceptProposal(e.ctx, "p1", "sha-p1", "ana")
	if !errors.Is(err, store.ErrConflict) || got.State != store.ProposalStale {
		t.Fatalf("AcceptProposal on a moved base = %+v, %v; want it stale and a conflict", got, err)
	}
	if _, err := e.s.StartValidation(e.ctx, "p2", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if p, err := e.s.RecordValidation(e.ctx, store.Validation{ID: "p2", Report: []byte(`{}`), Passed: true}); err != nil ||
		p.State != store.ProposalStale {
		t.Fatalf("validation on the old base = %+v, %v; want stale", p, err)
	}
	if _, err := e.s.StartValidation(e.ctx, "p2", time.Time{}); err != nil {
		t.Fatal(err)
	}
	p, err := e.s.RecordValidation(e.ctx, store.Validation{ID: "p2", Report: []byte(`{}`), Passed: true,
		Rebase: &store.Rebase{BaseRevision: hashOf(v2), Patch: "rebased", PatchSHA256: "sha-rebased",
			Paths: []string{"lessons/score.yaml"}, Steps: []string{"score"}}})
	if err != nil || p.State != store.ProposalReady || p.BaseRevision != hashOf(v2) || p.PatchSHA256 != "sha-rebased" {
		t.Fatalf("rebased validation = %+v, %v", p, err)
	}
	if _, err := e.s.Rollback(e.ctx, "listings", 0); err != nil {
		t.Fatal(err)
	}
	if states := e.proposalStates(); states["p2"] != store.ProposalStale {
		t.Fatalf("after a rollback: %v", states)
	}
}

// TestProposalSuppression: a rejection suppresses its fingerprint until an insight it answers weighs twice what it
// did at the rejection (info 1, warning 2, error 4); a proposal answering no insight is never suppressed.
func TestProposalSuppression(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	base := hashOf(fixture)
	e.record([]string{"cost"}, finding("i1", "cost", "warning"))
	np := func(id string) store.NewProposal {
		p := proposal(id, base, []string{"pipeline.yaml"}, nil)
		p.Fingerprint, p.Insights = "same idea", []string{"i1"}
		return p
	}
	e.createProposal(np("a"))
	if r, err := e.s.RejectProposal(e.ctx, "a", "the threshold is right", "ana"); err != nil ||
		r.RejectedSeverity != "warning" {
		t.Fatalf("RejectProposal = %+v, %v", r, err)
	}
	wantErr(t, func() error { _, err := e.s.CreateProposal(e.ctx, np("b")); return err }(), store.ErrConflict)
	free := np("c")
	free.Insights = nil
	e.createProposal(free)
	e.record([]string{"cost"}, finding("i1", "cost", "error"))
	e.createProposal(np("b"))
}

// TestProposalUnitsFailpoints: a crash before the acceptance commits leaves the proposal ready and its overlap open;
// after it, both are as the unit left them. The proposals-only token's hash is kept and replaced.
func TestProposalUnitsFailpoints(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	base := hashOf(fixture)
	e.createProposal(proposal("p1", base, []string{"pipeline.yaml"}, nil))
	e.createProposal(proposal("p2", base, []string{"pipeline.yaml"}, nil))
	e.ready("p1")
	for _, phase := range []string{"beforeCommit", "afterCommit"} {
		reset := failpoint.Set("acceptProposal." + phase)
		_, err := e.s.AcceptProposal(e.ctx, "p1", "sha-p1", "ana")
		reset()
		if !errors.As(err, new(failpoint.Crash)) {
			t.Fatalf("AcceptProposal with %s armed = %v", phase, err)
		}
		want := map[string]store.ProposalState{"p1": store.ProposalReady, "p2": store.ProposalDrafting}
		if phase == "afterCommit" {
			want = map[string]store.ProposalState{"p1": store.ProposalAccepted, "p2": store.ProposalStale}
		}
		if states := e.proposalStates(); !maps.Equal(states, want) {
			t.Fatalf("after a crash %s: %v, want %v", phase, states, want)
		}
	}
	wantErr(t, func() error { _, err := e.s.ProposalTokenHash(e.ctx); return err }(), store.ErrNotFound)
	for _, h := range []string{"h1", "h2"} {
		if err := e.s.SetProposalToken(e.ctx, h); err != nil {
			t.Fatal(err)
		}
		if got, err := e.s.ProposalTokenHash(e.ctx); err != nil || got != h {
			t.Fatalf("ProposalTokenHash = %q, %v; want %s", got, err, h)
		}
	}
	wantErr(t, e.s.SetProposalToken(e.ctx, ""), store.ErrInvalid)
}

// TestProposalPublishing: an apply under a base applies only while the active revision is still that base, and
// records proposalApplied for the accepted proposals the revision carries, validated or not, leaving out the rest;
// only an accepted proposal is recorded published (spec sections 11.3 and 11.7).
func TestProposalPublishing(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	base := hashOf(fixture)
	e.createProposal(proposal("p1", base, []string{"pipeline.yaml"}, nil))
	e.createProposal(proposal("open", base, []string{"lessons/score.yaml"}, []string{"score"}))
	wantErr(t, e.s.RecordPublished(e.ctx, "p1", "patch-file", "x.patch"), store.ErrConflict)
	e.ready("p1")
	if _, err := e.s.AcceptProposal(e.ctx, "p1", "sha-p1", "ana"); err != nil {
		t.Fatal(err)
	}
	if err := e.s.RecordPublished(e.ctx, "p1", "patch-file", "x.patch"); err != nil {
		t.Fatal(err)
	}
	v2, v3 := fixture+"\n# v2\n", fixture+"\n# v3\n"
	record := func(src, cond string, applied ...store.AppliedProposal) error {
		_, err := e.s.ApplyRevision(e.ctx, store.PackageRecord{Hash: hashOf(src), Bundle: []byte(src),
			Pipeline: parse(e.t, src), Base: cond, Proposals: applied})
		return err
	}
	wantErr(t, record(v2, hashOf("elsewhere"), store.AppliedProposal{ID: "p1", Validated: true}), store.ErrConflict)
	if err := record(v2, base, store.AppliedProposal{ID: "p1", Validated: true},
		store.AppliedProposal{ID: "open"}, store.AppliedProposal{ID: "absent"}); err != nil {
		t.Fatal(err)
	}
	if err := record(v3, "", store.AppliedProposal{ID: "p1"}); err != nil {
		t.Fatal(err)
	}
	evs, err := e.s.PipelineEvents(e.ctx, store.EventFilter{Pipeline: "listings",
		Kinds: []string{store.EventProposalPublished, store.EventProposalApplied}})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ev := range evs {
		got = append(got, ev.Kind+" "+string(ev.Data))
	}
	want := []string{
		`proposalPublished {"id":"p1","publisher":"patch-file","where":"x.patch"}`,
		`proposalApplied {"id":"p1","packageHash":"` + hashOf(v2) + `","parent":"` + base + `","revision":2,"validated":true}`,
		`proposalApplied {"id":"p1","packageHash":"` + hashOf(v3) + `","parent":"` + hashOf(v2) + `","revision":3,"validated":false}`,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("events\n%q\nwant\n%q", got, want)
	}
}
