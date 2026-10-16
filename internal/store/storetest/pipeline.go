package storetest

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

func TestApplyRevision(t *testing.T, open Opener) {
	e := newEnv(t, open)
	first := e.apply(fixture)
	if again := e.apply(fixture); again != first {
		t.Fatalf("re-applying the active package gave revision %d, want %d", again, first)
	}
	second := e.apply(fixtureFixed)
	if second == first {
		t.Fatal("a new package kept the revision")
	}
	if active, _ := e.s.ActiveRevision(e.ctx, "listings"); active != second {
		t.Fatalf("active = %d, want %d", active, second)
	}
	// Rolling back to the first package is a new revision of the same package.
	if back := e.apply(fixture); back == first || back == second {
		t.Fatalf("rollback revision %d", back)
	}
	rev, err := e.s.Revision(e.ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	p, err := rev.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if want := parse(t, fixture); !reflect.DeepEqual(p, want) {
		t.Errorf("decoded pipeline differs:\n got %+v\nwant %+v", p, want)
	}
	if rev.Pipeline != "listings" || rev.PackageHash != hashOf(fixture) || !rev.AppliedAt.Equal(t0) {
		t.Errorf("revision = %+v", rev)
	}
	if n := len(e.eventKinds(store.EventRevisionApplied)); n != 3 {
		t.Errorf("%d revisionApplied events, want 3", n)
	}

	_, err = e.s.ApplyRevision(e.ctx, store.PackageRecord{Hash: "h"})
	wantErr(t, err, store.ErrInvalid)
}

func TestApplyRevisionPausedSteps(t *testing.T, open Opener) {
	src := strings.Replace(fixture, "  audit:\n    action: ./actions/audit\n", "  audit:\n    action: ./actions/audit\n    paused: true\n", 1)
	e := newEnv(t, open, src)
	ps, err := e.s.PipelineState(e.ctx, "listings")
	if err != nil {
		t.Fatal(err)
	}
	if ps.Paused || !reflect.DeepEqual(ps.PausedSteps, []string{"audit"}) || ps.ActiveRevision != 1 {
		t.Fatalf("PipelineState = %+v", ps)
	}
	// A resume survives re-applying: the pause state was already set.
	if err := e.s.SetPaused(e.ctx, "listings", "audit", false); err != nil {
		t.Fatal(err)
	}
	e.apply(src + "\n")
	ps, _ = e.s.PipelineState(e.ctx, "listings")
	if len(ps.PausedSteps) != 0 {
		t.Fatalf("paused steps after re-apply = %v", ps.PausedSteps)
	}
}

func TestFire(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	created, err := e.s.Fire(e.ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: "r1", RunID: "r1"})
	if err != nil || created {
		t.Fatalf("second Fire = %v, %v; want a no-op", created, err)
	}
	if n := len(e.eventKinds(store.EventRunStarted)); n != 1 {
		t.Fatalf("%d runStarted events, want 1", n)
	}
	items := e.items("trigger.daily")
	if len(items) != 1 || items[0].ID != ids.TriggerItem("daily", "r1") || items[0].Key != "r1" {
		t.Fatalf("trigger items = %+v", items)
	}
	ds := e.deliveries("r1")
	if len(ds) != 1 || ds[0].Step != "crawl" || ds[0].ID != ids.Delivery("crawl", items[0].ID, 0) ||
		ds[0].State != store.DeliveryPending || ds[0].Attempt != 1 || !ds[0].NextAttemptAt.Equal(t0) {
		t.Fatalf("deliveries = %+v", ds)
	}

	tests := []struct {
		name string
		f    store.Firing
		want error
	}{
		{"missing fields", store.Firing{Pipeline: "listings"}, store.ErrInvalid},
		{"unknown pipeline", store.Firing{Pipeline: "nope", Trigger: "daily", Key: "k", RunID: "x"}, store.ErrNotFound},
		{"unknown trigger", store.Firing{Pipeline: "listings", Trigger: "nope", Key: "k", RunID: "x"}, store.ErrInvalid},
		{"unknown revision", store.Firing{Pipeline: "listings", Trigger: "daily", Key: "k", RunID: "x", Revision: 9}, store.ErrNotFound},
		{"bad payload", store.Firing{Pipeline: "listings", Trigger: "daily", Key: "k", RunID: "x", Payload: []byte("{")}, store.ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := e.s.Fire(e.ctx, tt.f)
			wantErr(t, err, tt.want)
		})
	}
}

func TestFireRevisionOfAnotherPipeline(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture, strings.Replace(fixture, "pipeline: listings", "pipeline: other", 1))
	_, err := e.s.Fire(e.ctx, store.Firing{Pipeline: "other", Trigger: "daily", Key: "k", RunID: "x", Revision: 1})
	wantErr(t, err, store.ErrInvalid)
}

func TestSetPaused(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	if err := e.s.SetPaused(e.ctx, "listings", "crawl", true); err != nil {
		t.Fatal(err)
	}
	if ls := e.lease(10); len(ls) != 0 {
		t.Fatalf("leased %d deliveries of a paused step", len(ls))
	}
	if err := e.s.SetPaused(e.ctx, "listings", "crawl", false); err != nil {
		t.Fatal(err)
	}
	if err := e.s.SetPaused(e.ctx, "listings", "", true); err != nil {
		t.Fatal(err)
	}
	if ps, _ := e.s.PipelineState(e.ctx, "listings"); !ps.Paused {
		t.Fatal("pipeline not paused")
	}
	if ls := e.lease(10); len(ls) != 0 {
		t.Fatalf("leased %d deliveries of a paused pipeline", len(ls))
	}
	if err := e.s.SetPaused(e.ctx, "listings", "", false); err != nil {
		t.Fatal(err)
	}
	if ls := e.lease(10); len(ls) != 1 {
		t.Fatalf("leased %d after resume, want 1", len(ls))
	}
	wantState(t, "paused events", len(e.eventKinds(store.EventPaused)), 2)
	wantState(t, "resumed events", len(e.eventKinds(store.EventResumed)), 2)
	wantErr(t, e.s.SetPaused(e.ctx, "nope", "", true), store.ErrNotFound)
}

// A revision whose unit crashed before its commit gives its number back: applied again, it is numbered as it would
// have been, on every store, so a revision's number never depends on a crash (spec section 9.1).
func TestRevisionNumbersSurviveACrash(t *testing.T, open Opener) {
	e := newEnv(t, open)
	reset := failpoint.Set("applyRevision.beforeCommit")
	_, err := e.s.ApplyRevision(e.ctx, store.PackageRecord{Hash: hashOf(fixture), Pipeline: parse(t, fixture)})
	reset()
	if !errors.As(err, new(failpoint.Crash)) {
		t.Fatalf("ApplyRevision = %v, want the failpoint crash", err)
	}
	wantState(t, "revision applied again", e.apply(fixture), store.RevisionID(1))
	wantState(t, "the next revision", e.apply(fixtureFixed), store.RevisionID(2))
}
