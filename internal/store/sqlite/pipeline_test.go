package sqlite

import (
	"reflect"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

func TestApplyRevision(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
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

func TestApplyRevisionPausedSteps(t *testing.T) {
	t.Parallel()
	src := strings.Replace(fixture, "  audit:\n    action: ./actions/audit\n", "  audit:\n    action: ./actions/audit\n    paused: true\n", 1)
	e := newEnv(t, src)
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

func TestFire(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
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
			t.Parallel()
			_, err := e.s.Fire(e.ctx, tt.f)
			wantErr(t, err, tt.want)
		})
	}
}

func TestFireRevisionOfAnotherPipeline(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture, strings.Replace(fixture, "pipeline: listings", "pipeline: other", 1))
	_, err := e.s.Fire(e.ctx, store.Firing{Pipeline: "other", Trigger: "daily", Key: "k", RunID: "x", Revision: 1})
	wantErr(t, err, store.ErrInvalid)
}

func TestFireWithoutConsumersSettles(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	e.firePipeline("listings", "adhoc", "m1")
	r := e.run("m1")
	if r.State != store.RunSucceeded || !r.SettledAt.Equal(t0) {
		t.Fatalf("run = %+v, want succeeded at once", r)
	}
}

// Two pipelines with a trigger of the same name fire runs and trigger items with the same ids; the pipeline id
// keeps them apart (spec section 4.10).
func TestFireIsolatesPipelines(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture, strings.Replace(fixture, "pipeline: listings", "pipeline: other", 1))
	e.firePipeline("listings", "daily", "run_x_daily")
	e.firePipeline("other", "daily", "run_x_daily")
	ls := e.lease(10)
	if len(ls) != 2 || ls[0].Pipeline == ls[1].Pipeline || ls[0].DeliveryID != ls[1].DeliveryID {
		t.Fatalf("leases = %+v; want the same delivery id in both pipelines", ls)
	}
	for _, l := range ls {
		e.done(l, out("a", `{"score":1}`))
	}
	other, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: "other", Stream: "listings.raw"})
	if err != nil || len(other) != 1 || len(e.items("listings.raw")) != 1 {
		t.Fatalf("items: other %v (%v), listings %v", other, err, e.items("listings.raw"))
	}
}

func TestSetPaused(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
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

// Revisions are decoded once and cached by package hash: two revisions of one package share the entry.
func TestRevisionCache(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	e.apply(fixtureFixed)
	e.apply(fixture) // revision 3: the package of revision 1
	var first, again, third, fixed *revision
	err := e.s.submit(e.ctx, "test", func(w *wtx) error {
		var err error
		for _, step := range []struct {
			id  store.RevisionID
			dst **revision
		}{{1, &first}, {1, &again}, {3, &third}, {2, &fixed}} {
			if *step.dst, err = w.revision(step.id); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if first != again || first != third || first == fixed {
		t.Fatalf("cache entries: 1=%p 1=%p 3=%p 2=%p; want 1 and 3 shared, 2 apart", first, again, third, fixed)
	}
}
