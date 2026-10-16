package storetest

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func TestClosedStore(t *testing.T, open Opener) {
	s := open(t, Options{})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := s.ExpireLeases(context.Background())
	wantErr(t, err, store.ErrClosed)
}

func TestCancelledContext(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.s.ExpireLeases(ctx)
	wantErr(t, err, context.Canceled)
}

func TestReads(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	e.clock.Advance(time.Second)
	e.fire("r2")

	runs, err := e.s.Runs(e.ctx, store.RunFilter{Pipeline: "listings"})
	if err != nil || len(runs) != 2 || runs[0].ID != "r2" {
		t.Fatalf("Runs = %v, %v; want r2 first", runs, err)
	}
	runs, err = e.s.Runs(e.ctx, store.RunFilter{State: store.RunRunning, Limit: 1})
	if err != nil || len(runs) != 1 {
		t.Fatalf("Runs limit 1 = %v, %v", runs, err)
	}
	r := e.run("r1")
	want := store.Run{
		Seq: 1, ID: "r1", Pipeline: "listings", Revision: 1, Trigger: "daily", TriggerKey: "r1", TriggerSource: store.SourceManual,
		Kind: store.RunKindTrigger,
		Vars: map[string]any{"minScore": int64(70)}, VarSources: map[string]string{"minScore": "default"},
		State: store.RunRunning, StartedAt: t0,
	}
	if !reflect.DeepEqual(r, want) {
		t.Errorf("Run = %+v\nwant  %+v", r, want)
	}
	_, err = e.s.Run(e.ctx, "listings", "nope")
	wantErr(t, err, store.ErrNotFound)

	items, err := e.s.Items(e.ctx, store.ItemFilter{RunID: "r1"})
	if err != nil || len(items) != 1 || items[0].Stream != "trigger.daily" || items[0].Step != "trigger.daily" {
		t.Fatalf("Items = %+v, %v", items, err)
	}
	if len(items[0].Parents) != 0 || string(items[0].Payload) != "{}" || !items[0].TS.Equal(t0) {
		t.Errorf("trigger item = %+v", items[0])
	}
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{State: store.DeliveryPending})
	if err != nil || len(ds) != 2 {
		t.Fatalf("Deliveries = %v, %v", ds, err)
	}
	evs, err := e.s.Events(e.ctx, 1, 2)
	if err != nil || len(evs) != 2 || evs[0].Seq != 2 {
		t.Fatalf("Events(1, 2) = %v, %v", evs, err)
	}
	if _, err := e.s.Events(e.ctx, 0, 0); err != nil {
		t.Fatalf("Events default limit: %v", err)
	}
	_, err = e.s.ActiveRevision(e.ctx, "nope")
	wantErr(t, err, store.ErrNotFound)
	_, err = e.s.Revision(e.ctx, 99)
	wantErr(t, err, store.ErrNotFound)
	_, err = e.s.PipelineState(e.ctx, "nope")
	wantErr(t, err, store.ErrNotFound)
}

// The reads of the HTTP API: paging cursors, lineage, a step's live state, the commit notifier.
func TestAPIReads(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	var notified []int64
	e.s.OnCommit(func(seq int64) { notified = append(notified, seq) })
	e.fire("r1")
	e.fire("r2") // started at the same instant as r1: Seq breaks the tie
	last, err := e.s.LastEventSeq(e.ctx)
	if err != nil || last == 0 || len(notified) != 2 || notified[1] != last {
		t.Fatalf("LastEventSeq = %d, %v; notified %v", last, err, notified)
	}
	e.s.OnCommit(nil)
	page, err := e.s.Runs(e.ctx, store.RunFilter{Pipeline: "listings", Limit: 1})
	if err != nil || len(page) != 1 || page[0].ID != "r2" {
		t.Fatalf("first page = %v, %v", page, err)
	}
	next, err := e.s.Runs(e.ctx, store.RunFilter{Pipeline: "listings", Limit: 1,
		After: store.RunPosition{StartedAt: page[0].StartedAt, Seq: page[0].Seq}})
	if err != nil || len(next) != 1 || next[0].ID != "r1" {
		t.Fatalf("second page = %v, %v", next, err)
	}

	l := e.leaseStep("crawl")
	e.done(l, out("a", `{"score": 80}`), out("b", `{"score": 10}`))
	children, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: "listings", Parents: []string{l.Item.ID}})
	if err != nil || len(children) != 4 { // two outputs, each on listings.raw and listings.audit
		t.Fatalf("children = %v, %v", children, err)
	}
	got, err := e.s.Items(e.ctx, store.ItemFilter{IDs: []string{children[0].ID, l.Item.ID}})
	if err != nil || len(got) != 2 || got[0].ID != l.Item.ID {
		t.Fatalf("Items by id = %v, %v", got, err)
	}
	after, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: "listings", After: children[0].Seq, Limit: 2})
	if err != nil || len(after) != 2 || after[0].ID != children[1].ID {
		t.Fatalf("Items after %d = %v, %v", children[0].Seq, after, err)
	}
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: "listings", ID: l.DeliveryID, ItemID: l.Item.ID,
		Step: "crawl"})
	if err != nil || len(ds) != 1 || ds[0].ID != l.DeliveryID {
		t.Fatalf("Deliveries of the item = %v, %v", ds, err)
	}
	evs, err := e.s.PipelineEvents(e.ctx, store.EventFilter{Pipeline: "listings", RunID: l.RunID,
		Kinds: []string{store.EventDeliveryDone}})
	if err != nil || len(evs) != 1 || !strings.Contains(string(evs[0].Data), `"itemId":"`+l.Item.ID+`"`) {
		t.Fatalf("done events of the run = %v, %v", evs, err)
	}

	counts, err := e.s.StreamCounts(e.ctx, "listings", "")
	if err != nil || counts["trigger.daily"] != 2 || counts["listings.raw"] != 2 {
		t.Fatalf("StreamCounts = %v, %v", counts, err)
	}
	if counts, err = e.s.StreamCounts(e.ctx, "listings", l.RunID); err != nil || counts["trigger.daily"] != 1 {
		t.Fatalf("StreamCounts of %s = %v, %v", l.RunID, counts, err)
	}
	stats, err := e.s.StepStats(e.ctx, "listings", t0)
	if err != nil {
		t.Fatal(err)
	}
	byStep := map[string]store.StepStats{}
	for _, s := range stats {
		byStep[s.Step] = s
	}
	if c := byStep["crawl"]; c.Pending != 1 || c.Finished != 1 || len(c.Latencies) != 1 {
		t.Errorf("crawl stats = %+v", c)
	}
	if s := byStep["score"]; s.Pending != 2 {
		t.Errorf("score stats = %+v", s)
	}

	revs, err := e.s.Revisions(e.ctx, "listings")
	if err != nil || len(revs) != 1 || revs[0].PackageHash != hashOf(fixture) {
		t.Fatalf("Revisions = %v, %v", revs, err)
	}
	bundle, err := e.s.Package(e.ctx, revs[0].PackageHash)
	if err != nil || string(bundle) != fixture {
		t.Fatalf("Package = %q, %v", bundle, err)
	}
	_, err = e.s.Package(e.ctx, "nope")
	wantErr(t, err, store.ErrNotFound)
}
