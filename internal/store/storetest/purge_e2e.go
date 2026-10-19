//go:build e2e

package storetest

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// Retention and purge cases the e2e build adds: the default build covers the same units through
// TestRetainKeepsWhatUnfinishedWorkNeeds and TestPurgeAcrossImports.

// TestRetainKeepsSampledItems: an item with an open review sample keeps its payload until the sample is decided.
func TestRetainKeepsSampledItems(t *testing.T, open Opener) {
	e := newEnv(t, open, reviewFixture)
	e.fire("r1")
	e.done(e.leaseGate("crawl"), out("a", `{"price":1}`))
	l := e.leaseGate("gate")
	if _, err := e.s.Park(e.ctx, store.ParkRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Display: "a",
		Spec: store.ReviewSpec{Inbox: "analysts"}, AutoApprove: true, Sample: true}); err != nil {
		t.Fatal(err)
	}
	e.done(e.leaseGate("report"))
	e.clock.Advance(25 * time.Hour)
	if res := e.retain(store.RetentionRequest{}, "listings.raw"); res.Items != 0 {
		t.Fatalf("expired %d items with a sample open", res.Items)
	}
	if err := e.s.DecideSample(e.ctx, store.SampleDecisionRequest{Pipeline: "listings", DeliveryID: l.DeliveryID,
		Decision: store.SampleAgree, Reviewer: "ana"}); err != nil {
		t.Fatal(err)
	}
	if res := e.retain(store.RetentionRequest{}, "listings.raw"); res.Items != 1 {
		t.Fatalf("expired %d items once the sample was decided, want 1", res.Items)
	}
}

// TestPurgeErasesReviewsAndDeadLetters: fencing cancels a parked review and discards an open dead letter, settling
// their run; erasing drops the review's rendering and the failure's message, and the dedupe keys an item set.
func TestPurgeErasesReviewsAndDeadLetters(t *testing.T, open Opener) {
	e := newEnv(t, open, reviewFixture)
	parked := e.parkAll(func(string) store.ReviewSpec { return analysts }, "a")
	p, err := e.purge(store.PurgeRequest{ID: "p1", Key: "a"})
	if err != nil || p.Items != 1 || p.State != store.PurgeDone {
		t.Fatalf("purge = %+v, %v", p, err)
	}
	r := e.review(parked["a"])
	if r.State != store.ReviewCancelled || r.Display != "" || r.Item.Key != "" {
		t.Fatalf("review of an erased item = %+v", r)
	}
	if run := e.run("r1"); run.SettledAt.IsZero() {
		t.Fatalf("run not settled once its only parked review was cancelled: %+v", run)
	}

	// A dead letter: discarded at the fence, its error gone from it and from the failure's events.
	e.fire("r2")
	e.fail(e.leaseGate("crawl"), false)
	trigger := e.itemsIn("listings", "r2", "trigger.daily")[0].ID
	if _, err := e.purge(store.PurgeRequest{ID: "p2", Item: trigger, Pipeline: "listings"}); err != nil {
		t.Fatal(err)
	}
	if dls := e.deadLetters("r2"); len(dls) != 1 || dls[0].State != store.DeadLetterDiscarded || dls[0].Error != "" {
		t.Fatalf("dead letter of an erased item = %+v", dls)
	}
	for _, d := range e.deliveries("r2") {
		if d.ErrorMessage != "" {
			t.Fatalf("delivery %s of an erased item kept its error %q", d.ID, d.ErrorMessage)
		}
	}
	for _, ev := range e.events() {
		if strings.Contains(string(ev.Data), "boom") {
			t.Fatalf("event %s quotes the erased item's error: %s", ev.Kind, ev.Data)
		}
	}
}

// TestPurgeAcrossImports: a purge by key fences and erases the items with the key, their descendants and the copies
// imports made of them down a chain of imports, and nothing else. Fenced, pending deliveries are cancelled and a
// leased one's outputs are discarded when it reports; erased, an item has no payload, key or artifacts, and its
// deliveries' transcripts are gone. Its events never carry the key, and webhook trigger tokens are untouched.
func TestPurgeAcrossImports(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline)
	e.createToken("src", "hook")
	e.applyVars(dealPipeline, map[string]any{"tag": "y"})
	e.apply(relayPipeline)
	e.firePipeline("src", "go", "r1")
	e.done(e.leaseIn("src", "crawl"), withArtifacts(out("k1", `{"n":1}`), ref("photo", shaA)), out("k2", `{"n":2}`))
	listings := e.itemsIn("src", "r1", "listings")
	a, b := listings[0].ID, listings[1].ID
	score := e.leaseIn("src", "score")
	if _, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: score.Ref(), Attempt: 1,
		Outputs:   []store.Output{out("k1", `{"score":5}`)},
		Artifacts: []store.ArtifactRef{ref("transcript.json", shaB)}}); err != nil {
		t.Fatal(err)
	}
	good := e.itemsIn("src", "r1", "good")[0].ID
	dealRun, approved := ids.ImportRun("approved", "r1"), ids.ImportItem("approved", a)
	notify := e.leaseIn("deal", "notify") // a's copy, leased while the purge fences it
	wantState(t, "leased copy", notify.Item.ID, approved)
	lineage := map[string][]string{
		"src":   {a, good},
		"deal":  {approved, ids.ImportItem("scored", good)},
		"relay": {ids.ImportItem("again", approved)},
	}

	e.fail(e.leaseIn("deal", "file"), true) // its error quoted by a deliveryRetry event, which erasing scrubs
	p, created, err := e.s.StartPurge(e.ctx, store.PurgeRequest{ID: "p1", Key: "k1"})
	if err != nil || !created || p.Items != 5 || p.State != store.PurgeErasing || p.Scope != store.PurgeKey ||
		!slices.Equal(p.Pipelines, []string{"deal", "relay", "src"}) {
		t.Fatalf("StartPurge = %+v, %v, %v", p, created, err)
	}
	for name, its := range lineage {
		for _, id := range its {
			if it := e.item(name, id); it.Tombstone != store.TombstoneErased || string(it.Payload) != "null" {
				t.Fatalf("fenced %s %s = %+v", name, id, it)
			}
		}
	}
	if it := e.item("src", b); it.Tombstone != "" {
		t.Fatalf("k2 fenced: %+v", it)
	}
	// No lease on the lineage: its pending deliveries are cancelled; k2's are still there to lease.
	for _, l := range e.lease(100) {
		if slices.Contains(lineage[l.Pipeline], l.Item.ID) {
			t.Fatalf("leased %s on fenced %s", l.Step, l.Item.ID)
		}
		e.done(l)
	}
	for name, its := range lineage {
		for _, id := range its {
			ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: name, ItemID: id})
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range ds {
				if d.State == store.DeliveryPending {
					t.Fatalf("delivery %s of fenced %s still pending", d.ID, id)
				}
			}
		}
	}
	// The leased copy reports: cancelled, its outputs discarded.
	if st, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: notify.Ref(), Attempt: 1}); err != nil ||
		st != store.DeliveryCancelled {
		t.Fatalf("Done on a fenced item = %s, %v", st, err)
	}
	if again, created, err := e.s.StartPurge(e.ctx, store.PurgeRequest{ID: "p1", Key: "k1"}); err != nil || created ||
		again.Items != 5 {
		t.Fatalf("StartPurge again = %+v, %v, %v", again, created, err)
	}

	p, err = e.purge(store.PurgeRequest{ID: "p1", Key: "k1"})
	if err != nil || p.State != store.PurgeDone || p.Erased != 5 || p.FinishedAt.IsZero() {
		t.Fatalf("purge = %+v, %v", p, err)
	}
	for name, its := range lineage {
		for _, id := range its {
			if it := e.item(name, id); it.Key != "" || it.Tombstone != store.TombstoneErased || len(it.Artifacts) != 0 {
				t.Fatalf("erased %s %s = %+v", name, id, it)
			}
		}
	}
	if rc := e.refcounts("src"); rc[shaA] != 0 || rc[shaB] != 0 {
		t.Fatalf("src refcounts = %v, want the photo and the transcript unreferenced", rc)
	}
	if arts, err := e.s.AttemptArtifacts(e.ctx, "src", score.DeliveryID); err != nil || len(arts) != 0 {
		t.Fatalf("an erased item's transcripts = %+v, %v", arts, err)
	}
	if it := e.item("src", b); it.Key != "k2" || string(it.Payload) != `{"n":2}` {
		t.Fatalf("k2 = %+v", it)
	}
	if r := e.runIn("deal", dealRun); r.State == store.RunCancelled {
		t.Fatalf("the consumer run was cancelled: %+v", r)
	}
	started, completed := 0, 0
	for _, ev := range e.events() {
		if strings.Contains(string(ev.Data), "k1") || strings.Contains(string(ev.Data), "boom") {
			t.Fatalf("event %s carries the key: %s", ev.Kind, ev.Data)
		}
		switch ev.Kind {
		case store.EventPurgeStarted:
			started++
		case store.EventPurgeCompleted:
			completed++
		}
	}
	if started != 3 || completed != 3 {
		t.Fatalf("%d purgeStarted and %d purgeCompleted events, want one per pipeline", started, completed)
	}
	if got, err := e.s.Purge(e.ctx, "p1"); err != nil || got.State != store.PurgeDone {
		t.Fatalf("Purge = %+v, %v", got, err)
	}
	if open, err := e.s.Purges(e.ctx); err != nil || len(open) != 0 {
		t.Fatalf("unfinished purges = %+v, %v", open, err)
	}
	// Erased, the key matches nothing: a purge of it again is done at once.
	if p, err := e.purge(store.PurgeRequest{ID: "p2", Key: "k1"}); err != nil || p.Items != 0 || p.State != store.PurgeDone {
		t.Fatalf("purge again = %+v, %v", p, err)
	}
	// Scoped by pipeline and by item.
	if p, err := e.purge(store.PurgeRequest{ID: "p3", Item: ids.ImportItem("approved", b), Pipeline: "deal"}); err != nil ||
		p.Items != 2 || p.Scope != store.PurgeItem || !slices.Equal(p.Pipelines, []string{"deal", "relay"}) {
		t.Fatalf("purge of a copy = %+v, %v", p, err)
	}
	if it := e.item("src", b); it.Tombstone != "" {
		t.Fatalf("purging a copy erased its producer item: %+v", it)
	}
	e.wantToken("src", "hook")
	for _, req := range []store.PurgeRequest{{Key: "k"}, {ID: "x"}, {ID: "x", Key: "k", Item: "i"}} {
		if _, _, err := e.s.StartPurge(e.ctx, req); !errors.Is(err, store.ErrInvalid) {
			t.Fatalf("StartPurge %+v = %v", req, err)
		}
	}
	if _, err := e.s.Purge(e.ctx, "p_missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Purge of a missing purge = %v", err)
	}
}
