//go:build e2e

package storetest

import (
	"strings"
	"testing"
	"time"

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
