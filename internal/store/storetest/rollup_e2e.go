//go:build e2e

package storetest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Timing tests the e2e build adds: the default build covers the same paths through TestRollupMath.

// A dead-letter replay restarts the delivery's timings.
func TestReplayResetsTimings(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	l := e.leaseStep("crawl")
	e.clock.Advance(time.Second)
	e.fail(l, false)
	e.clock.Advance(time.Hour)
	if n, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, false); err != nil || n != 1 {
		t.Fatalf("replay = %d, %v", n, err)
	}
	d := e.deliveries("r1")[0]
	if d.CreatedAt != t0.Add(time.Hour+time.Second) || !d.FinishedAt.IsZero() || d.Wait != 0 || d.Work != 0 {
		t.Fatalf("replayed delivery timings: %+v", d)
	}
}

// Delivery timings: the queue wait counts from when an attempt is due (backoff excluded), the work from its lease,
// and the latency from creation to terminal; each attempt's end records its share in its event.
func TestDeliveryTimings(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	e.clock.Advance(2 * time.Second) // queue wait
	l := e.leaseStep("crawl")
	e.clock.Advance(3 * time.Second) // work
	wantState(t, "first attempt", e.fail(l, true), store.DeliveryPending)
	e.clock.Advance(time.Minute) // past the 5s backoff (10s with a jitter of one half), plus 55s of queue wait
	l = e.leaseStep("crawl")
	e.clock.Advance(4 * time.Second)
	e.done(l, out("a", `{"score":80}`))

	d := e.deliveries("r1")[0]
	wantState(t, "wait", d.Wait, 2*time.Second+55*time.Second)
	wantState(t, "work", d.Work, 7*time.Second)
	wantState(t, "created", d.CreatedAt, t0)
	wantState(t, "finished", d.FinishedAt, t0.Add(69*time.Second))
	wantState(t, "latency", d.Latency(time.Time{}), 69*time.Second)

	var data map[string]any
	done := e.eventKinds(store.EventDeliveryDone)[0]
	if err := json.Unmarshal(done.Data, &data); err != nil {
		t.Fatal(err)
	}
	wantState(t, "workMs", data["workMs"], any(float64(4000)))
	wantState(t, "latencyMs", data["latencyMs"], any(float64(69000)))
	retry := e.eventKinds(store.EventDeliveryRetry)[0]
	data = nil
	if err := json.Unmarshal(retry.Data, &data); err != nil {
		t.Fatal(err)
	}
	wantState(t, "retry error", data["error"], any("boom"))
	if _, terminal := data["latencyMs"]; terminal {
		t.Errorf("a retried attempt records a latency: %s", retry.Data)
	}
	// A delivery still pending has no finish, and its latency runs to now.
	pending := e.deliveries("r1")[1]
	if !pending.FinishedAt.IsZero() || pending.Latency(t0.Add(time.Hour)) != time.Hour-69*time.Second {
		t.Errorf("pending delivery: finished %v, latency %v", pending.FinishedAt, pending.Latency(t0.Add(time.Hour)))
	}
}
