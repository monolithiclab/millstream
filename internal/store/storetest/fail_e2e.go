//go:build e2e

package storetest

import (
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Fail variants the e2e build adds: the default build covers the same paths through the other Fail tests.

func TestFailNonRetryableIsDeadAtOnce(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	wantState(t, "state", e.fail(e.lease(1)[0], false), store.DeliveryDead)
	if d := e.deliveries("r1")[0]; d.Attempt != 1 {
		t.Fatalf("attempt %d", d.Attempt)
	}
}

func TestFailTruncatesMessage(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	l := e.lease(1)[0]
	long := strings.Repeat("é", store.MaxErrorMessage) // two bytes each
	if _, err := e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: 1, Message: long, CostUSD: 1.5}); err != nil {
		t.Fatal(err)
	}
	d := e.deliveries("r1")[0]
	if len(d.ErrorMessage) != store.MaxErrorMessage || d.ErrorMessage != long[:store.MaxErrorMessage] || d.CostUSD != 1.5 {
		t.Fatalf("message %d bytes, cost %v", len(d.ErrorMessage), d.CostUSD)
	}
	// Without a class, the dead letter carries the bare message.
	if dls := e.deadLetters("r1"); dls[0].Error != d.ErrorMessage {
		t.Fatal("dead-letter error differs from the delivery's")
	}
}

// crawl retries with backoff 10s capped at 30s and three attempts; the jitter is one half.
func TestFailRetrySchedule(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	waits := []time.Duration{5 * time.Second, 10 * time.Second} // half of 10s, then half of 20s
	for attempt, wait := range waits {
		l := e.lease(1)[0]
		wantState(t, "attempt", l.Attempt, attempt+1)
		wantState(t, "state", e.fail(l, true), store.DeliveryPending)
		d := e.deliveries("r1")[0]
		if d.State != store.DeliveryPending || d.Attempt != attempt+2 || !d.NextAttemptAt.Equal(e.clock.Now().Add(wait)) ||
			d.ErrorClass != "test" || d.ErrorMessage != "boom" {
			t.Fatalf("after attempt %d: %+v; want pending, next attempt in %v", attempt+1, d, wait)
		}
		e.clock.Advance(wait)
	}
	l := e.lease(1)[0]
	wantState(t, "last attempt", l.Attempt, 3)
	wantState(t, "state", e.fail(l, true), store.DeliveryDead)
	if n := len(e.eventKinds(store.EventDeliveryRetry)); n != 2 {
		t.Fatalf("%d deliveryRetry events, want 2", n)
	}
	if dls := e.deadLetters("r1"); len(dls) != 1 || dls[0].Error != "test: boom" {
		t.Fatalf("dead letters = %+v", dls)
	}
}
