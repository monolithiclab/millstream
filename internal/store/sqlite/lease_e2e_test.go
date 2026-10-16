//go:build e2e

package sqlite

import (
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Lease variants the e2e build adds: the default build covers the same paths through the other lease tests.

// The dispatcher serves pipelines with ready deliveries in round-robin (spec section 4.10): a pipeline with a
// backlog never starves another, and the first pipeline served rotates.
func TestLeaseRoundRobin(t *testing.T) {
	e := newEnv(t, fixture, strings.Replace(fixture, "pipeline: listings", "pipeline: other", 1))
	for _, id := range []string{"a1", "a2", "a3", "a4"} {
		e.firePipeline("listings", "daily", id)
	}
	e.firePipeline("other", "daily", "b1")
	ls := e.lease(2)
	if len(ls) != 2 || ls[0].Pipeline == ls[1].Pipeline {
		t.Fatalf("leases = %+v; want one per pipeline", ls)
	}
	first := ls[0].Pipeline
	e.firePipeline("other", "daily", "b2")
	ls = e.lease(1)
	if len(ls) != 1 || ls[0].Pipeline == first {
		t.Fatalf("next lease from %s; want the rotation to start with the other pipeline", ls[0].Pipeline)
	}
}

func TestLeaseFencingTokenGrows(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	l := e.lease(1)[0]
	e.clock.Advance(2 * time.Minute)
	refs, err := e.s.ExpireLeases(e.ctx)
	if want := l.Ref(); err != nil || len(refs) != 1 || refs[0] != want {
		t.Fatalf("ExpireLeases = %+v, %v; want %+v", refs, err, want)
	}
	e.clock.Advance(time.Minute) // past the backoff
	l2 := e.lease(1)[0]
	if l2.FencingToken != l.FencingToken+1 {
		t.Fatalf("token %d after %d", l2.FencingToken, l.FencingToken)
	}
	// The first owner's messages are refused.
	_, err = e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1})
	wantErr(t, err, store.ErrStaleLease)
	if n := len(e.eventKinds(store.EventLeaseExpired)); n != 1 {
		t.Fatalf("%d leaseExpired events", n)
	}
	if refs, _ := e.s.ExpireLeases(e.ctx); len(refs) != 0 {
		t.Fatalf("expired live leases %+v", refs)
	}
}

func TestLeaseSkipsFutureRetries(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.fail(e.lease(1)[0], true)
	if ls := e.lease(1); len(ls) != 0 {
		t.Fatalf("leased a retry before its backoff: %+v", ls)
	}
	e.clock.Advance(5 * time.Second)
	if ls := e.lease(1); len(ls) != 1 || ls[0].Attempt != 2 {
		t.Fatalf("lease after backoff = %+v", ls)
	}
}

// An expired lease consumes its attempt (spec section 5.3): the delivery is retried after backoff while attempts
// remain, then ends dead with its dead letter, class leaseExpired.
func TestLeaseExpiryConsumesAttempts(t *testing.T) {
	e := newEnv(t, fixture) // crawl: maxAttempts 3, backoff 10s, jitter one half
	e.fire("r1")
	for attempt := 1; attempt <= 3; attempt++ {
		l := e.leaseStep("crawl")
		wantState(t, "attempt", l.Attempt, attempt)
		e.clock.Advance(2 * time.Minute)
		if refs, err := e.s.ExpireLeases(e.ctx); err != nil || len(refs) != 1 {
			t.Fatalf("ExpireLeases = %v, %v", refs, err)
		}
		d := e.deliveries("r1")[0]
		if d.ErrorClass != store.ClassLeaseExpired {
			t.Fatalf("delivery = %+v; want class leaseExpired", d)
		}
		if attempt < 3 {
			if d.State != store.DeliveryPending || d.Attempt != attempt+1 || !d.NextAttemptAt.After(e.clock.Now()) {
				t.Fatalf("after expiry %d: delivery = %+v; want pending, next attempt after backoff", attempt, d)
			}
			e.clock.Advance(time.Minute)
		} else if d.State != store.DeliveryDead {
			t.Fatalf("after the last attempt: delivery = %+v; want dead", d)
		}
	}
	dls := e.deadLetters("r1")
	if len(dls) != 1 || dls[0].Stream != "crawl.dlq" || !strings.HasPrefix(dls[0].Error, "leaseExpired: ") {
		t.Fatalf("dead letters = %+v", dls)
	}
}
