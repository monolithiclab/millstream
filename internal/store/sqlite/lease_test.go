package sqlite

import (
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func TestLease(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.fire("r2")
	ls := e.lease(1)
	if len(ls) != 1 {
		t.Fatalf("leased %d, want 1", len(ls))
	}
	l := ls[0]
	if l.RunID != "r1" || l.Step != "crawl" || l.Attempt != 1 || l.FencingToken != 1 || l.Revision != 1 ||
		!l.ExpiresAt.Equal(t0.Add(time.Minute)) || l.Item.Stream != "trigger.daily" || string(l.Item.Payload) != "{}" {
		t.Fatalf("lease = %+v; want the oldest delivery, token 1", l)
	}
	d := e.deliveries("r1")[0]
	if d.State != store.DeliveryLeased || d.LeaseOwner != "node1" || d.FencingToken != 1 {
		t.Fatalf("delivery = %+v", d)
	}
	if ls := e.lease(10); len(ls) != 1 || ls[0].RunID != "r2" {
		t.Fatalf("second lease = %+v, want r2 only", ls)
	}
	if ls := e.lease(10); len(ls) != 0 {
		t.Fatalf("leased %d with nothing due", len(ls))
	}
	if n := len(e.eventKinds(store.EventDeliveryLeased)); n != 2 {
		t.Fatalf("%d deliveryLeased events, want 2", n)
	}

	if ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "n", TTL: time.Second}); err != nil || ls != nil {
		t.Fatalf("Max 0 = %v, %v", ls, err)
	}
	_, err := e.s.Lease(e.ctx, store.LeaseRequest{Max: 1, TTL: time.Second})
	wantErr(t, err, store.ErrInvalid)
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

func TestLeaseSlotsAndExclusions(t *testing.T) {
	e := newEnv(t, fixture)
	for _, id := range []string{"r1", "r2", "r3"} {
		e.fire(id)
	}
	crawl := store.StepRef{Pipeline: "listings", Step: "crawl"}
	tests := []struct {
		name string
		req  store.LeaseRequest
		want int
	}{
		{"slots cap the step", store.LeaseRequest{Slots: map[store.StepRef]int{crawl: 2}}, 2},
		{"zero slots", store.LeaseRequest{Slots: map[store.StepRef]int{crawl: 0}}, 0},
		{"other steps uncapped", store.LeaseRequest{Slots: map[store.StepRef]int{{Pipeline: "listings", Step: "x"}: 0}}, 3},
		{"step excluded", store.LeaseRequest{Exclude: []store.StepRef{crawl}}, 0},
		{"pipeline excluded", store.LeaseRequest{Exclude: []store.StepRef{{Pipeline: "listings"}}}, 0},
		{"other pipeline excluded", store.LeaseRequest{Exclude: []store.StepRef{{Pipeline: "other"}}}, 3},
	}
	for _, tt := range tests {
		// Not parallel: each case leases and then expires what it leased.
		req := tt.req
		req.Owner, req.Max, req.TTL = "n", 10, time.Minute
		ls, err := e.s.Lease(e.ctx, req)
		if err != nil || len(ls) != tt.want {
			t.Fatalf("%s: leased %d (%v), want %d", tt.name, len(ls), err, tt.want)
		}
		e.clock.Advance(2 * time.Minute)
		if _, err := e.s.ExpireLeases(e.ctx); err != nil {
			t.Fatal(err)
		}
	}
}

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

func TestRenew(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.fire("r2")
	ls := e.lease(2)
	e.clock.Advance(30 * time.Second)
	stale := ls[1].Ref()
	stale.FencingToken++
	lost, err := e.s.Renew(e.ctx, "node1", []store.LeaseRef{ls[0].Ref(), stale}, time.Minute)
	if err != nil || len(lost) != 1 || lost[0] != stale {
		t.Fatalf("Renew = %v, %v; want the stale ref lost", lost, err)
	}
	if lost, _ := e.s.Renew(e.ctx, "node2", []store.LeaseRef{ls[0].Ref()}, time.Minute); len(lost) != 1 {
		t.Fatalf("another owner renewed: lost %v", lost)
	}
	e.clock.Advance(45 * time.Second) // 75 s after the lease: only the renewed one survives
	if refs, _ := e.s.ExpireLeases(e.ctx); len(refs) != 1 {
		t.Fatalf("expired %+v, want 1", refs)
	}
	if d := e.deliveries("r1")[0]; d.State != store.DeliveryLeased || !d.LeaseExpiresAt.Equal(t0.Add(90*time.Second)) {
		t.Fatalf("renewed delivery = %+v", d)
	}
	_, err = e.s.Renew(e.ctx, "node1", nil, 0)
	wantErr(t, err, store.ErrInvalid)
}
