//go:build e2e

package storetest

import (
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Lease variants the e2e build adds: the default build covers the same paths through the other lease tests.

// The dispatcher serves pipelines with ready deliveries in round-robin (spec section 4.10): a pipeline with a
// backlog never starves another, each call carries on after the pipeline served last, and a pipeline the engine
// holds back (at its limit) drops out of the rotation without giving the next one an extra turn.
func TestLeaseRoundRobin(t *testing.T, open Opener) {
	third := strings.Replace(fixture, "pipeline: listings", "pipeline: third", 1)
	e := newEnv(t, open, fixture, strings.Replace(fixture, "pipeline: listings", "pipeline: other", 1), third)
	for _, name := range []string{"listings", "other", "third"} {
		for _, id := range []string{"a1", "a2", "a3", "a4"} {
			e.firePipeline(name, "daily", id)
		}
	}
	ls := e.lease(2)
	if len(ls) != 2 || ls[0].Pipeline != "listings" || ls[1].Pipeline != "other" {
		t.Fatalf("leases = %+v; want listings then other", ls)
	}
	order := func(req store.LeaseRequest) string {
		t.Helper()
		req.Owner, req.Max, req.TTL = "node1", 1, time.Minute
		got, err := e.s.Lease(e.ctx, req)
		if err != nil || len(got) != 1 {
			t.Fatalf("Lease = %+v, %v", got, err)
		}
		return got[0].Pipeline
	}
	var served []string
	for range 3 {
		served = append(served, order(store.LeaseRequest{}))
	}
	if strings.Join(served, " ") != "third listings other" {
		t.Fatalf("served %v; want the rotation to carry on after other", served)
	}
	// listings is at its limit: other and third alternate, neither taking its turn.
	served = nil
	for range 4 {
		served = append(served, order(store.LeaseRequest{PipelineSlots: map[string]int{"listings": 0}}))
	}
	if strings.Join(served, " ") != "third other third other" {
		t.Fatalf("served %v with listings held back; want other and third in turn", served)
	}
}

func TestLeaseFencingTokenGrows(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
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

func TestLeaseSkipsFutureRetries(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
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
func TestLeaseExpiryConsumesAttempts(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture) // crawl: maxAttempts 3, backoff 10s, jitter one half
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

func TestLeaseSlotsAndExclusions(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
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
		{"only another step", store.LeaseRequest{Only: []store.StepRef{{Pipeline: "listings", Step: "x"}}}, 0},
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
	// Every case above but the zeros spent an attempt: Only's positive case leases from a store of its own.
	e = newEnv(t, open, fixture)
	e.fire("r1")
	if ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "n", Max: 10, TTL: time.Minute,
		Only: []store.StepRef{crawl}}); err != nil || len(ls) != 1 {
		t.Fatalf("only the step: leased %d (%v), want 1", len(ls), err)
	}
}
