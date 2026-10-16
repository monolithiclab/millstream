package storetest

import (
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func TestLease(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
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

func TestRenew(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
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
