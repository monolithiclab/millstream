package storetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// budgeted is a crawl fanning out to an agent score step, under a run and a day budget filled in by budgetPipeline.
const budgeted = `
pipeline: listings
budget: { runUsd: %g, dayUsd: %g }
triggers:
  daily: { cron: "0 6 * * *" }
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.daily
    out: [listings.raw]
  score:
    action: ./actions/score
    in: listings.raw
    retry: { maxAttempts: 3, backoff: 10s, maxBackoff: 30s }
`

func budgetPipeline(runUSD, dayUSD float64) string { return fmt.Sprintf(budgeted, runUSD, dayUSD) }

var (
	scoreRef   = store.StepRef{Pipeline: "listings", Step: "score"}
	scoreAgent = store.AgentStep{StepRef: scoreRef, Revision: 1} // the score step of the first revision applied
)

// fanOut fires run runID and completes its crawl with n listings, so n score deliveries are due.
func (e *env) fanOut(runID string, n int) {
	e.t.Helper()
	e.firePipeline("listings", "daily", runID)
	ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node1", Max: 1, TTL: time.Minute,
		Slots: map[store.StepRef]int{scoreRef: 0}})
	if err != nil || len(ls) != 1 || ls[0].Step != "crawl" {
		e.t.Fatalf("lease crawl: %+v, %v", ls, err)
	}
	outs := make([]store.Output, n)
	for i := range outs {
		outs[i] = out(fmt.Sprintf("l%d", i), `{}`)
	}
	e.done(ls[0], outs...)
}

// leaseAgent leases up to n score deliveries as an agent step reserving reserve each.
func (e *env) leaseAgent(n int, reserve float64) []store.Lease {
	e.t.Helper()
	ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node1", Max: n, TTL: time.Minute,
		Reserve: map[store.AgentStep]float64{scoreAgent: reserve}})
	if err != nil {
		e.t.Fatalf("Lease: %v", err)
	}
	return ls
}

func (e *env) state() store.PipelineState {
	e.t.Helper()
	ps, err := e.s.PipelineState(e.ctx, "listings")
	if err != nil {
		e.t.Fatalf("PipelineState: %v", err)
	}
	return ps
}

func (e *env) doneCost(l store.Lease, cost float64) {
	e.t.Helper()
	if _, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, CostUSD: cost}); err != nil {
		e.t.Fatalf("Done: %v", err)
	}
}

func (e *env) delivery(id string) store.Delivery {
	e.t.Helper()
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: "listings"})
	if err != nil {
		e.t.Fatalf("Deliveries: %v", err)
	}
	for _, d := range ds {
		if d.ID == id {
			return d
		}
	}
	e.t.Fatalf("no delivery %s", id)
	return store.Delivery{}
}

func approx(a, b float64) bool { return a-b < 1e-9 && b-a < 1e-9 }

// A node that dies keeps its reservation: the expired lease charges it in full to the delivery, the run and the day.
// A failure without a reported cost does the same; a reported cost replaces the reservation.
func TestReservationChargedWhenUnreported(t *testing.T, open Opener) {
	e := newEnv(t, open, budgetPipeline(0, 0.25))
	e.fanOut("r1", 3)
	ls := e.leaseAgent(3, 0.1)
	wantState(t, "leased", len(ls), 2)
	e.clock.Advance(2 * time.Minute)
	expired, err := e.s.ExpireLeases(e.ctx)
	if err != nil || len(expired) != 2 {
		t.Fatalf("ExpireLeases = %+v, %v", expired, err)
	}
	for _, l := range ls {
		if d := e.delivery(l.DeliveryID); d.ReservedUSD != 0 || d.CostUSD != 0.1 || d.State != store.DeliveryPending {
			t.Fatalf("expired delivery = %+v", d)
		}
	}
	wantState(t, "run cost", e.run("r1").CostUSD, 0.2)
	// 0.2 spent today: one more reservation of 0.1 would pass 0.25.
	e.clock.Advance(time.Minute)
	if ls := e.leaseAgent(3, 0.1); len(ls) != 0 {
		t.Fatalf("leased %d after the expired leases were charged", len(ls))
	}
	if err := e.s.SetPaused(e.ctx, "listings", "", false); err != nil {
		t.Fatal(err)
	}
	// A reservation of 0.05 fits; the runtime then loses the process, so it is charged in full.
	l := e.leaseAgent(1, 0.05)[0]
	if _, err := e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Class: "crash",
		Retryable: true, Unreported: true}); err != nil {
		t.Fatal(err)
	}
	if d := e.delivery(l.DeliveryID); !approx(d.CostUSD, 0.15) || d.ReservedUSD != 0 {
		t.Fatalf("crashed delivery = %+v; want 0.1 expired and 0.05 crashed", d)
	}
	if c := e.run("r1").CostUSD; !approx(c, 0.25) {
		t.Fatalf("run cost = %v", c)
	}
}

// A reported failure cost replaces the reservation, and an agent step reserving nothing is still held back once the
// budget is reached.
func TestFailCostReplacesReservation(t *testing.T, open Opener) {
	e := newEnv(t, open, budgetPipeline(0.1, 0))
	e.fanOut("r1", 3)
	l := e.leaseAgent(1, 0.08)[0]
	if _, err := e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Class: "action",
		Retryable: false, CostUSD: 0.1}); err != nil {
		t.Fatal(err)
	}
	if d := e.delivery(l.DeliveryID); d.CostUSD != 0.1 || d.ReservedUSD != 0 {
		t.Fatalf("failed delivery = %+v", d)
	}
	if ls := e.leaseAgent(3, 0); len(ls) != 0 {
		t.Fatalf("leased %d reserving nothing with the budget reached", len(ls))
	}
	if e.state().PauseCauses["score"] != "budget.run" {
		t.Fatalf("state = %+v", e.state())
	}
}
