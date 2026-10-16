//go:build e2e

package sqlite

import (
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Budget variants the e2e build adds: the default build covers the same paths through the other budget tests.

// The day budget is per pipeline and UTC day of the store clock: it counts every run, and a new day starts from
// nothing.
func TestDayBudgetRollsOverByUTCDay(t *testing.T) {
	e := newEnv(t, budgetPipeline(0, 0.2))
	e.clock.Advance(17*time.Hour + 30*time.Minute) // 23:30 UTC
	e.fanOut("r1", 2)
	e.fanOut("r2", 2)
	ls := e.leaseAgent(10, 0.1)
	wantState(t, "leased within the day", len(ls), 2)
	ps := e.state()
	if ps.PauseCauses["score"] != "budget.day" {
		t.Fatalf("state = %+v", ps)
	}
	evs := e.eventKinds(store.EventBudgetReached)
	if len(evs) != 1 || evs[0].RunID != "" || !strings.Contains(string(evs[0].Data), `"scope":"day"`) {
		t.Fatalf("budgetReached = %+v", evs)
	}
	for _, l := range ls {
		e.doneCost(l, 0.1)
	}
	if err := e.s.SetPaused(e.ctx, "listings", "score", false); err != nil {
		t.Fatal(err)
	}
	if ls := e.leaseAgent(10, 0.1); len(ls) != 0 {
		t.Fatalf("leased %d with the day spent", len(ls))
	}
	e.clock.Advance(time.Hour) // 00:30 UTC, the next day
	ls = e.leaseAgent(10, 0.1)
	wantState(t, "leased the next day", len(ls), 2)
	wantState(t, "budgetReached events", len(e.eventKinds(store.EventBudgetReached)), 1)
}

// A delivery a budget holds back does not take its step's slot: a run within its budget is still served past the
// older deliveries of a run that reached its own.
func TestBudgetSkipsPastHeldBackDeliveries(t *testing.T) {
	e := newEnv(t, budgetPipeline(0.1, 0))
	e.fanOut("r1", 3)
	e.fanOut("r2", 3)
	req := store.LeaseRequest{Owner: "node1", Max: 1, TTL: time.Minute,
		Reserve: map[store.StepRef]float64{scoreRef: 0.1}, Slots: map[store.StepRef]int{scoreRef: 1}}
	first, err := e.s.Lease(e.ctx, req)
	if err != nil || len(first) != 1 || first[0].RunID != "r1" {
		t.Fatalf("first lease = %+v, %v", first, err)
	}
	if err := e.s.SetPaused(e.ctx, "listings", "", false); err != nil { // nothing paused yet: a no-op
		t.Fatal(err)
	}
	ls, err := e.s.Lease(e.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	// r1's next delivery reaches the run budget and pauses the step, so nothing more is leased in this call.
	if len(ls) != 0 || e.state().PauseCauses["score"] != "budget.run" {
		t.Fatalf("lease past r1's budget = %+v, state %+v", ls, e.state())
	}
	if err := e.s.SetPaused(e.ctx, "listings", "", false); err != nil {
		t.Fatal(err)
	}
	ls, err = e.s.Lease(e.ctx, req)
	if err != nil || len(ls) != 1 || ls[0].RunID != "r2" {
		t.Fatalf("after resume = %+v, %v; want r2 served past r1's held-back deliveries", ls, err)
	}
}
