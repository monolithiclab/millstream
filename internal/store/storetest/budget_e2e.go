//go:build e2e

package storetest

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Budget variants the e2e build adds: the default build covers the same paths through the other budget tests.

// The day budget is per pipeline and UTC day of the store clock: it counts every run, and a new day starts from
// nothing.
func TestDayBudgetRollsOverByUTCDay(t *testing.T, open Opener) {
	e := newEnv(t, open, budgetPipeline(0, 0.2))
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
func TestBudgetSkipsPastHeldBackDeliveries(t *testing.T, open Opener) {
	e := newEnv(t, open, budgetPipeline(0.1, 0))
	e.fanOut("r1", 3)
	e.fanOut("r2", 3)
	req := store.LeaseRequest{Owner: "node1", Max: 1, TTL: time.Minute,
		Reserve: map[store.AgentStep]float64{scoreAgent: 0.1}, Slots: map[store.StepRef]int{scoreRef: 1}}
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

// Review focus 4: concurrent leases never reserve past the run budget, and the delivery the budget stops pauses the
// agent steps once, with the cause shown; resuming lifts the pause, and the run that reached its budget does not
// pause the pipeline again.
func TestRunBudgetConcurrentLeases(t *testing.T, open Opener) {
	e := newEnv(t, open, budgetPipeline(0.3, 0))
	e.fanOut("r1", 8)
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		leased []store.Lease
	)
	for range concurrentLeases {
		wg.Go(func() {
			ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node1", Max: 2, TTL: time.Minute,
				Reserve: map[store.AgentStep]float64{scoreAgent: 0.1}})
			if err != nil {
				t.Errorf("Lease: %v", err)
			}
			mu.Lock()
			leased = append(leased, ls...)
			mu.Unlock()
		})
	}
	wg.Wait()
	wantState(t, "leased within a budget of three reservations", len(leased), 3)
	for _, l := range leased {
		if d := e.delivery(l.DeliveryID); d.ReservedUSD != 0.1 {
			t.Fatalf("delivery %s reserved %v, want 0.1", d.ID, d.ReservedUSD)
		}
	}
	evs := e.eventKinds(store.EventBudgetReached)
	wantState(t, "budgetReached events", len(evs), 1)
	var data struct {
		Scope           string
		Limit, Reserved float64
		Steps           []string
	}
	if err := json.Unmarshal(evs[0].Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Scope != store.BudgetRun || data.Limit != 0.3 || !approx(data.Reserved, 0.3) ||
		strings.Join(data.Steps, ",") != "score" || evs[0].RunID != "r1" {
		t.Fatalf("budgetReached = %s run %q", evs[0].Data, evs[0].RunID)
	}
	ps := e.state()
	if strings.Join(ps.PausedSteps, ",") != "score" || ps.PauseCauses["score"] != "budget.run" {
		t.Fatalf("pipeline state = %+v", ps)
	}

	// Done replaces the reservation with the reported cost.
	e.doneCost(leased[0], 0.05)
	if d := e.delivery(leased[0].DeliveryID); d.ReservedUSD != 0 || d.CostUSD != 0.05 {
		t.Fatalf("done delivery = %+v", d)
	}
	wantState(t, "run cost", e.run("r1").CostUSD, 0.05)

	if err := e.s.SetPaused(e.ctx, "listings", "", false); err != nil {
		t.Fatal(err)
	}
	if ps := e.state(); len(ps.PausedSteps) != 0 || ps.PauseCauses != nil {
		t.Fatalf("after resume: %+v", ps)
	}
	// 0.05 spent and 0.2 reserved: another 0.1 would pass 0.3. Held back, without pausing again for this run.
	if ls := e.leaseAgent(5, 0.1); len(ls) != 0 {
		t.Fatalf("leased %d past the budget", len(ls))
	}
	wantState(t, "budgetReached events", len(e.eventKinds(store.EventBudgetReached)), 1)
	if ps := e.state(); len(ps.PausedSteps) != 0 {
		t.Fatalf("paused again: %+v", ps)
	}
	// Spending less than reserved makes room again.
	e.doneCost(leased[1], 0.01)
	e.doneCost(leased[2], 0.01)
	if ls := e.leaseAgent(5, 0.1); len(ls) != 2 {
		t.Fatalf("leased %d with 0.07 spent, want 2", len(ls))
	}
}

// A step a user paused keeps its pause, and its lack of a cause, when a budget pauses the others; resuming the
// pipeline does not lift it.
func TestBudgetLeavesUserPauses(t *testing.T, open Opener) {
	e := newEnv(t, open, budgetPipeline(0.1, 0))
	e.fanOut("r1", 3)
	e.leaseAgent(1, 0.1)
	if err := e.s.SetPaused(e.ctx, "listings", "crawl", true); err != nil {
		t.Fatal(err)
	}
	ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node1", Max: 1, TTL: time.Minute,
		Reserve: map[store.AgentStep]float64{scoreAgent: 0.1, {StepRef: store.StepRef{Pipeline: "listings", Step: "crawl"}, Revision: 1}: 0.1}})
	if err != nil || len(ls) != 0 {
		t.Fatalf("Lease = %+v, %v", ls, err)
	}
	ps := e.state()
	if strings.Join(ps.PausedSteps, ",") != "crawl,score" || ps.PauseCauses["crawl"] != "" ||
		ps.PauseCauses["score"] != "budget.run" {
		t.Fatalf("state = %+v", ps)
	}
	if err := e.s.SetPaused(e.ctx, "listings", "", false); err != nil {
		t.Fatal(err)
	}
	if ps := e.state(); strings.Join(ps.PausedSteps, ",") != "crawl" {
		t.Fatalf("after resume = %+v", ps)
	}
}
