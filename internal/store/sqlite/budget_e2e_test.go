//go:build e2e

package sqlite

import (
	"slices"
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

// Deliveries a run budget holds back are read once per lease, not once per pass: with 200 of them due, a lease
// takes two passes (one denies the run, the next excludes it) and reads a bounded number of rows, and another run's
// agent delivery and another step's delivery still lease.
func TestBudgetHeldBackDeliveriesReadOnce(t *testing.T) {
	e := newEnv(t, budgetPipeline(0.1, 0))
	e.fanOut("r1", 2)
	if ls := e.leaseAgent(1, 0.1); len(ls) != 1 {
		t.Fatalf("first lease = %+v", ls)
	}
	if ls := e.leaseAgent(1, 0.1); len(ls) != 0 { // reaches r1's budget, pausing the step
		t.Fatalf("lease past r1's budget = %+v", ls)
	}
	if err := e.s.SetPaused(e.ctx, "listings", "", false); err != nil {
		t.Fatal(err)
	}
	// 199 more of r1's score deliveries, copies of its pending one under later generations: written directly, a
	// crawl emitting 200 items taking longer than the budget of a test.
	if _, err := e.s.write.ExecContext(e.ctx, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 199)
		INSERT INTO deliveries (pipeline, id, run_id, step, item_id, generation, state, attempt, next_attempt_at)
		SELECT d.pipeline, d.id || '-' || n.i, d.run_id, d.step, d.item_id, d.generation + n.i, d.state, d.attempt,
			d.next_attempt_at
		FROM deliveries d, n WHERE d.step = 'score' AND d.state = ?`, store.DeliveryPending); err != nil {
		t.Fatal(err)
	}
	e.fanOut("r2", 1)
	e.firePipeline("listings", "daily", "r3") // its crawl is due

	var passes, rows int
	e.s.leasePassHook = func(queues map[string][]candidate) {
		passes++
		for _, q := range queues {
			rows += len(q)
		}
	}
	defer func() { e.s.leasePassHook = nil }()
	ls := e.leaseAgent(10, 0.1)
	var got []string
	for _, l := range ls {
		got = append(got, l.RunID+"/"+l.Step)
	}
	slices.Sort(got)
	if !slices.Equal(got, []string{"r2/score", "r3/crawl"}) {
		t.Fatalf("leased %v, want r2's score and r3's crawl", got)
	}
	if passes > 2 || rows > 2*10+2 {
		t.Fatalf("%d passes read %d candidates, want at most 2 passes and 22 candidates", passes, rows)
	}
}
