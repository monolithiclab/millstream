package sqlite

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
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

// Review focus 4: concurrent leases never reserve past the run budget, and the delivery the budget stops pauses the
// agent steps once, with the cause shown; resuming lifts the pause, and the run that reached its budget does not
// pause the pipeline again.
func TestRunBudgetConcurrentLeases(t *testing.T) {
	e := newEnv(t, budgetPipeline(0.3, 0))
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

// A node that dies keeps its reservation: the expired lease charges it in full to the delivery, the run and the day.
// A failure without a reported cost does the same; a reported cost replaces the reservation.
func TestReservationChargedWhenUnreported(t *testing.T) {
	e := newEnv(t, budgetPipeline(0, 0.25))
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
func TestFailCostReplacesReservation(t *testing.T) {
	e := newEnv(t, budgetPipeline(0.1, 0))
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

// A step a user paused keeps its pause, and its lack of a cause, when a budget pauses the others; resuming the
// pipeline does not lift it.
func TestBudgetLeavesUserPauses(t *testing.T) {
	e := newEnv(t, budgetPipeline(0.1, 0))
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
