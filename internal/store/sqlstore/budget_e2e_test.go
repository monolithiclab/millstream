//go:build e2e

package sqlstore

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Deliveries a run budget holds back are read once per lease, not once per pass: with 200 of them due, a lease
// takes two passes (one denies the run, the next excludes it) and reads a bounded number of rows, and another run's
// agent delivery and another step's delivery still lease.
func TestBudgetHeldBackDeliveriesReadOnce(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, `
pipeline: listings
budget: { runUsd: 0.1 }
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
`)
	scoreRef := store.StepRef{Pipeline: "listings", Step: "score"}
	agent := map[store.AgentStep]float64{{StepRef: scoreRef, Revision: 1}: 0.1}
	lease := func(n int, req store.LeaseRequest) []store.Lease {
		t.Helper()
		req.Owner, req.Max, req.TTL = "node1", n, time.Minute
		ls, err := s.Lease(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return ls
	}
	fire := func(runID string) {
		t.Helper()
		if _, err := s.Fire(ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: runID, RunID: runID}); err != nil {
			t.Fatal(err)
		}
	}
	fanOut := func(runID string, n int) { // n score deliveries of runID due
		t.Helper()
		fire(runID)
		ls := lease(1, store.LeaseRequest{Slots: map[store.StepRef]int{scoreRef: 0}})
		outs := make([]store.Output, n)
		for i := range outs {
			outs[i] = store.Output{Key: fmt.Sprintf("l%d", i)}
		}
		if _, err := s.Done(ctx, store.DoneRequest{LeaseRef: ls[0].Ref(), Attempt: 1, Outputs: outs}); err != nil {
			t.Fatal(err)
		}
	}
	fanOut("r1", 2)
	if ls := lease(1, store.LeaseRequest{Reserve: agent}); len(ls) != 1 {
		t.Fatalf("first lease = %+v", ls)
	}
	if ls := lease(1, store.LeaseRequest{Reserve: agent}); len(ls) != 0 { // reaches r1's budget, pausing the step
		t.Fatalf("lease past r1's budget = %+v", ls)
	}
	if err := s.SetPaused(ctx, "listings", "", false); err != nil {
		t.Fatal(err)
	}
	// 199 more of r1's score deliveries, copies of its pending one under later generations: written directly, a
	// crawl emitting 200 items taking longer than the budget of a test.
	if err := s.Exec(ctx, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 199)
		INSERT INTO deliveries (pipeline, id, run_id, step, item_id, generation, state, attempt, next_attempt_at,
			created_at)
		SELECT d.pipeline, d.id || '-' || n.i, d.run_id, d.step, d.item_id, d.generation + n.i, d.state, d.attempt,
			d.next_attempt_at, d.created_at
		FROM deliveries d, n WHERE d.step = 'score' AND d.state = ?`, store.DeliveryPending); err != nil {
		t.Fatal(err)
	}
	fanOut("r2", 1)
	fire("r3") // its crawl is due

	var passes, rows int
	s.leasePassHook = func(queues map[string][]candidate) {
		passes++
		for _, q := range queues {
			rows += len(q)
		}
	}
	defer func() { s.leasePassHook = nil }()
	var got []string
	for _, l := range lease(10, store.LeaseRequest{Reserve: agent}) {
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
