//go:build e2e

package sqlite

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// Dead-letter variants the e2e build adds: the default build covers the same paths through the other replay tests.

// A delivery dead again after a replay publishes a new dead letter, under the next failure sequence, so readers of
// the dlq stream see the new failure; the replayed one stays replayed.
func TestReplayDeadAgainCreatesNewDeadLetter(t *testing.T) {
	e, l := deadRun(t)
	if _, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, false); err != nil {
		t.Fatal(err)
	}
	e.fail(e.leaseStep("crawl"), false)
	dls := e.deadLetters("r1")
	states := map[string]store.DeadLetterState{}
	for _, dl := range dls {
		states[dl.ID] = dl.State
	}
	first, second := ids.DeadLetter("crawl.dlq", l.DeliveryID, "", 0), ids.DeadLetter("crawl.dlq", l.DeliveryID, "", 1)
	if len(dls) != 2 || states[first] != store.DeadLetterReplayed || states[second] != store.DeadLetterOpen {
		t.Fatalf("dead letters = %+v; want %s replayed and %s open", dls, first, second)
	}
	wantState(t, "dlq items", len(e.items("crawl.dlq")), 2)
	e.drain() // alert reads the new dead letter
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
}

// A run whose dead letter is discarded before it settles settles completedWithErrors.
func TestDiscardBeforeSettlement(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.fail(e.leaseStep("crawl"), false)
	dl := e.deadLetters("r1")[0]
	if _, err := e.s.DiscardDeadLetters(e.ctx, "listings", []string{dl.ID}); err != nil {
		t.Fatal(err)
	}
	wantState(t, "run", e.run("r1").State, store.RunRunning) // alert still pending
	e.drain()
	wantState(t, "run", e.run("r1").State, store.RunCompletedWithErrors)
}
