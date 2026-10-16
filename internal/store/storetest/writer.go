package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/store"
)

// checkEvents asserts the event log is gap-free from 1 and strictly increasing.
func checkEvents(t *testing.T, e *env) []store.Event {
	t.Helper()
	evs := e.events()
	for i, ev := range evs {
		if ev.Seq != int64(i+1) {
			t.Fatalf("event %d has seq %d: the log has a gap or is out of order", i, ev.Seq)
		}
	}
	return evs
}

var errUnit = errors.New("unit failed on purpose")

// Many concurrent Done calls batch into shared transactions and each publishes exactly its own outputs.
func TestConcurrentDone(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	const runs = concurrentDones
	for i := range runs {
		e.fire(fmt.Sprint("r", i))
	}
	leases := e.lease(runs)
	if len(leases) != runs {
		t.Fatalf("leased %d", len(leases))
	}
	var wg sync.WaitGroup
	errs := make([]error, runs)
	for i, l := range leases {
		wg.Go(func() {
			_, errs[i] = e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1, Outputs: []store.Output{
				out("a", fmt.Sprintf(`{"score":%d}`, i)), out("b", `{"score":99}`),
			}})
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Done %d: %v", i, err)
		}
	}
	if n := len(e.items("listings.raw")); n != 2*runs {
		t.Fatalf("%d raw items, want %d", n, 2*runs)
	}
	ds, _ := e.s.Deliveries(e.ctx, store.DeliveryFilter{State: store.DeliveryPending})
	if len(ds) != 4*runs { // score and audit for each of two outputs
		t.Fatalf("%d pending deliveries, want %d", len(ds), 4*runs)
	}
	checkEvents(t, e)
}

func TestCloseDrainsQueuedUnits(t *testing.T, open Opener) {
	s := open(t, Options{})
	ctx := context.Background()
	if _, err := s.ApplyRevision(ctx, store.PackageRecord{Hash: "h", Pipeline: parse(t, fixture)}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	const n = 16
	results := make(chan error, n)
	for i := range n {
		wg.Go(func() {
			_, err := s.Fire(ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: fmt.Sprint(i), RunID: fmt.Sprint(i)})
			results <- err
		})
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	wg.Wait()
	close(results)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	// Every unit either committed or was refused because the store was closing: none hangs or half-applies.
	for err := range results {
		if err != nil && !errors.Is(err, store.ErrClosed) {
			t.Fatalf("Fire: %v", err)
		}
	}
}

// The failpoint tests arm process-wide state, so they are not parallel: Go runs them before any parallel test
// resumes.

// A crash before the commit leaves no trace of the unit.
func TestFailpointBeforeCommit(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	l := e.lease(1)[0]
	before := len(e.events())

	reset := failpoint.Set("done.beforeCommit")
	_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1, Outputs: []store.Output{out("a", `{"score":1}`)}})
	reset()
	var crash failpoint.Crash
	if !errors.As(err, &crash) || crash.Name != "done.beforeCommit" {
		t.Fatalf("Done = %v, want the failpoint crash", err)
	}
	if d := e.deliveries("r1")[0]; d.State != store.DeliveryLeased {
		t.Fatalf("delivery %s after a crash before commit", d.State)
	}
	if n := len(e.items("listings.raw")); n != 0 || len(e.events()) != before {
		t.Fatalf("%d items, %d new events after a crash before commit", n, len(e.events())-before)
	}
	// The store carries on, and the retry publishes once.
	e.done(l, out("a", `{"score":1}`))
	wantState(t, "raw items", len(e.items("listings.raw")), 1)
	checkEvents(t, e)
}

// A crash after the commit reports failure, but the unit committed.
func TestFailpointAfterCommit(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	reset := failpoint.Set("fire.afterCommit")
	_, err := e.s.Fire(e.ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: "k", RunID: "r1"})
	reset()
	if !errors.As(err, new(failpoint.Crash)) {
		t.Fatalf("Fire = %v, want the failpoint crash", err)
	}
	if r := e.run("r1"); r.State != store.RunRunning {
		t.Fatalf("run %s", r.State)
	}
	// Firing again is the no-op a restarted engine relies on.
	created, err := e.s.Fire(e.ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: "k", RunID: "r1"})
	if err != nil || created {
		t.Fatalf("refire = %v, %v", created, err)
	}
}
