package sqlite

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

// Units that fail after writing are rolled back alone, whatever batch they share with units that commit, and
// give their event sequence numbers back.
func TestFailingUnitsRollBackAlone(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	const n = 32 // enough concurrent units that batches mix failing and committing ones
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			if i%2 == 1 {
				errs[i] = e.s.submit(e.ctx, "test", func(w *wtx) error {
					if err := w.event("listings", "", "doomed", nil); err != nil {
						return err
					}
					return errUnit
				})
				return
			}
			_, errs[i] = e.s.Fire(e.ctx, store.Firing{Pipeline: "listings", Trigger: "daily",
				Key: fmt.Sprint(i), RunID: fmt.Sprint("r", i)})
		})
	}
	wg.Wait()
	for i, err := range errs {
		if i%2 == 1 && !errors.Is(err, errUnit) || i%2 == 0 && err != nil {
			t.Fatalf("unit %d: %v", i, err)
		}
	}
	evs := checkEvents(t, e)
	for _, ev := range evs {
		if ev.Kind == "doomed" {
			t.Fatal("a failed unit's event committed")
		}
	}
	runs, err := e.s.Runs(e.ctx, store.RunFilter{})
	if err != nil || len(runs) != n/2 {
		t.Fatalf("%d runs (%v), want %d", len(runs), err, n/2)
	}
}

// Many concurrent Done calls batch into shared transactions and each publishes exactly its own outputs.
func TestConcurrentDone(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
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

func TestCloseDrainsQueuedUnits(t *testing.T) {
	t.Parallel()
	s, err := Open(Memory, Options{})
	if err != nil {
		t.Fatal(err)
	}
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
func TestFailpointBeforeCommit(t *testing.T) {
	e := newEnv(t, fixture)
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
func TestFailpointAfterCommit(t *testing.T) {
	e := newEnv(t, fixture)
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

func TestUnitsAreNamed(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for _, u := range Units {
		if u == "" || seen[u] {
			t.Fatalf("unit name %q empty or repeated", u)
		}
		seen[u] = true
	}
	wantState(t, "units", len(Units), 19)
}

// A unit whose savepoint cannot be rolled back leaves the transaction in an unknown state: the whole batch is
// aborted, with an error for every unit in it, and nothing commits.
func TestRollbackFailureAbortsBatch(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	unit := func(fn func(*wtx) error) *request {
		return &request{ctx: e.ctx, unit: "test", fn: fn, done: make(chan error, 1)}
	}
	good := func(w *wtx) error { return w.event("listings", "", "kept?", nil) }
	sabotage := func(w *wtx) error {
		if err := w.event("listings", "", "doomed", nil); err != nil {
			return err
		}
		if _, err := w.exec(`RELEASE unit`); err != nil { // ROLLBACK TO unit now has no savepoint to go back to
			return err
		}
		return errUnit
	}
	results := e.s.commit([]*request{unit(good), unit(sabotage), unit(good)})
	for i, err := range results {
		if err == nil {
			t.Errorf("unit %d committed", i)
		}
	}
	if !errors.Is(results[1], errUnit) {
		t.Errorf("sabotaged unit: %v, want its own error", results[1])
	}
	if evs := e.events(); len(evs) != 1 || evs[0].Kind != store.EventRevisionApplied {
		t.Fatalf("events = %+v; want only the revision applied", evs)
	}
}
