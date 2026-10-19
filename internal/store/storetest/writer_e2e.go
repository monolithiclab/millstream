//go:build e2e

package storetest

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Units that fail after writing are rolled back alone, whatever batch they share with units that commit, and
// give their event sequence numbers back.
func TestFailingUnitsRollBackAlone(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	const n = 32 // enough concurrent units that batches mix failing and committing ones
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			if i%2 == 1 {
				errs[i] = e.s.Unit(e.ctx, func(exec func(string, ...any) error) error {
					if err := exec(`INSERT INTO events (seq, ts, pipeline, run_id, kind, data)
						VALUES ((SELECT COALESCE(MAX(seq), 0) + 1 FROM events), ?, 'listings', '', 'doomed', '{}')`,
						t0.Format(time.RFC3339)); err != nil {
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
