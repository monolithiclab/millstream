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
