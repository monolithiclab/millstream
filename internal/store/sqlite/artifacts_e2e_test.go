//go:build e2e

package sqlite

import (
	"encoding/json"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// Artifact variants the e2e build adds: the default build covers the same paths through the other artifact tests.

// TestRepublishedItemCountsNothing: an output published again under the same id (a retry) is a no-op, its
// artifacts included.
func TestRepublishedItemCountsNothing(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	err := e.s.submit(e.ctx, "test", func(w *wtx) error {
		run, err := w.run("listings", "r1")
		if err != nil {
			return err
		}
		rev, err := w.revision(run.revision)
		if err != nil {
			return err
		}
		step, err := rev.step("crawl")
		if err != nil {
			return err
		}
		d, err := w.delivery("listings", ids.Delivery("crawl", ids.TriggerItem("daily", "r1"), 0))
		if err != nil {
			return err
		}
		o, err := canonical(json.RawMessage(`{"score": 1}`))
		if err != nil {
			return err
		}
		for range 2 {
			if err := w.publishOutput(rev, run, step, d, withArtifacts(store.Output{Key: "a"}, ref("x", shaA)), 0, o,
				routing{streams: []string{"listings.raw"}}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.refcounts("listings"); got[shaA] != 1 {
		t.Fatalf("refcounts = %v, want 1", got)
	}
}
