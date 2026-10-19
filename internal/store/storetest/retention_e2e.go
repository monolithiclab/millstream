//go:build e2e

package storetest

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/metrics"
	"github.com/monolithiclab/millstream/internal/store"
)

// Retention variants the e2e build adds: their paths are covered in the default build by the other tests.

// TestMetricTotalsSurviveRetention: the totals are running sums the rollup keeps beside its minutes, so retention
// deleting the minutes leaves them as they were, and the next pass adds to them: they never fall back, and equal the
// sums of every minute ever rolled up.
func TestMetricTotalsSurviveRetention(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	e.rollup()
	first := e.allRollups()
	before := e.totals()
	e.clock.Advance(25 * time.Hour)
	if res := e.retain(store.RetentionRequest{Rollups: 24 * time.Hour}); res.Rollups != len(first) {
		t.Fatalf("retention = %+v, want every rollup (%d) deleted", res, len(first))
	}
	if got := e.totals(); !reflect.DeepEqual(got, before) {
		t.Fatalf("totals after retention\n%+v\nwant\n%+v", got, before)
	}
	e.fire("r2")
	e.rollup()
	got := e.totals()
	if want := sortedTotals(metrics.Totals(append(first, e.allRollups()...))); !reflect.DeepEqual(got, want) {
		t.Fatalf("totals\n%+v\nwant the sums of every minute rolled up\n%+v", got, want)
	}
	for _, b := range before {
		i := slices.IndexFunc(got, func(g store.MetricTotal) bool {
			return g.Pipeline == b.Pipeline && g.Name == b.Name && reflect.DeepEqual(g.Labels, b.Labels)
		})
		if i < 0 || got[i].Count < b.Count || b.Name == metrics.ItemsOut && got[i].Sum <= b.Sum {
			t.Errorf("series %+v fell back or stood still: %+v", b, got)
		}
	}
}
