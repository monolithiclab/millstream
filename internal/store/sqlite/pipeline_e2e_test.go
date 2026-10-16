//go:build e2e

package sqlite

import (
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/store"
)

// Firing variants the e2e build adds: the default build covers the same paths through TestFire.

// Two pipelines with a trigger of the same name fire runs and trigger items with the same ids; the pipeline id
// keeps them apart (spec section 4.10).
func TestFireIsolatesPipelines(t *testing.T) {
	e := newEnv(t, fixture, strings.Replace(fixture, "pipeline: listings", "pipeline: other", 1))
	e.firePipeline("listings", "daily", "run_x_daily")
	e.firePipeline("other", "daily", "run_x_daily")
	ls := e.lease(10)
	if len(ls) != 2 || ls[0].Pipeline == ls[1].Pipeline || ls[0].DeliveryID != ls[1].DeliveryID {
		t.Fatalf("leases = %+v; want the same delivery id in both pipelines", ls)
	}
	for _, l := range ls {
		e.done(l, out("a", `{"score":1}`))
	}
	other, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: "other", Stream: "listings.raw"})
	if err != nil || len(other) != 1 || len(e.items("listings.raw")) != 1 {
		t.Fatalf("items: other %v (%v), listings %v", other, err, e.items("listings.raw"))
	}
}

func TestFireWithoutConsumersSettles(t *testing.T) {
	e := newEnv(t, fixture)
	e.firePipeline("listings", "adhoc", "m1")
	r := e.run("m1")
	if r.State != store.RunSucceeded || !r.SettledAt.Equal(t0) {
		t.Fatalf("run = %+v, want succeeded at once", r)
	}
}
