//go:build !e2e

package cli

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

// e2eBuild says whether this is the e2e build. The default build skips the parts of a test that run the engine
// end to end or go through state database files, which the e2e build runs as well.
const e2eBuild = false

// runFixture is the run fixture, loaded once: engines only read it.
var runFixture = sync.OnceValues(func() (*pipeline.Package, error) { return pipeline.Load(fixture("run")) })

// newState opens an in-memory store and settles run r1 of the run fixture in it through engine stubs, the outcomes
// the fake action would give: crawl emits k0, k1 and k2, and score succeeds or, with scoreErrors, fails each item
// as the fake action's error mode does; that failing run's projections (metrics, insights) are made too, as run
// --once makes them. db names a database that is never created: the commands use the store.
func newState(t *testing.T, scoreErrors bool) *state {
	t.Helper()
	pkg, err := runFixture()
	if err != nil {
		t.Fatal(err)
	}
	st := memoryStore(t)
	steps := func(name string) store.StepRef { return store.StepRef{Pipeline: "listings", Step: name} }
	crawl := engine.Stub{}
	for i, key := range []string{"k0", "k1", "k2"} {
		crawl.Outputs = append(crawl.Outputs, store.Output{Key: key, Payload: json.RawMessage{byte('0' + i)}})
	}
	score := engine.Stub{}
	if scoreErrors {
		score.Err = &exec.ActionError{Message: "portal answered 503"}
	}
	e, err := engine.New(engine.Options{Store: st, NoCron: true,
		Stubs: map[store.StepRef]engine.Stub{steps("crawl"): crawl, steps("score"): score}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := e.Apply(ctx, pkg); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Fire(ctx, "listings", "adhoc", "r1", nil); err != nil {
		t.Fatal(err)
	}
	if as, err := e.Pull(ctx, "seed", 10); err != nil || len(as) != 0 { // a fully stubbed run ends in one pull
		t.Fatalf("pull: %v, %v", as, err)
	}
	if scoreErrors {
		if err := e.Project(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return &state{t: t, db: unusedDB(), st: st}
}
