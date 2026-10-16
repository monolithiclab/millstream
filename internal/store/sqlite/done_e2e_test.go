//go:build e2e

package sqlite

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store"
)

// Done variants the e2e build adds: the default build covers the same paths through the other Done tests.

func TestDoneTerminalStepDropsOutputs(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{"score":1}`))
	e.done(e.leaseStep("audit"), out("", `{"ignored":true}`))
	for _, l := range e.lease(10) {
		e.done(l)
	}
	wantState(t, "run", e.run("r1").State, store.RunSucceeded)
}
