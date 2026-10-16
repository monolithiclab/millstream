//go:build e2e

package insights

import (
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Variants the e2e build adds: the default build covers the same paths through the other tests.

// The projector records what it finds, updates it on the next pass, and resolves what clears: a day later, after
// a clean quick run, the failures and the unschedulable step are gone from the window.
func TestProjectLifecycle(t *testing.T) {
	s := fixture(t)
	if err := Project(s.ctx, s.st, s.p, now, nil); err != nil {
		t.Fatal(err)
	}
	if err := Project(s.ctx, s.st, s.p, now.Add(time.Minute), nil); err != nil { // same findings: same rows
		t.Fatal(err)
	}
	ins, err := s.st.Insights(s.ctx, store.InsightFilter{Pipeline: "shop", State: store.InsightOpen})
	if err != nil || len(ins) != 6 {
		t.Fatalf("open insights = %d, %v; want 6", len(ins), err)
	}
	s.quickRun("later", now.Add(26*time.Hour))
	if err := Project(s.ctx, s.st, s.p, now.Add(27*time.Hour), nil); err != nil {
		t.Fatal(err)
	}
	states := map[string]store.InsightState{}
	all, err := s.st.Insights(s.ctx, store.InsightFilter{Pipeline: "shop"})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range all {
		states[in.Kind+"/"+in.Scope+"/"+string(in.State)] = in.State
	}
	for _, want := range []string{"failureCluster/score/resolved", "unschedulable/publish/resolved",
		"slowStep/score/resolved", "cost/score/open"} {
		if _, ok := states[want]; !ok {
			t.Errorf("no %s insight; have %v", want, states)
		}
	}
}
