package engine

import (
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// fairnessPipelines are the pipelines of the fairness test: b's backlog is larger than the others', lim holds at
// most 2 in flight across its steps (limits.concurrency), and rated starts at most one delivery a minute.
var fairnessPipelines = []struct {
	name, steps string
	backlog     int
}{
	{"b", "  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 50 }\n", 7},
	{"c", "  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 50 }\n", 7},
	{"lim", "  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 10 }\n" +
		"  b: { action: ./actions/a, in: trigger.adhoc, concurrency: 10 }\nlimits: { concurrency: 2 }\n", 2},
	{"rated", "  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 10, rate: 1/min }\n", 3},
}

// newFairnessEnv applies the fairness pipelines and fires each one's backlog.
func newFairnessEnv(t *testing.T) *env {
	t.Helper()
	v := newEnv(t, Options{LeaseTTL: time.Hour})
	for _, p := range fairnessPipelines {
		v.apply(newPackage(t, manualPipeline(p.name, p.steps), "a"))
		for i := range p.backlog {
			v.fire(p.name, "adhoc", fmt.Sprintf("r%02d", i))
		}
	}
	return v
}

// pullFrom has node pull one delivery from e and returns it, nil when none is due within the limits.
func pullFrom(t *testing.T, e *Engine, v *env, nodeID string) *store.Lease {
	t.Helper()
	as, err := e.Pull(v.ctx, nodeID, 1)
	if err != nil {
		t.Fatalf("Pull %s: %v", nodeID, err)
	}
	if len(as) == 0 {
		return nil
	}
	return &as[0].Lease
}

// wantRound has four nodes take turns pulling one delivery each, len(want) pulls in all, and checks that the
// pipelines served are exactly want, each once, in any order: one round of the round-robin. It returns the leases.
func wantRound(t *testing.T, e *Engine, v *env, round string, want ...string) []store.Lease {
	t.Helper()
	got := map[string]int{}
	var leases []store.Lease
	for i := range want {
		l := pullFrom(t, e, v, fmt.Sprintf("node-%d", i%4))
		if l == nil {
			t.Fatalf("%s: pull %d leased nothing; served %v, want %v", round, i, got, want)
		}
		got[l.Pipeline]++
		leases = append(leases, *l)
	}
	for _, name := range want {
		if got[name] != 1 {
			t.Fatalf("%s: served %v, want each of %v once", round, got, want)
		}
	}
	return leases
}

// Many nodes pulling one delivery at a time from one engine are served the pipelines in strict rotation, whatever
// their backlogs: a pipeline at its limit or out of rate tokens drops out of the rotation without giving the next
// pipeline an extra turn, and comes back once a slot or a token frees up (spec sections 4.10 and 5.9). An engine
// taking the deployment over rebuilds the limits from the leased deliveries: a pipeline at its limit stays there
// until a lease ends, a rated step starts with no token, and the others are served in rotation.
func TestRoundRobinUnderLimits(t *testing.T) {
	v := newFairnessEnv(t)
	first := wantRound(t, v.e, v, "first round", "b", "c", "lim", "rated")
	// rated spent its token; lim has one slot left.
	wantRound(t, v.e, v, "second round", "b", "c", "lim")
	// lim is at its limit, rated has no token: b and c share the nodes evenly.
	wantRound(t, v.e, v, "at the limit", "b", "c")
	// One of lim's deliveries ends: lim is back in the next round, once.
	var limLease store.Lease
	for _, l := range first {
		if l.Pipeline == "lim" {
			limLease = l
		}
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: limLease.Ref(), Attempt: limLease.Attempt}); err != nil {
		t.Fatal(err)
	}
	wantRound(t, v.e, v, "after lim's done", "b", "c", "lim")
	// A minute later rated has one token again.
	v.clock.Advance(time.Minute)
	wantRound(t, v.e, v, "a minute later", "b", "c", "rated")
	perStep, perPipeline := v.e.countInflight()
	if perPipeline["lim"] != 2 || perPipeline["rated"] != 2 || perStep[store.StepRef{Pipeline: "b", Step: "a"}] != 5 {
		t.Fatalf("in flight %v", perPipeline)
	}

	next, err := New(Options{Store: v.st, Now: v.clock.Now, LeaseTTL: time.Hour, Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range fairnessPipelines {
		if _, err := next.Apply(v.ctx, newPackage(t, manualPipeline(p.name, p.steps), "a")); err != nil {
			t.Fatal(err)
		}
	}
	if err := next.resync(v.ctx); err != nil {
		t.Fatal(err)
	}
	if _, rebuilt := next.countInflight(); !maps.Equal(rebuilt, perPipeline) {
		t.Fatalf("rebuilt in flight %v; want the leases of the previous engine, %v", rebuilt, perPipeline)
	}
	wantRound(t, next, v, "after the takeover", "b", "c")
	v.clock.Advance(time.Minute)
	wantRound(t, next, v, "a token one interval in", "b", "c", "rated")
}
