package engine

import (
	"context"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/metrics"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

const projectPipeline = `pipeline: p
triggers:
  adhoc: { manual: {} }
steps:
  src: { action: ./actions/a, in: trigger.adhoc, out: raw }
  sink: { action: ./actions/a, in: raw }
`

// stubbedEnv runs projectPipeline with both steps stubbed: src emits one item, sink fails for good.
func stubbedEnv(t *testing.T, opts Options) *env {
	t.Helper()
	opts.Stubs = map[store.StepRef]Stub{
		{Pipeline: "p", Step: "src"}:  {Outputs: []store.Output{output("a", `{"n":1}`)}},
		{Pipeline: "p", Step: "sink"}: {Err: &exec.ActionError{Message: "upstream refused item 42"}},
	}
	v := newEnv(t, opts)
	v.apply(newPackage(t, projectPipeline, "a"))
	return v
}

func (v *env) insights() []store.Insight {
	v.t.Helper()
	ins, err := v.st.Insights(v.ctx, store.InsightFilter{Pipeline: "p"})
	if err != nil {
		v.t.Fatal(err)
	}
	return ins
}

// On its ticker, a pipeline's insights run only once insights.every has passed or a run settled since; Project runs
// them whatever. A pass rolls the run's events up and records its insights.
func TestProject(t *testing.T) {
	t.Parallel()
	v := stubbedEnv(t, Options{})
	pass := func(what string, force, wantRan bool) {
		t.Helper()
		before, ran := v.e.lastInsights["p"]
		project := func(ctx context.Context) error { return v.e.project(ctx, false) }
		if force {
			project = v.e.Project
		}
		if err := project(v.ctx); err != nil {
			t.Fatal(err)
		}
		if got := !ran || !v.e.lastInsights["p"].Equal(before); got != wantRan {
			t.Fatalf("%s: insights ran %v, want %v", what, got, wantRan)
		}
	}
	pass("first pass", false, true)
	v.clock.Advance(time.Minute)
	pass("before insights.every, no run settled", false, false)
	id := v.fire("p", "adhoc", "r1")
	v.pull(10) // settles at the store clock, after the last pass
	if r := v.run("p", id); r.SettledAt.IsZero() {
		t.Fatalf("run %s not settled", r.State)
	}
	pass("a run settled", false, true)
	rs, err := v.st.MetricRollups(v.ctx, store.MetricFilter{Pipeline: "p", Name: metrics.ItemsDead})
	if err != nil || len(rs) != 1 || rs[0].Labels["step"] != "sink" || rs[0].Labels["class"] != node.ClassAction {
		t.Fatalf("dead rollups = %+v, %v", rs, err)
	}
	ins := v.insights()
	if len(ins) != 1 || ins[0].Kind != "failureCluster" || ins[0].Scope != "sink" || ins[0].Severity != "error" {
		t.Fatalf("insights = %+v", ins)
	}
	v.clock.Advance(time.Minute)
	pass("Project", true, true)
	v.clock.Advance(16 * time.Minute)
	pass("after insights.every", false, true)
}

// The classes the insights and metrics call unschedulable are the engine's.
func TestUnschedulableClasses(t *testing.T) {
	t.Parallel()
	if len(metrics.UnschedulableClasses) != 2 || !metrics.UnschedulableClasses[ClassUnsupported] ||
		!metrics.UnschedulableClasses[ClassSandbox] {
		t.Fatalf("UnschedulableClasses = %v", metrics.UnschedulableClasses)
	}
}
