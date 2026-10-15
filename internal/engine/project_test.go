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
	v.apply(writePackage(t, projectPipeline, "a"))
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

// Project rolls the run's events up and records its insights.
func TestProject(t *testing.T) {
	t.Parallel()
	v := stubbedEnv(t, Options{})
	id := v.fire("p", "adhoc", "r1")
	v.pull(10)
	if r := v.run("p", id); r.SettledAt.IsZero() {
		t.Fatalf("run %s not settled", r.State)
	}
	if err := v.e.Project(v.ctx); err != nil {
		t.Fatal(err)
	}
	rs, err := v.st.MetricRollups(v.ctx, store.MetricFilter{Pipeline: "p", Name: metrics.ItemsDead})
	if err != nil || len(rs) != 1 || rs[0].Labels["step"] != "sink" || rs[0].Labels["class"] != node.ClassAction {
		t.Fatalf("dead rollups = %+v, %v", rs, err)
	}
	ins := v.insights()
	if len(ins) != 1 || ins[0].Kind != "failureCluster" || ins[0].Scope != "sink" || ins[0].Severity != "error" {
		t.Fatalf("insights = %+v", ins)
	}
}

// On its ticker, a pipeline's insights run only once insights.every has passed or a run settled since.
func TestProjectWhenDue(t *testing.T) {
	t.Parallel()
	v := stubbedEnv(t, Options{})
	if err := v.e.project(v.ctx, false); err != nil { // first pass: always
		t.Fatal(err)
	}
	first := v.e.lastInsights["p"]
	v.clock.Advance(time.Minute)
	if err := v.e.project(v.ctx, false); err != nil {
		t.Fatal(err)
	}
	if !v.e.lastInsights["p"].Equal(first) {
		t.Fatal("insights ran again before insights.every and without a settled run")
	}
	v.fire("p", "adhoc", "r1")
	v.pull(10) // settles at the store clock, after the last pass
	if err := v.e.project(v.ctx, false); err != nil {
		t.Fatal(err)
	}
	if v.e.lastInsights["p"].Equal(first) || len(v.insights()) != 1 {
		t.Fatal("a settled run did not trigger the insights")
	}
	settled := v.e.lastInsights["p"]
	v.clock.Advance(16 * time.Minute)
	if err := v.e.project(v.ctx, false); err != nil {
		t.Fatal(err)
	}
	if v.e.lastInsights["p"].Equal(settled) {
		t.Fatal("insights did not run after insights.every")
	}
}

// Run runs the projectors on their ticker and stops them before it returns.
func TestRunProjects(t *testing.T) {
	t.Parallel()
	v := stubbedEnv(t, Options{ProjectInterval: 10 * time.Millisecond})
	v.fire("p", "adhoc", "r1")
	v.pull(10)
	ctx, cancel := context.WithCancel(v.ctx)
	done := make(chan error, 1)
	go func() { done <- v.e.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for len(v.insights()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the projectors never ran")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// The classes the insights and metrics call unschedulable are the engine's.
func TestUnschedulableClasses(t *testing.T) {
	t.Parallel()
	if len(metrics.UnschedulableClasses) != 2 || !metrics.UnschedulableClasses[ClassUnsupported] ||
		!metrics.UnschedulableClasses[ClassSandbox] {
		t.Fatalf("UnschedulableClasses = %v", metrics.UnschedulableClasses)
	}
}
