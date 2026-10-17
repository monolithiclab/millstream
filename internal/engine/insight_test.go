package engine

import (
	"encoding/json"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// fixPipeline reads insights: fix takes failure clusters at improve.minSeverity, quiet slow steps from info.
const fixPipeline = `pipeline: fix
triggers:
  fix: { insight: { kinds: [failureCluster] } }
  quiet: { insight: { minSeverity: info, kinds: [slowStep] } }
steps:
  draft: { action: ./actions/a, in: [trigger.fix, trigger.quiet] }
`

// An insight that opens fires the insight triggers of the other pipelines that take its kind at its severity: one
// run, its input the insight, keyed by the insight and its event, so passes after it, and another engine's first
// pass, fire nothing more.
func TestInsightTriggerFiresARun(t *testing.T) {
	v := stubbedEnv(t, Options{Improve: pipeline.Improve{MinSeverity: pipeline.SeverityError}})
	v.apply(newPackage(t, fixPipeline, "a"))
	v.fire("p", "adhoc", "r1")
	v.pull(10) // sink fails for good: a failureCluster of severity error
	if err := v.e.Project(v.ctx); err != nil {
		t.Fatal(err)
	}
	ins := v.insights()
	if len(ins) != 1 || ins[0].Kind != pipeline.InsightFailureCluster {
		t.Fatalf("insights = %+v", ins)
	}
	opened := v.events(store.EventInsightOpened)
	runs := v.runs("fix")
	want := ids.ManualRun(ids.InsightKey(ins[0].ID, opened[0].Seq), "fix")
	if len(runs) != 1 || runs[0].ID != want || runs[0].TriggerSource != store.SourceInsight {
		t.Fatalf("runs of fix = %+v, want one %s", runs, want)
	}
	items, err := v.st.Items(v.ctx, store.ItemFilter{Pipeline: "fix", RunID: want})
	if err != nil || len(items) != 1 {
		t.Fatalf("trigger items = %+v, %v", items, err)
	}
	var input map[string]any
	if err := json.Unmarshal(items[0].Payload, &input); err != nil {
		t.Fatal(err)
	}
	if input["pipeline"] != "p" || input["insightId"] != ins[0].ID || input["kind"] != "failureCluster" ||
		input["scope"] != "sink" || input["severity"] != "error" || input["evidence"] == nil {
		t.Fatalf("run input = %v", input)
	}

	if err := v.e.Project(v.ctx); err != nil {
		t.Fatal(err)
	}
	other, err := New(Options{Store: v.st, Now: v.clock.Now, Logger: newLogger(v.logs),
		Improve: pipeline.Improve{MinSeverity: pipeline.SeverityError}})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Project(v.ctx); err != nil {
		t.Fatal(err)
	}
	if runs := v.runs("fix"); len(runs) != 1 {
		t.Fatalf("after more passes, runs of fix = %+v", runs)
	}

	// Under a stricter improve.minSeverity than the insight's, nothing fires.
	if v.e.insightMatches(&pipeline.InsightSpec{}, "cost", pipeline.SeverityWarning) {
		t.Error("a warning fired a trigger whose minimum is error")
	}
}
