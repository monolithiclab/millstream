package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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

// insightStore serves the events and insights fireInsightTriggers reads and records the firings, no SQLite.
type insightStore struct {
	store.Store
	evs     []store.Event
	ins     []store.Insight
	firings []store.Firing
}

func (s *insightStore) PipelineEvents(context.Context, store.EventFilter) ([]store.Event, error) {
	return s.evs, nil
}

func (s *insightStore) Insights(context.Context, store.InsightFilter) ([]store.Insight, error) {
	return s.ins, nil
}

func (s *insightStore) ActiveRevision(context.Context, string) (store.RevisionID, error) {
	return 1, nil
}

func (s *insightStore) Fire(_ context.Context, f store.Firing) (bool, error) {
	s.firings = append(s.firings, f)
	return true, nil
}

// A pass fires each matching trigger once per event of another pipeline's insight, skipping muted insights, its
// own pipeline's, and the events an earlier pass read.
func TestFireInsightTriggers(t *testing.T) {
	ev := func(seq int64, pipelineName, id, severity string) store.Event {
		data, _ := json.Marshal(map[string]string{"insightId": id, "kind": "failureCluster", "severity": severity})
		return store.Event{Seq: seq, TS: t0, Pipeline: pipelineName, Kind: store.EventInsightOpened, Data: data}
	}
	st := &insightStore{
		evs: []store.Event{ev(1, "p", "in_a", "error"), ev(2, "p", "in_m", "error"), ev(3, "fix", "in_a", "error"),
			ev(4, "p", "in_a", "info")},
		ins: []store.Insight{{Pipeline: "p", ID: "in_a", Kind: "failureCluster", State: store.InsightOpen},
			{Pipeline: "p", ID: "in_m", Kind: "failureCluster", State: store.InsightMuted}},
	}
	e, err := New(Options{Store: st, Now: func() time.Time { return t0 }})
	if err != nil {
		t.Fatal(err)
	}
	p, err := parsed(fixPipeline)
	if err != nil {
		t.Fatal(err)
	}
	e.revs[1] = &revision{id: 1, p: p}
	pipelines := []store.PipelineState{{Name: "fix", ActiveRevision: 1}, {Name: "off", Disabled: true}}
	for range 2 {
		if err := e.fireInsightTriggers(t.Context(), pipelines); err != nil {
			t.Fatal(err)
		}
	}
	if len(st.firings) != 1 || st.firings[0].Key != "in_a.1" || st.firings[0].Source != store.SourceInsight ||
		e.insightSeq != 4 {
		t.Fatalf("firings = %+v, seq %d", st.firings, e.insightSeq)
	}
}

// An insight fires a trigger of one of its kinds (any without) at or above its minSeverity, else
// improve.minSeverity, else warning.
func TestInsightMatches(t *testing.T) {
	e := &Engine{}
	strict := &Engine{opts: Options{Improve: pipeline.Improve{MinSeverity: pipeline.SeverityError}}}
	for _, tt := range []struct {
		e              *Engine
		spec           pipeline.InsightSpec
		kind, severity string
		want           bool
	}{
		{e, pipeline.InsightSpec{}, "cost", "warning", true},
		{e, pipeline.InsightSpec{}, "cost", "info", false},
		{strict, pipeline.InsightSpec{}, "cost", "warning", false},
		{strict, pipeline.InsightSpec{MinSeverity: "info"}, "cost", "info", true},
		{e, pipeline.InsightSpec{Kinds: []string{"waste"}}, "cost", "error", false},
	} {
		if got := tt.e.insightMatches(&tt.spec, tt.kind, tt.severity); got != tt.want {
			t.Errorf("%+v %s %s = %v", tt.spec, tt.kind, tt.severity, got)
		}
	}
}
