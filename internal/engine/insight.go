package engine

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/insights"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Insight triggers (spec sections 4.6 and 11.1): each insight of another pipeline that opens, reopens or escalates
// at or above a trigger's minSeverity fires a run of the trigger's pipeline, its input the insight. The run is keyed
// by the insight and the event that opened or escalated it, so a pass that reads an event again, after a restart
// or on a standby taking over, finds the run it fired instead of firing another.

// insightTrigger is an insight trigger of a pipeline's active revision.
type insightTrigger struct {
	pipeline, name string
	spec           *pipeline.InsightSpec
}

// fireInsightTriggers fires the insight triggers of the pipelines for the insight events recorded since the last
// pass, or in the last insights window on the engine's first pass; none on an engine that fires only on request
// (NoCron). The caller holds projMu.
func (e *Engine) fireInsightTriggers(ctx context.Context, pipelines []store.PipelineState) error {
	if e.opts.NoCron {
		return nil
	}
	var triggers []insightTrigger
	for _, ps := range pipelines {
		if ps.Disabled {
			continue
		}
		rev, err := e.revision(ctx, ps.ActiveRevision)
		if err != nil {
			return fmt.Errorf("insight triggers of %s: %w", ps.Name, err)
		}
		for _, name := range slices.Sorted(maps.Keys(rev.p.Triggers)) {
			if spec := rev.p.Triggers[name].Insight; spec != nil {
				triggers = append(triggers, insightTrigger{pipeline: ps.Name, name: name, spec: spec})
			}
		}
	}
	if len(triggers) == 0 {
		return nil
	}
	since := e.insightTS // the last event read: later passes read only what came since
	if since.IsZero() {
		since = e.opts.Now().Add(-insights.Window)
	}
	evs, err := e.st.PipelineEvents(ctx, store.EventFilter{
		Kinds: []string{store.EventInsightOpened, store.EventInsightEscalated}, Since: since,
	})
	if err != nil {
		return fmt.Errorf("insight triggers: %w", err)
	}
	found := map[string]map[string]store.Insight{} // by pipeline and id, read once per pass
	for _, ev := range evs {
		if ev.Seq <= e.insightSeq {
			continue
		}
		var data struct {
			InsightID string `json:"insightId"`
			Kind      string `json:"kind"`
			Severity  string `json:"severity"`
		}
		if err := json.Unmarshal(ev.Data, &data); err != nil {
			return fmt.Errorf("insight triggers: event %d: %w", ev.Seq, err)
		}
		for _, t := range triggers {
			if t.pipeline == ev.Pipeline || !e.insightMatches(t.spec, data.Kind, data.Severity) {
				continue
			}
			if found[ev.Pipeline] == nil {
				if found[ev.Pipeline], err = e.insightsOf(ctx, ev.Pipeline); err != nil {
					return err
				}
			}
			in, ok := found[ev.Pipeline][data.InsightID]
			if !ok || in.State == store.InsightMuted {
				continue
			}
			if err := e.fireInsight(ctx, t, ev.Seq, in); err != nil {
				return err
			}
		}
		e.insightSeq, e.insightTS = ev.Seq, ev.TS
	}
	return nil
}

// insightMatches reports whether an insight of kind at severity fires the trigger: one of its kinds (any without),
// at or above its minSeverity, else improve.minSeverity, else warning.
func (e *Engine) insightMatches(spec *pipeline.InsightSpec, kind, severity string) bool {
	minSeverity := cmp.Or(spec.MinSeverity, e.opts.Improve.MinSeverity, pipeline.SeverityWarning)
	return (len(spec.Kinds) == 0 || slices.Contains(spec.Kinds, kind)) &&
		pipeline.SeverityRank(severity) >= pipeline.SeverityRank(minSeverity)
}

// insightsOf returns a pipeline's insights by id.
func (e *Engine) insightsOf(ctx context.Context, pipelineName string) (map[string]store.Insight, error) {
	ins, err := e.st.Insights(ctx, store.InsightFilter{Pipeline: pipelineName})
	if err != nil {
		return nil, fmt.Errorf("insight triggers: %w", err)
	}
	out := make(map[string]store.Insight, len(ins))
	for _, in := range ins {
		out[in.ID] = in
	}
	return out, nil
}

// fireInsight fires trigger t for insight in, as event seq recorded it: the run input is the insight. A store error
// is returned, and the pass reads the event again next time.
func (e *Engine) fireInsight(ctx context.Context, t insightTrigger, seq int64, in store.Insight) error {
	payload, err := json.Marshal(map[string]any{"pipeline": in.Pipeline, "insightId": in.ID, "kind": in.Kind,
		"scope": in.Scope, "severity": in.Severity, "evidence": in.Evidence})
	if err != nil {
		return fmt.Errorf("insight triggers: %w", err)
	}
	key := ids.InsightKey(in.ID, seq)
	_, _, err = e.fire(ctx, store.Firing{Pipeline: t.pipeline, Trigger: t.name, Key: key,
		RunID: ids.ManualRun(key, t.name), Source: store.SourceInsight, Payload: payload})
	if errors.Is(err, ErrFiringConfig) { // firing again changes nothing: logged, and the next insights go on
		e.log.Error("insight trigger", "pipeline", t.pipeline, "trigger", t.name, "insightId", in.ID,
			"error", err.Error())
		return nil
	}
	return err
}
