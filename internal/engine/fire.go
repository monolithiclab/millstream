package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Fire fires a trigger of the pipeline's active revision under key: the scheduled time of a cron firing
// (ids.CronKey) or the request id of a manual one, generated when key is empty. The run id is a function of the
// key and the trigger, so firing the same key again returns the existing run with created false. The run's
// variables are resolved now, from Options.Flags, the environment and the defaults, and stored on the run with
// their sources; the pipeline's secrets must resolve too, or nothing is fired (spec section 5.5, config errors).
// Secrets themselves are never stored: they are read again at each dispatch.
func (e *Engine) Fire(ctx context.Context, pipelineName, trigger, key string, payload json.RawMessage) (string, bool,
	error,
) {
	revID, err := e.st.ActiveRevision(ctx, pipelineName)
	if err != nil {
		return "", false, fmt.Errorf("fire %s.%s: %w", pipelineName, trigger, err)
	}
	rev, err := e.revision(ctx, revID)
	if err != nil {
		return "", false, fmt.Errorf("fire %s.%s: %w", pipelineName, trigger, err)
	}
	if _, ok := rev.p.Triggers[trigger]; !ok {
		return "", false, fmt.Errorf("fire %s.%s: pipeline %s has no trigger %q: %w", pipelineName, trigger,
			pipelineName, trigger, store.ErrInvalid)
	}
	res, err := pipeline.ResolveVars(rev.p, e.declaredFlags(rev.p), e.opts.Env)
	if err != nil {
		return "", false, fmt.Errorf("fire %s.%s: %w", pipelineName, trigger, err)
	}
	if key == "" {
		key = NewRequestID(e.opts.Now())
	}
	runID := ids.ManualRun(key, trigger)
	created, err := e.st.Fire(ctx, store.Firing{
		Pipeline: pipelineName, Trigger: trigger, Key: key, RunID: runID, Revision: revID,
		Vars: res.Vars, VarSources: res.Sources, Payload: payload,
	})
	if err != nil {
		return "", false, fmt.Errorf("fire %s.%s: %w", pipelineName, trigger, err)
	}
	e.log.Info("trigger fired", "pipeline", pipelineName, "runId", runID, "trigger", trigger, "created", created)
	return runID, created, nil
}

// FireCron fires a cron trigger for its scheduled time, with the cron run input {trigger, scheduledAt}, under the
// run id ids.CronRun(at, trigger).
func (e *Engine) FireCron(ctx context.Context, pipelineName, trigger string, at time.Time) (string, bool, error) {
	payload, err := json.Marshal(map[string]any{"trigger": trigger, "scheduledAt": at.UTC().Format(time.RFC3339)})
	if err != nil {
		return "", false, fmt.Errorf("fire %s.%s: %w", pipelineName, trigger, err)
	}
	return e.Fire(ctx, pipelineName, trigger, ids.CronKey(at), payload)
}

// declaredFlags returns the flags naming variables p declares.
func (e *Engine) declaredFlags(p *pipeline.Pipeline) map[string]string {
	out := map[string]string{}
	for name, v := range e.opts.Flags {
		if _, ok := p.Vars[name]; ok {
			out[name] = v
		}
	}
	return out
}

// NewRequestID returns a request id for a manual firing: the UTC time to the second and 8 random hex digits, so
// ids sort by time and do not collide.
func NewRequestID(now time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails
	return now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}
