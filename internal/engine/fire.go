package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// ErrFiringConfig is returned, wrapped, by Fire when the firing cannot happen as configured: a variable or secret
// that does not resolve, or a trigger the pipeline does not have. Firing again changes nothing until the
// configuration does, unlike a store error, which the scheduler retries.
var ErrFiringConfig = errors.New("firing configuration")

// Fire fires a trigger of the pipeline's active revision by hand under key, the request id of the firing, generated
// when key is empty. The run id is a function of the key and the trigger, so firing the same key again returns the
// existing run with created false. A cron trigger may be fired this way too: the run is recorded as manual and does
// not move where catch-up resumes. The payload must match the trigger's input schema, and the key be a request id
// (requestKey). The run's variables are resolved now, from Options.Flags, the environment and
// the defaults, and stored on the run with their sources; the pipeline's secrets must resolve too, or nothing is
// fired (spec section 5.5, config errors). Secrets themselves are never stored: they are read again at each
// dispatch.
func (e *Engine) Fire(ctx context.Context, pipelineName, trigger, key string, payload json.RawMessage) (string, bool,
	error,
) {
	return e.fireRequest(ctx, store.SourceManual, pipelineName, trigger, key, payload)
}

// FireCron fires a cron trigger for its scheduled time, with the cron run input {trigger, scheduledAt}, under the
// run id ids.CronRun(at, trigger).
func (e *Engine) FireCron(ctx context.Context, pipelineName, trigger string, at time.Time) (string, bool, error) {
	payload, err := json.Marshal(map[string]any{"trigger": trigger, "scheduledAt": at.UTC().Format(time.RFC3339)})
	if err != nil {
		return "", false, fmt.Errorf("fire %s.%s: %w", pipelineName, trigger, err)
	}
	return e.fire(ctx, store.Firing{
		Pipeline: pipelineName, Trigger: trigger, Key: ids.CronKey(at), RunID: ids.CronRun(at, trigger),
		Source: store.SourceCron, ScheduledAt: at, Payload: payload,
	})
}

// fire resolves the run's variables against the active revision and applies the Trigger unit.
func (e *Engine) fire(ctx context.Context, f store.Firing) (string, bool, error) {
	fail := func(err error) (string, bool, error) {
		return "", false, fmt.Errorf("fire %s.%s: %w", f.Pipeline, f.Trigger, err)
	}
	revID, err := e.st.ActiveRevision(ctx, f.Pipeline)
	if err != nil {
		return fail(err)
	}
	rev, err := e.revision(ctx, revID)
	if err != nil {
		return fail(err)
	}
	t, ok := rev.p.Triggers[f.Trigger]
	if !ok {
		return fail(fmt.Errorf("%w: pipeline %s has no trigger %q: %w", ErrFiringConfig, f.Pipeline, f.Trigger,
			store.ErrInvalid))
	}
	if f.Source == store.SourceWebhook {
		if t.Webhook == nil {
			return fail(fmt.Errorf("trigger %s is not a webhook trigger: %w", f.Trigger, store.ErrNotFound))
		}
		if err := e.takeWebhook(f.Pipeline, f.Trigger, t.Webhook.Rate); err != nil {
			return "", false, err
		}
	}
	if f.Source != store.SourceCron { // a request's input must fit the trigger's schema
		if err := e.checkPayload(ctx, rev, f.Trigger, f.Payload); err != nil {
			return fail(err)
		}
	}
	res, err := pipeline.ResolveVarsWith(rev.p, e.declaredFlags(rev.p), e.opts.Env, e.secrets(rev.p.Name))
	if err != nil {
		return fail(fmt.Errorf("%w: %w", ErrFiringConfig, err))
	}
	f.Revision, f.Vars, f.VarSources = revID, res.Vars, res.Sources
	created, err := e.st.Fire(ctx, f)
	if err != nil {
		return fail(err)
	}
	e.log.Info("trigger fired", "pipeline", f.Pipeline, "runId", f.RunID, "trigger", f.Trigger,
		"source", string(f.Source), "created", created)
	return f.RunID, created, nil
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
