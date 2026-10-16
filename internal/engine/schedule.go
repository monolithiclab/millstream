package engine

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/pipeline"
)

// missedAfter is how late a scheduled time may be handled and still count as on time. An older one was missed
// while no engine ran (or the engine stalled) and follows the trigger's catchup policy.
const missedAfter = time.Minute

// maxDue bounds the missed times kept per trigger and tick, so a long outage of a frequent cron cannot fire or skip
// without end; the oldest beyond the bound are dropped.
const maxDue = 10000

// cronState is the scheduler's view of one cron trigger.
type cronState struct {
	spec  pipeline.CronSpec // the definition the state was built for; a new revision changing it rebuilds it
	sched cron.Schedule     // nil when the definition does not parse
	last  time.Time         // the last scheduled time handled
}

// schedule fires every cron trigger due by the engine's clock, across the active revision of every pipeline.
// Firing ignores pause state: a paused pipeline or step keeps its deliveries pending (spec section 5.3). A disabled
// pipeline's triggers do not fire: their times pass as skipped, so resuming it does not catch them up.
func (e *Engine) schedule(ctx context.Context) {
	pipelines, err := e.st.Pipelines(ctx)
	if err != nil {
		if ctx.Err() == nil {
			e.log.Error("scheduler: list pipelines", "error", err.Error())
		}
		return
	}
	now := e.opts.Now()
	for _, ps := range pipelines {
		rev, err := e.revision(ctx, ps.ActiveRevision)
		if err != nil {
			e.log.Error("scheduler: read revision", "pipeline", ps.Name, "error", err.Error())
			continue
		}
		for _, name := range slices.Sorted(maps.Keys(rev.p.Triggers)) {
			if spec := rev.p.Triggers[name].Cron; spec != nil {
				e.scheduleTrigger(ctx, rev, name, *spec, now, ps.Disabled)
			}
		}
	}
}

func (e *Engine) scheduleTrigger(ctx context.Context, rev *revision, name string, spec pipeline.CronSpec,
	now time.Time, disabled bool,
) {
	log := e.log.With("pipeline", rev.p.Name, "trigger", name)
	k := [2]string{rev.p.Name, name}
	st := e.crons[k]
	if st == nil || st.spec != spec {
		var err error
		st, err = e.newCronState(ctx, rev, name, spec)
		if st != nil {
			e.crons[k] = st
		}
		if err != nil {
			log.Error("scheduler: cron trigger", "error", err.Error())
			return
		}
	}
	if st.sched == nil {
		return
	}
	var missed, onTime []time.Time
	dropped := 0
	for t := st.sched.Next(st.last); !t.After(now); t = st.sched.Next(t) {
		if now.Sub(t) <= missedAfter {
			onTime = append(onTime, t)
			continue
		}
		if len(missed) == e.maxDue {
			missed, dropped = missed[1:], dropped+1
		}
		missed = append(missed, t)
	}
	if dropped > 0 {
		log.Warn("scheduler: too many missed cron firings, oldest dropped", "dropped", dropped)
	}
	due := slices.Concat(missed, onTime) // ascending
	if len(due) == 0 {
		return
	}
	if disabled {
		if err := e.st.SkipFirings(ctx, rev.p.Name, name, due); err != nil {
			log.Error("scheduler: record skipped firings", "error", err.Error())
			return
		}
		log.Info("cron firings skipped: the pipeline is disabled", "count", len(due))
		st.last = due[len(due)-1]
		return
	}
	fire := onTime
	switch spec.Catchup {
	case pipeline.CatchupAll:
		fire = due
	case pipeline.CatchupNone:
		if len(missed) > 0 {
			if err := e.st.SkipFirings(ctx, rev.p.Name, name, missed); err != nil {
				log.Error("scheduler: record skipped firings", "error", err.Error())
				return // retried at the next tick
			}
			log.Info("missed cron firings skipped", "count", len(missed))
		}
	default: // latest: the most recent time, on time or else missed, fires once
		if len(onTime) == 0 {
			fire = missed[len(missed)-1:]
		}
	}
	for _, t := range fire {
		if _, _, err := e.FireCron(ctx, rev.p.Name, name, t); err != nil {
			log.Error("scheduler: fire", "scheduledAt", ids.CronKey(t), "error", err.Error())
			if !errors.Is(err, ErrFiringConfig) {
				// A store error: the time is not handled, and the next tick fires it again. Firings already made
				// by this tick are no-ops then, since run ids are deterministic.
				return
			}
			// A configuration error is not retried every tick: the time is handled.
		}
	}
	st.last = due[len(due)-1]
}

// newCronState builds the state of a cron trigger. Catch-up starts after the last scheduled time the trigger fired
// a cron run for or skipped, or after the revision was applied when it never did; manual firings do not count.
func (e *Engine) newCronState(ctx context.Context, rev *revision, name string, spec pipeline.CronSpec) (*cronState,
	error,
) {
	st := &cronState{spec: spec}
	src := spec.Expr
	if spec.TZ != "" {
		src = "CRON_TZ=" + spec.TZ + " " + src
	}
	sched, err := e.parser.Parse(src)
	if err != nil {
		return st, fmt.Errorf("parse %q: %w", src, err) // kept without a schedule: the error is logged once
	}
	st.sched = sched
	last, err := e.st.LastFiring(ctx, rev.p.Name, name)
	if err != nil {
		return nil, err
	}
	st.last = rev.appliedAt
	if !last.IsZero() {
		st.last = last
	}
	return st, nil
}
