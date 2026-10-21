package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/insights"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// projectLoop runs the projectors every ProjectInterval until ctx is cancelled.
func (e *Engine) projectLoop(ctx context.Context) {
	tick := time.NewTicker(e.opts.ProjectInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if err := e.project(ctx, false); err != nil && ctx.Err() == nil {
			e.log.Error("projections", "error", err.Error())
		}
		if e.projected != nil {
			e.projected()
		}
	}
}

// Project runs the projectors now (spec sections 10.2 and 11.1): the metric rollup over every event not yet rolled
// up, then the insights projector of every pipeline whose insights are on, then the insight triggers. Run calls it
// on a ticker, where a pipeline's insights run only when insights.every has passed or one of its runs settled since
// their last pass; "run --once" calls it once its runs settled. A pipeline whose insights fail is logged and the others still run.
func (e *Engine) Project(ctx context.Context) error { return e.project(ctx, true) }

func (e *Engine) project(ctx context.Context, force bool) error {
	e.projMu.Lock()
	defer e.projMu.Unlock()
	for {
		n, err := e.st.Rollup(ctx)
		if err != nil {
			return fmt.Errorf("roll up metrics: %w", err)
		}
		if n < store.RollupBatch {
			break
		}
	}
	pipelines, err := e.st.Pipelines(ctx)
	if err != nil {
		return fmt.Errorf("insights: %w", err)
	}
	pending := map[string][]insights.PendingStep{}
	if un, err := e.Unschedulable(ctx); err != nil {
		e.log.Warn("unschedulable deliveries", "error", err.Error())
	} else {
		for _, u := range un {
			pending[u.Pipeline] = append(pending[u.Pipeline],
				insights.PendingStep{Step: u.Step, Count: u.Pending, Needs: u.Placement.String()})
		}
	}
	var errs []error
	for _, ps := range pipelines {
		if err := e.projectInsights(ctx, ps, force, pending[ps.Name]); err != nil {
			errs = append(errs, err)
		}
	}
	if err := e.fireInsightTriggers(ctx, pipelines); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// projectInsights runs a pipeline's insights projector when it is due, or always with force, with its steps no node
// can take now. The caller holds projMu.
func (e *Engine) projectInsights(ctx context.Context, ps store.PipelineState, force bool,
	pending []insights.PendingStep,
) error {
	rev, err := e.revision(ctx, ps.ActiveRevision)
	if err != nil {
		return fmt.Errorf("insights of %s: %w", ps.Name, err)
	}
	cfg, err := rev.p.InsightsConfig()
	if err != nil {
		return fmt.Errorf("insights of %s: %w", ps.Name, err)
	}
	now := e.opts.Now()
	last, ran := e.lastInsights[ps.Name]
	if !force && ran && now.Sub(last) < cfg.Every {
		settled, err := e.settledSince(ctx, ps.Name, last)
		if err != nil || !settled {
			return err
		}
	}
	steps := insights.StepsOf(rev.p, func(ref string) *pipeline.Action { return e.action(rev, ref).a })
	if err := insights.Project(ctx, e.st, rev.p, now, insights.Config{Steps: steps, Pending: pending,
		DistillMinDeliveries: e.opts.Improve.Distill.MinDeliveries,
		FilterMaxFalseDrop:   e.opts.Improve.Filter.MaxFalseDrop}); err != nil {
		return err
	}
	e.lastInsights[ps.Name] = now
	return nil
}

// settledSince reports whether one of the pipeline's recent runs settled after t.
func (e *Engine) settledSince(ctx context.Context, pipelineName string, t time.Time) (bool, error) {
	runs, err := e.st.Runs(ctx, store.RunFilter{Pipeline: pipelineName, Limit: 20})
	if err != nil {
		return false, fmt.Errorf("insights of %s: %w", pipelineName, err)
	}
	for _, r := range runs {
		if r.SettledAt.After(t) {
			return true, nil
		}
	}
	return false, nil
}
