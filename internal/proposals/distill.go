package proposals

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/eval"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// A distill proposal (spec section 11.5) adds a code action under a new actions/<name>/ directory, with the
// distill.Spec file the improve package records there, and makes it the step's action with the agent under
// fallback. The engine runs the code, in the sandbox, on the step's holdout, recomputed from history with the
// boundary the spec names, so the drafter never chooses what it is judged on: it passes when the code agrees with
// the truth on at least improve.distill.minAgreement of the items it covers, covers at least minCoverage of them,
// and at least distill.MinCovered; then the evals decide as for any proposal.

// HoldoutSource reads an agent step's holdout: the pairs of the entities first seen at or after from
// (backtest.Backtester.Holdout).
type HoldoutSource interface {
	Holdout(ctx context.Context, pipelineName, step string, from time.Time) ([]distill.Pair, error)
}

// DistillReport is a distill proposal's measure on its holdout: the step, the code and the agent kept as its
// fallback, the boundary, and the verdict with the agent cost the code saves on what it covers.
type DistillReport struct {
	Step        string    `json:"step"`
	Action      string    `json:"action"`
	Fallback    string    `json:"fallback"`
	HoldoutFrom time.Time `json:"holdoutFrom"`
	distill.Verdict
	// Error is why the code could not be measured; the proposal then fails.
	Error string `json:"error,omitempty"`
}

// distill measures a distill proposal's code on its holdout.
func (v Validator) distill(ctx context.Context, pipelineName string, p Prepared, after side) *DistillReport {
	rep := &DistillReport{Verdict: distill.Verdict{Thresholds: v.Distill.WithDefaults(), Failures: []string{},
		Disagreements: []string{}}}
	fail := func(err error) *DistillReport {
		rep.Error, rep.Passed, rep.Failures = err.Error(), false, append(rep.Failures, "error")
		return rep
	}
	spec, ref, err := distillSpec(p)
	if err != nil {
		return fail(err)
	}
	rep.Step, rep.Action, rep.Fallback, rep.HoldoutFrom = spec.Step, ref, spec.Agent, spec.HoldoutFrom
	switch {
	case v.Holdout == nil:
		return fail(errors.New("this engine reads no history to validate a distill proposal on"))
	case after.refused != nil:
		return fail(after.refused)
	}
	pairs, err := v.Holdout.Holdout(ctx, pipelineName, spec.Step, spec.HoldoutFrom)
	if err != nil {
		return fail(fmt.Errorf("the holdout of %s: %w", spec.Step, err))
	}
	act, err := after.resolver.Resolve(after.pkg, ref)
	if err != nil {
		return fail(fmt.Errorf("resolve %s: %w", ref, err))
	}
	results, err := v.runHoldout(ctx, pipelineName, after, act, ref, pairs)
	if err != nil {
		return fail(err)
	}
	rep.Verdict = distill.Evaluate(results, v.Distill)
	return rep
}

// distillSpec reads the spec of the action a distill patch adds, and checks the patched step: the new action is
// its action, and the agent it ran on the base revision its fallback on abstain.
func distillSpec(p Prepared) (distill.Spec, string, error) {
	dir := ""
	for _, c := range p.Changes {
		if d, ok := actionDir(c.Path); ok && c.Op == patch.OpCreate {
			dir = d
		}
	}
	if dir == "" {
		return distill.Spec{}, "", errors.New("the patch adds no action directory")
	}
	ref := "./actions/" + dir
	file := "actions/" + dir + "/" + distill.SpecFile
	data, ok := p.Patched.Files[file]
	if !ok {
		return distill.Spec{}, ref, fmt.Errorf("%s is missing: a distill proposal records its step and holdout there",
			file)
	}
	spec, err := distill.ParseSpec(data)
	if err != nil {
		return spec, ref, fmt.Errorf("%s: %w", file, err)
	}
	before, okBefore := p.Base.Pipeline.Steps[spec.Step]
	now, okNow := p.Patched.Pipeline.Steps[spec.Step]
	switch {
	case !okBefore || !okNow:
		return spec, ref, fmt.Errorf("%s names step %s, which the pipeline lacks", file, spec.Step)
	case before.Action != spec.Agent:
		return spec, ref, fmt.Errorf("step %s runs %s, not the agent %s names", spec.Step, before.Action, file)
	case now.Action != ref:
		return spec, ref, fmt.Errorf("the patched step %s runs %s, not the new action %s", spec.Step, now.Action, ref)
	case now.Fallback == nil || now.Fallback.Action != spec.Agent || !slices.Contains(now.Fallback.When, "abstain"):
		return spec, ref, fmt.Errorf("the patched step %s must keep %s as its fallback on abstain", spec.Step,
			spec.Agent)
	}
	return spec, ref, nil
}

// runHoldout runs the code on each holdout pair's input, in one process (restarted after a failure), as the step's
// delivery of the pair's item would: an abstention leaves the item uncovered, an error is a covered item it got
// wrong, and outputs agree when they are the truth's.
func (v Validator) runHoldout(ctx context.Context, pipelineName string, s side, act *pipeline.Action, ref string,
	pairs []distill.Pair,
) ([]distill.Result, error) {
	env := func(string) (string, bool) { return "", false }
	if v.Evals.Env != nil {
		env = v.Evals.Env(pipelineName)
	}
	spec := eval.ProcessSpec(eval.Options{Package: s.pkg, Action: act, Ref: ref, Env: env}, map[string]any{},
		map[string]any{})
	opts := exec.Options{Logger: v.Evals.Logger, Launchers: s.launchers}
	var proc *exec.Process
	defer func() {
		if proc != nil {
			_ = proc.Close()
		}
	}()
	run := protocol.Run{ID: "holdout", Trigger: "holdout", StartedAt: time.Now().UTC(), Revision: s.pkg.Hash}
	out := make([]distill.Result, 0, len(pairs))
	for i, pr := range pairs {
		if proc == nil {
			var err error
			if proc, err = exec.Start(ctx, spec, opts); err != nil {
				return nil, fmt.Errorf("start %s: %w", ref, err)
			}
		}
		item := protocol.Item{Delivery: fmt.Sprintf("holdout-%d", i), ID: pr.Item, Key: pr.Key, Payload: pr.Input,
			Artifacts: []protocol.Artifact{}, Run: run, Attempt: 1, IdempotencyKey: "holdout-" + pr.Item}
		res, err := proc.Deliver(ctx, exec.Delivery{Item: item, Timeout: pipeline.DefaultTimeout})
		r := distill.Result{Item: pr.Item, CostUSD: pr.CostUSD}
		switch {
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case err != nil:
			r.Error = err.Error()
			_ = proc.Close()
			proc = nil
		case res.Err != nil && res.Err.Abstain:
			r.Abstained = true
		case res.Err != nil:
			r.Error = res.Err.Message
		default:
			got := make([]distill.Output, len(res.Outputs))
			for j, o := range res.Outputs {
				got[j] = distill.Output{Key: o.Key, Payload: o.Payload}
			}
			r.Agreed = distill.Agree(pr.Outputs, got, pr.Key)
		}
		out = append(out, r)
	}
	return out, nil
}
