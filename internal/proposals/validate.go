package proposals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/eval"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/sandbox"
	"github.com/monolithiclab/millstream/internal/store"
)

// Validation (spec section 11.3) is the engine's, never an LLM's: check on the patched package, then the evals of
// every affected action before and after, judged by the base revision's evals (a proposal that changes evals has
// its new evals reported separately, so it cannot redefine its own success), then a backtest. A proposal is ready
// when all three pass. A distill proposal is measured on its holdout in the backtest's place (distill.go).

// Backtester runs a backtest of a patch (spec section 11.3, POST /api/v1/backtests): the changed steps replayed on a
// sample of past items in a scratch store, before and after. The engine's implementation adapts the backtest
// package's runner; it says whether the after side holds up and gives its report as JSON.
type Backtester interface {
	Backtest(ctx context.Context, req BacktestRequest) (BacktestResult, error)
}

// BacktestRequest asks for a backtest of a patch on its base revision, of the steps it touches.
type BacktestRequest struct {
	Pipeline string
	// BaseRevision is the package hash the patch applies to.
	BaseRevision string
	Patch        string
	Steps        []string
}

// BacktestResult is a backtest's verdict and its report.
type BacktestResult struct {
	Passed bool
	Report json.RawMessage
}

// Evals runs an action's eval cases as `millstream eval` does (eval.Run): the agent's harness calls in Mode, from
// the recordings under the action's evals/recordings in replay and record modes.
type Evals struct {
	// Mode is the harness mode, agent.ModeReplay when empty.
	Mode string
	// Env looks up PATH, HOME, TMPDIR and the secrets of a pipeline; nil is an empty environment.
	Env func(pipelineName string) func(string) (string, bool)
	// Launchers start exec and container processes by kind (eval.Options); tests run actions in-process.
	Launchers map[string]exec.Launcher
	// Prices are the environment file's model prices.
	Prices map[string]pipeline.Price
	Logger *slog.Logger
	// Sandbox runs the proposed exec actions, those whose directory the patch changes (sandbox.Sandbox); without
	// one, their cases fail rather than run with the node's privileges (spec section 15.3).
	Sandbox sandbox.Builder
}

// side is how one side's actions run: the base revision's as configured, the patched package's with its proposed
// code confined.
type side struct {
	pkg       *pipeline.Package
	resolver  pipeline.ActionResolver
	launchers map[string]exec.Launcher
	// proposed are the patched package's exec action directories the patch changes, and refused why they cannot run.
	proposed []string
	refused  error
}

// Validator validates prepared proposals.
type Validator struct {
	// Check checks a package as apply would, against the rest of the deployment.
	Check func(ctx context.Context, pkg *pipeline.Package) []pipeline.Diagnostic
	// Resolver resolves action references; nil means pipeline.Resolver{}.
	Resolver pipeline.ActionResolver
	Evals    Evals
	// Backtest runs the backtest (backtest.ForProposals); without one, every backtest fails.
	Backtest Backtester
	// Holdout reads the holdout a distill proposal is validated on (the backtester's), and Distill holds the
	// thresholds it must meet there (improve.distill); without a source, every distill proposal fails.
	Holdout HoldoutSource
	Distill distill.Thresholds
	// Scratch is where the packages are written for the evals, each under its content hash as the engine's package
	// directory holds them (and prunes them); empty writes them in a temporary directory removed afterwards.
	Scratch string
}

// Report is a validation report, recorded with the proposal as JSON.
type Report struct {
	Passed       bool   `json:"passed"`
	BaseRevision string `json:"baseRevision"`
	PatchSHA256  string `json:"patchSha256"`
	Check        Check  `json:"check"`
	// Evals compare each affected action before and after on the base revision's cases; NewEvals are the cases a
	// patch adds or changes, run after, reported alone.
	Evals    []EvalReport `json:"evals"`
	NewEvals []EvalReport `json:"newEvals"`
	Backtest Backtest     `json:"backtest"`
	// Distill is a distill proposal's measure on its holdout, which takes the backtest's place (spec section 11.5).
	Distill *DistillReport `json:"distill,omitempty"`
}

// Check is check's verdict on the patched package.
type Check struct {
	Passed      bool         `json:"passed"`
	Diagnostics []Diagnostic `json:"diagnostics"`
}

// Diagnostic is a check finding, as the report spells it.
type Diagnostic struct {
	Severity string `json:"severity"`
	Path     string `json:"path"`
	Message  string `json:"message"`
}

// EvalReport is one action's evals: a step's action before (Action) and after (After, when the step runs another).
type EvalReport struct {
	Step   string     `json:"step"`
	Action string     `json:"action"`
	After  string     `json:"after,omitempty"`
	Passed bool       `json:"passed"`
	Cases  []EvalCase `json:"cases"`
	// Error is why the cases could not run at all.
	Error string `json:"error,omitempty"`
}

// EvalCase is one case's outcome before (nil for a new eval) and after.
type EvalCase struct {
	Case   string     `json:"case"`
	Before *EvalScore `json:"before,omitempty"`
	After  EvalScore  `json:"after"`
}

// EvalScore is a case's score on one side.
type EvalScore struct {
	Passed bool    `json:"passed"`
	Score  float64 `json:"score"`
	Error  string  `json:"error,omitempty"`
	// Fallback says the step's action abstained and its fallback answered the case (spec section 11.5).
	Fallback  bool `json:"fallback,omitempty"`
	abstained bool
}

// Backtest is the backtest's verdict and report, or why it did not run.
type Backtest struct {
	Passed  bool            `json:"passed"`
	Skipped string          `json:"skipped,omitempty"`
	Report  json.RawMessage `json:"report,omitempty"`
}

// Validate validates a prepared proposal of a pipeline. Its error is a failure to run the validation, not a
// proposal that fails it.
func (v Validator) Validate(ctx context.Context, pipelineName string, p Prepared) (Report, error) {
	r := Report{BaseRevision: p.Base.Hash, PatchSHA256: p.SHA256, Evals: []EvalReport{}, NewEvals: []EvalReport{}}
	scratch := v.Scratch
	if scratch == "" {
		dir, err := os.MkdirTemp("", "millstream-proposal-")
		if err != nil {
			return r, err
		}
		defer func() { _ = os.RemoveAll(dir) }()
		scratch = dir
	}
	base, err := pipeline.Materialize(scratch, p.Base.Files)
	if err != nil {
		return r, err
	}
	patched, err := pipeline.Materialize(scratch, p.Patched.Files)
	if err != nil {
		return r, err
	}
	diags := v.Check(ctx, patched)
	r.Check = Check{Passed: !pipeline.HasErrors(diags), Diagnostics: make([]Diagnostic, len(diags))}
	for i, d := range diags {
		r.Check.Diagnostics[i] = Diagnostic{Severity: d.Severity, Path: d.Path, Message: d.Message}
	}
	if !r.Check.Passed {
		r.Backtest.Skipped = "check failed"
		return r, nil
	}
	after, cleanup := v.patchedSide(ctx, patched, p.Changes)
	defer cleanup()
	before := side{pkg: base, resolver: v.resolver(), launchers: v.Evals.Launchers}
	r.Evals, r.NewEvals = v.evals(ctx, pipelineName, before, after, p.Changes)
	evalsPassed := !slices.ContainsFunc(r.Evals, func(e EvalReport) bool { return !e.Passed })
	// The touched steps the patched pipeline still has: a removed step has nothing to replay.
	if p.Kind == store.ProposalDistill {
		r.Distill = v.distill(ctx, pipelineName, p, after)
		r.Backtest.Skipped = "a distill proposal is judged on its holdout instead (spec section 11.5)"
		r.Passed = evalsPassed && r.Distill.Passed
		return r, nil
	}
	steps := slices.DeleteFunc(slices.Clone(p.Steps), func(s string) bool {
		_, ok := p.Patched.Pipeline.Steps[s]
		return !ok
	})
	res, err := v.backtest(ctx, BacktestRequest{Pipeline: pipelineName, BaseRevision: p.Base.Hash, Patch: p.Patch,
		Steps: steps})
	if err != nil {
		r.Backtest.Skipped = "the backtest failed: " + err.Error()
	} else {
		r.Backtest = Backtest{Passed: res.Passed, Report: res.Report}
	}
	r.Passed = evalsPassed && r.Backtest.Passed
	return r, nil
}

// backtest runs the backtest; an engine without a runner fails it.
func (v Validator) backtest(ctx context.Context, req BacktestRequest) (BacktestResult, error) {
	if v.Backtest == nil {
		return BacktestResult{}, errors.New("this engine has no backtest runner")
	}
	return v.Backtest.Backtest(ctx, req)
}

func (v Validator) resolver() pipeline.ActionResolver {
	if v.Resolver == nil {
		return pipeline.Resolver{}
	}
	return v.Resolver
}

// patchedSide is how the patched package's actions run: the exec actions whose directory the patch changes in the
// sandbox, refused without one; the container actions there without the network.
func (v Validator) patchedSide(ctx context.Context, patched *pipeline.Package, changes []patch.File) (side, func()) {
	all, execs := sandbox.Proposed(patched, v.resolver(), (&patch.Patch{Files: changes}).Paths())
	s := side{pkg: patched, resolver: sandbox.Confine(v.resolver(), all), launchers: v.Evals.Launchers, proposed: execs}
	switch {
	case len(s.proposed) == 0:
		return s, func() {}
	case v.Evals.Sandbox == nil:
		s.refused = errors.New("proposed exec code runs only in the container sandbox, and this deployment has no " +
			"container runtime")
		return s, func() {}
	}
	launchers, cleanup, err := v.Evals.Sandbox.Launchers(ctx, patched, s.proposed, v.Evals.Launchers)
	if err != nil {
		s.refused = err
		return s, func() {}
	}
	s.launchers = launchers
	return s, cleanup
}

// evals runs the base evals of each affected step's action, before on the base package and after on the patched
// one, and the evals the patch adds or changes, after alone. A step is affected when its action changes, or the
// patch changes its action's directory.
func (v Validator) evals(ctx context.Context, pipelineName string, before, after side, changes []patch.File,
) (affected, added []EvalReport) {
	base, patched := before.pkg, after.pkg
	changedDirs, evalDirs := map[string]bool{}, map[string]bool{}
	for _, c := range changes {
		if dir, ok := actionDir(c.Path); ok {
			changedDirs[dir] = true
			if strings.HasPrefix(c.Path, "actions/"+dir+"/"+eval.Dir+"/") && c.Op != patch.OpDelete {
				evalDirs[dir] = true
			}
		}
	}
	affected, added = []EvalReport{}, []EvalReport{}
	done := map[[2]string]bool{}
	for _, name := range slices.Sorted(maps.Keys(base.Pipeline.Steps)) {
		afterStep, ok := patched.Pipeline.Steps[name]
		beforeStep := base.Pipeline.Steps[name]
		dir, local := localDir(beforeStep.Action)
		if !ok || !local || (beforeStep.Action == afterStep.Action && !changedDirs[dir]) {
			continue
		}
		pair := [2]string{beforeStep.Action, afterStep.Action}
		if done[pair] {
			continue
		}
		done[pair] = true
		affected = append(affected, v.compare(ctx, pipelineName, name, before, after, beforeStep.Action, afterStep.Action))
	}
	for _, dir := range slices.Sorted(maps.Keys(evalDirs)) {
		ref := "./actions/" + dir
		rep := EvalReport{Action: ref, Cases: []EvalCase{}, Passed: true}
		cases, act, err := v.cases(after, ref)
		if err == nil {
			for _, c := range cases {
				score := v.run(ctx, pipelineName, after, ref, act, c)
				rep.Cases = append(rep.Cases, EvalCase{Case: c.Name, After: score})
				rep.Passed = rep.Passed && score.Passed
			}
		} else {
			rep.Passed, rep.Error = false, err.Error()
		}
		added = append(added, rep)
	}
	return affected, added
}

// compare runs the base action's cases on the base action and on the step's action after the patch: it passes when
// no case that passed before fails after.
func (v Validator) compare(ctx context.Context, pipelineName, step string, base, patched side,
	beforeRef, afterRef string,
) EvalReport {
	rep := EvalReport{Step: step, Action: beforeRef, Cases: []EvalCase{}, Passed: true}
	if afterRef != beforeRef {
		rep.After = afterRef
	}
	fail := func(err error) EvalReport {
		rep.Passed, rep.Error = false, err.Error()
		return rep
	}
	cases, beforeAct, err := v.cases(base, beforeRef)
	if err != nil {
		return fail(err)
	}
	afterAct, err := patched.resolver.Resolve(patched.pkg, afterRef)
	if err != nil {
		return fail(fmt.Errorf("resolve %s after the patch: %w", afterRef, err))
	}
	fallback, fallbackRef := v.abstainFallback(patched, step)
	for _, c := range cases {
		before := v.run(ctx, pipelineName, base, beforeRef, beforeAct, c)
		after := v.run(ctx, pipelineName, patched, afterRef, afterAct, c)
		if after.abstained && fallback != nil {
			after = v.run(ctx, pipelineName, patched, fallbackRef, fallback, c)
			after.Fallback = true
		}
		rep.Cases = append(rep.Cases, EvalCase{Case: c.Name, Before: &before, After: after})
		if before.Passed && !after.Passed {
			rep.Passed = false
		}
	}
	return rep
}

// cases loads the eval cases of a local action of pkg, with the action resolved.
func (v Validator) cases(s side, ref string) ([]*eval.Case, *pipeline.Action, error) {
	act, err := s.resolver.Resolve(s.pkg, ref)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve %s: %w", ref, err)
	}
	cases, err := eval.Load(filepath.Join(s.pkg.Dir, filepath.FromSlash(act.Dir)))
	if err != nil {
		return nil, nil, fmt.Errorf("the evals of %s: %w", ref, err)
	}
	return cases, act, nil
}

// run runs one case on an action of a side; proposed exec code that cannot run in the sandbox fails it unrun.
func (v Validator) run(ctx context.Context, pipelineName string, s side, ref string, act *pipeline.Action,
	c *eval.Case,
) EvalScore {
	if s.refused != nil && slices.Contains(s.proposed, strings.TrimSuffix(act.Dir, "/")+"/") {
		return EvalScore{Error: s.refused.Error()}
	}
	pkg, e := s.pkg, v.Evals
	opts := eval.Options{Package: pkg, Action: act, Ref: ref, Mode: e.Mode, Launchers: s.launchers,
		Logger: e.Logger, Env: func(string) (string, bool) { return "", false }}
	if opts.Mode == "" {
		opts.Mode = agent.ModeReplay
	}
	if opts.Mode != agent.ModeLive {
		opts.Dir = filepath.Join(pkg.Dir, filepath.FromSlash(act.Dir), filepath.FromSlash(eval.RecordingsDir))
	}
	if e.Env != nil {
		opts.Env = e.Env(pipelineName)
	}
	if act.Agent != nil {
		if price, ok := e.Prices[act.Agent.Model]; ok {
			opts.Price = &price
		}
	}
	res := eval.Run(ctx, opts, c)
	return EvalScore{Passed: res.Passes(), Score: res.Score, Error: res.Err, abstained: res.Abstained}
}

// abstainFallback resolves the fallback a step of a side hands its abstentions to, nil without one.
func (v Validator) abstainFallback(s side, step string) (*pipeline.Action, string) {
	st, ok := s.pkg.Pipeline.Steps[step]
	if !ok || st.Fallback == nil || !slices.Contains(st.Fallback.When, "abstain") {
		return nil, ""
	}
	act, err := s.resolver.Resolve(s.pkg, st.Fallback.Action)
	if err != nil {
		return nil, ""
	}
	return act, st.Fallback.Action
}

// localDir returns the action directory of a local reference, ./actions/<dir>.
func localDir(ref string) (string, bool) {
	rest, ok := strings.CutPrefix(ref, "./actions/")
	return rest, ok && rest != "" && !strings.Contains(rest, "/")
}
