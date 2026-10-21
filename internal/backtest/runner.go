package backtest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/sandbox"
	"github.com/monolithiclab/millstream/internal/store"
)

// DefaultTimeout bounds each replay without Options.Timeout.
const DefaultTimeout = 30 * time.Minute

// Options configure a Backtester. Store is required.
type Options struct {
	// Store holds the history and the revisions; a backtest only reads it.
	Store store.Store
	// Artifacts holds the imported evidence bundles; nil reads local history only.
	Artifacts artifacts.Store
	// Mode is the deployment's environment (pipeline.ModeProduction refuses every backtest); empty reads
	// development.
	Mode string
	// Improve is the environment file's improve key: backtest.sample and budget.dayUsd.
	Improve pipeline.Improve
	// Budget records what backtests spent today against Improve.Budget.DayUSD; nil keeps a Ledger of its own.
	Budget Budget
	// Resolver, Launchers, Agents, Env and Secrets run the replayed steps as the deployment's embedded node would.
	Resolver  pipeline.ActionResolver
	Launchers map[string]exec.Launcher
	Agents    engine.Agents
	Env       func(string) (string, bool)
	Secrets   func(pipelineName string) pipeline.SecretLookup
	// Check is what the patched package is checked with.
	Check pipeline.CheckOptions
	// Sandbox runs the proposed exec actions, those whose files the patch touches (sandbox.Sandbox); without one, a
	// patch touching an exec action's files is refused (ErrUntrusted).
	Sandbox sandbox.Builder
	// Slots is how many deliveries a replay runs at once; zero means 4.
	Slots int
	// Timeout bounds each replay; zero means DefaultTimeout.
	Timeout time.Duration
	// WorkDir is where the scratch packages and artifacts go, removed after each backtest; empty means the system's
	// temporary directory.
	WorkDir string
	// Now is the clock of evidence expiry and of the budget's day; nil means time.Now.
	Now    func() time.Time
	Logger *slog.Logger
}

// Budget is what backtests spend against improve.budget.dayUsd (spec section 11.3).
type Budget interface {
	// Spent returns what was charged on now's UTC day.
	Spent(now time.Time) float64
	// Charge adds usd to now's UTC day.
	Charge(now time.Time, usd float64)
}

// Ledger is a Budget in memory: one process's backtests.
type Ledger struct {
	mu    sync.Mutex
	day   string
	spent float64
}

// Spent implements Budget.
func (l *Ledger) Spent(now time.Time) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.day != now.UTC().Format(time.DateOnly) {
		return 0
	}
	return l.spent
}

// Charge implements Budget.
func (l *Ledger) Charge(now time.Time, usd float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if day := now.UTC().Format(time.DateOnly); l.day != day {
		l.day, l.spent = day, 0
	}
	l.spent += usd
}

// Backtester is the engine's Runner.
type Backtester struct {
	opts Options
	log  *slog.Logger
	// served is the earliest distill boundary Training served for each pipeline and step (spec section 11.5): a
	// distill proposal's holdout may start no earlier.
	mu     sync.Mutex
	served map[[2]string]time.Time
	// reserved is what the running backtests hold of the day's improve budget: each reserves what is left when it
	// starts, so concurrent ones never spend it several times over.
	reserved float64
}

var _ Runner = (*Backtester)(nil)

// New returns a Backtester.
func New(opts Options) *Backtester {
	if opts.Budget == nil {
		opts.Budget = &Ledger{}
	}
	if opts.Resolver == nil {
		opts.Resolver = pipeline.Resolver{}
	}
	if opts.Env == nil {
		opts.Env = func(string) (string, bool) { return "", false }
	}
	if opts.Secrets == nil {
		opts.Secrets = pipeline.EnvSecrets(opts.Env)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	opts.Slots = cmp.Or(opts.Slots, 4)
	opts.Timeout = cmp.Or(opts.Timeout, DefaultTimeout)
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Backtester{opts: opts, log: log}
}

// Prepare checks what a request can be refused for without reading history or running anything: the fields, the
// environment, the patch and its stated base, and the budget. Run calls it first; the API calls it before answering.
func (b *Backtester) Prepare(req Request) (*patch.Patch, error) {
	switch {
	case b.opts.Mode == pipeline.ModeProduction:
		return nil, ErrProduction
	case req.Pipeline == "" || req.BaseRevision == "" || len(req.Patch) == 0:
		return nil, fmt.Errorf("a pipeline, a base revision and a patch are required: %w", store.ErrInvalid)
	case req.Sample < 0:
		return nil, fmt.Errorf("sample: must be positive: %w", store.ErrInvalid)
	case req.Source != "" && req.Source != SourceLocal && req.Source != SourceEvidence:
		return nil, fmt.Errorf("source: must be %s or %s: %w", SourceLocal, SourceEvidence, store.ErrInvalid)
	}
	p, err := patch.Parse(req.Patch)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", err, store.ErrInvalid)
	}
	if p.Base != "" && p.Base != req.BaseRevision {
		return nil, fmt.Errorf("the patch applies to %s, not %s: %w", p.Base, req.BaseRevision, store.ErrInvalid)
	}
	if left, limited := b.remaining(); limited && left <= 0 {
		return nil, b.errBudgetSpent()
	}
	return p, nil
}

// Run implements Runner.
func (b *Backtester) Run(ctx context.Context, req Request) (*Report, error) {
	p, err := b.Prepare(req)
	if err != nil {
		return nil, err
	}
	rep := &Report{Pipeline: req.Pipeline, BaseRevision: req.BaseRevision, PatchHash: patch.Hash(req.Patch),
		StartedAt: b.opts.Now().UTC()}
	work, err := os.MkdirTemp(b.opts.WorkDir, "millstream-backtest-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(work) }()

	base, patched, err := b.packages(ctx, req, p, work)
	if err != nil {
		return nil, err
	}
	rep.PatchedRevision = patched.Hash
	baseInfo, err := resolveActions(base, b.opts.Resolver)
	if err != nil {
		return nil, fmt.Errorf("base revision: %w", err)
	}
	patchedInfo, err := resolveActions(patched, b.opts.Resolver)
	if err != nil {
		return nil, fmt.Errorf("patched package: %w: %w", err, store.ErrInvalid)
	}
	touched := p.Paths()
	proposedDirs, proposedExec := sandbox.Proposed(patched, b.opts.Resolver, touched)
	if len(proposedExec) > 0 && b.opts.Sandbox == nil {
		return nil, fmt.Errorf("%w: %s, an exec action, would run with the node's privileges: this deployment "+
			"has no container runtime", ErrUntrusted, strings.TrimSuffix(proposedExec[0], "/"))
	}
	changed, err := changedSteps(base, patched, touched, req.Steps, baseInfo, patchedInfo)
	if err != nil {
		return nil, err
	}
	if len(changed) == 0 {
		return nil, fmt.Errorf("%w: the patch changes no step", ErrNoHistory)
	}
	rep.ChangedSteps = changed

	h, s, err := b.history(ctx, req, affected(base.Pipeline, patched.Pipeline, changed))
	if err != nil {
		return nil, err
	}
	rep.Source, rep.Bundles = h.source, h.bundles
	rep.Sampled, rep.Strata, rep.Runs = len(s.sampled), strata(s.sampled), len(s.roots)
	if s.excluded > 0 {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf("%d past deliveries were left out: their lineage cannot "+
			"be replayed (an import or replay run, or a payload gone)", s.excluded))
	}
	rep.Divergence = divergence(h, s, req.BaseRevision)
	for _, d := range rep.Divergence {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf("%d sampled deliveries were recorded on revision %s, "+
			"not the base: their outcomes may differ for that reason alone", d.Sampled, short(d.PackageHash)))
	}

	// Written to disk only now, for the actions to run from: a refusal writes nothing.
	if base, err = pipeline.Materialize(work, base.Files); err != nil {
		return nil, err
	}
	if patched, err = pipeline.Materialize(work, patched.Files); err != nil {
		return nil, err
	}
	patchedLaunchers := b.opts.Launchers
	if len(proposedExec) > 0 {
		var cleanup func()
		patchedLaunchers, cleanup, err = b.opts.Sandbox.Launchers(ctx, patched, proposedExec, b.opts.Launchers)
		switch {
		case errors.Is(err, sandbox.ErrUnsupported):
			return nil, fmt.Errorf("%w: %w", ErrUntrusted, err)
		case err != nil:
			return nil, fmt.Errorf("the proposed code: %w: %w", err, store.ErrInvalid)
		}
		defer cleanup()
	}
	isChanged := map[string]bool{}
	for _, c := range changed {
		isChanged[c] = true
	}
	limit, limited, release, err := b.reserve()
	if err != nil {
		return nil, err
	}
	var spent float64
	defer func() {
		b.opts.Budget.Charge(b.opts.Now(), spent)
		release()
	}()
	replays := make([]*replayed, 2)
	for i, side := range []struct {
		pkg       *pipeline.Package
		info      actionInfo
		launchers map[string]exec.Launcher
		resolver  pipeline.ActionResolver
	}{{base, baseInfo, b.opts.Launchers, b.opts.Resolver},
		{patched, patchedInfo, patchedLaunchers, sandbox.Confine(b.opts.Resolver, proposedDirs)}} {
		firings, err := b.firings(h, s, side.pkg.Pipeline)
		if err != nil {
			return nil, err
		}
		r, err := b.replay(ctx, work, replayOptions{pkg: side.pkg, launchers: side.launchers,
			resolver: side.resolver, firings: firings, limit: limit - spent,
			limited: limited, stubber: &stubber{h: h, keep: s.keep, changed: isChanged, external: side.info.external,
				review: side.info.review, decisions: map[string]decision{}}})
		if r != nil {
			spent += r.costUSD
		}
		if err != nil {
			return nil, err
		}
		replays[i] = r
	}
	rep.CostUSD = spent
	rep.Before, rep.After = measure(h, s, replays[0], ""), measure(h, s, replays[1], "")
	for _, step := range changed {
		rep.Steps = append(rep.Steps, StepReport{Step: step, Before: measure(h, s, replays[0], step),
			After: measure(h, s, replays[1], step)})
	}
	rep.FinishedAt = b.opts.Now().UTC()
	return rep, nil
}

// remaining is what is left of today's improve budget, and whether there is a limit at all.
func (b *Backtester) remaining() (float64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.left()
}

// left is what is left of the day's improve budget, held by no running backtest; b.mu is held.
func (b *Backtester) left() (float64, bool) {
	limit := b.opts.Improve.Budget.DayUSD
	return limit - b.opts.Budget.Spent(b.opts.Now()) - b.reserved, limit > 0
}

// errBudgetSpent is the refusal of a backtest that finds nothing left.
func (b *Backtester) errBudgetSpent() error {
	return fmt.Errorf("%w: %.2f USD spent or held by running backtests today", ErrBudget, b.opts.Improve.Budget.DayUSD)
}

// reserve holds what is left of the day's improve budget for a backtest until release: a concurrent backtest finds
// nothing left meanwhile (ErrBudget). Without a budget it holds nothing.
func (b *Backtester) reserve() (limit float64, limited bool, release func(), err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	left, limited := b.left()
	switch {
	case !limited:
		return 0, false, func() {}, nil
	case left <= 0:
		return 0, true, nil, b.errBudgetSpent()
	}
	b.reserved += left
	return left, true, func() {
		b.mu.Lock()
		b.reserved -= left
		b.mu.Unlock()
	}, nil
}

// packages returns the base revision's package and the patched one, in memory under work, the patched one checked.
func (b *Backtester) packages(ctx context.Context, req Request, p *patch.Patch, work string) (*pipeline.Package,
	*pipeline.Package, error,
) {
	revs, err := b.opts.Store.Revisions(ctx, req.Pipeline)
	if err != nil {
		return nil, nil, err
	}
	i := slices.IndexFunc(revs, func(r store.Revision) bool { return r.PackageHash == req.BaseRevision })
	if i < 0 {
		return nil, nil, fmt.Errorf("pipeline %s has no revision %s: %w", req.Pipeline, req.BaseRevision,
			store.ErrNotFound)
	}
	bundle, err := b.opts.Store.Package(ctx, req.BaseRevision)
	if err != nil {
		return nil, nil, err
	}
	files, err := pipeline.DecodeBundle(bundle)
	if err != nil {
		return nil, nil, err
	}
	patchedFiles, err := p.Apply(files)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", err, store.ErrInvalid)
	}
	base, err := pipeline.FromFiles(work, files)
	if err != nil {
		return nil, nil, err
	}
	patched, err := pipeline.FromFiles(work, patchedFiles)
	if err != nil {
		return nil, nil, fmt.Errorf("the patched package: %w: %w", err, store.ErrInvalid)
	}
	if patched.Pipeline.Name != req.Pipeline {
		return nil, nil, fmt.Errorf("the patch renames pipeline %s: %w", req.Pipeline, store.ErrInvalid)
	}
	check := b.opts.Check
	if check.Secrets == nil {
		check.Secrets = b.opts.Secrets
	}
	if diags := pipeline.Check(patched, b.opts.Resolver, b.opts.Env, check); pipeline.HasErrors(diags) {
		findings := make([]engine.Finding, len(diags))
		for i, d := range diags {
			findings[i] = engine.Finding{Pipeline: req.Pipeline, Diagnostic: d}
		}
		return nil, nil, fmt.Errorf("the patched package: %w", &engine.CheckError{Findings: findings})
	}
	return base, patched, nil
}

// history reads the history to sample and draws the sample over the affected steps: local history unless it has
// nothing to replay there, or unless the request names its source.
func (b *Backtester) history(ctx context.Context, req Request, steps map[string]bool) (*history, sample, error) {
	n := cmp.Or(req.Sample, b.opts.Improve.Backtest.Sample, DefaultSample)
	var h *history
	var s sample
	var err error
	if req.Source != SourceEvidence {
		if h, err = loadLocal(ctx, b.opts.Store, req.Pipeline); err != nil {
			return nil, s, err
		}
		if s = draw(h, steps, n); len(s.sampled) > 0 || req.Source == SourceLocal {
			return h, s, nonEmpty(s)
		}
	}
	if h, err = loadEvidence(ctx, b.opts.Store, b.opts.Artifacts, req.Pipeline, b.opts.Now()); err != nil {
		return nil, s, err
	}
	s = draw(h, steps, n)
	return h, s, nonEmpty(s)
}

func nonEmpty(s sample) error {
	if len(s.sampled) == 0 {
		return fmt.Errorf("%w: no past delivery of the changed steps or downstream of them ended", ErrNoHistory)
	}
	return nil
}

// firings are the sample's runs as p fires them again.
func (b *Backtester) firings(h *history, s sample, p *pipeline.Pipeline) ([]firing, error) {
	resolve := func(p *pipeline.Pipeline) (pipeline.Resolved, error) {
		return pipeline.ResolveVarsWith(p, nil, b.opts.Env, b.opts.Secrets(p.Name))
	}
	var out []firing
	for _, root := range s.roots {
		f, err := h.firing(root, p, resolve)
		if err != nil {
			return nil, err
		}
		if _, ok := p.Triggers[f.trigger]; !ok {
			continue // the patch removed the trigger: its runs no longer happen
		}
		out = append(out, f)
	}
	return out, nil
}

// divergence counts the sampled deliveries recorded on revisions other than base.
func divergence(h *history, s sample, base string) []Divergence {
	counts := map[string]int{}
	for _, sd := range s.sampled {
		if rev := h.revisions[h.deliveries[sd.Delivery].RunID]; rev != "" && rev != base {
			counts[rev]++
		}
	}
	var out []Divergence
	for _, rev := range slices.Sorted(maps.Keys(counts)) {
		out = append(out, Divergence{PackageHash: rev, Sampled: counts[rev]})
	}
	return out
}

func short(hash string) string { return hash[:min(len(hash), 12)] }
