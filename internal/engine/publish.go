package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/proposals"
	"github.com/monolithiclab/millstream/internal/store"
)

// Publishing (spec sections 11.3 and 11.7): the apply publish action creates a development revision on the
// proposal's base only; and whatever applies a revision carrying accepted proposals (the Millstream-Proposal
// trailers of the commit it came from, or the apply action itself) records them applied, validated in that
// combination when the revision is exactly the proposal's patch over its base. A revision carrying one in any other
// combination is validated against the revision before it, as a proposal is (check, evals, backtest), and a
// regression raises a regression insight.

// ApplyProposal applies an accepted proposal's patched files as the pipeline's new development revision while its
// active revision is still base (store.ErrConflict otherwise), recording the proposal applied: the apply publish
// action (publish.Applier). A production deployment refuses it (pipeline.ErrProduction).
func (e *Engine) ApplyProposal(ctx context.Context, proposalID, pipelineName, base string, files map[string][]byte,
) (store.RevisionID, error) {
	if e.opts.Mode == pipeline.ModeProduction {
		return 0, fmt.Errorf("apply proposal %s: improve.publish: apply is %w", proposalID, pipeline.ErrProduction)
	}
	pkg, err := e.PackageFromFiles(files)
	if err != nil {
		return 0, err
	}
	if pkg.Pipeline.Name != pipelineName {
		return 0, fmt.Errorf("apply proposal %s: the package is pipeline %s, not %s: %w", proposalID,
			pkg.Pipeline.Name, pipelineName, store.ErrInvalid)
	}
	applied, _, err := e.ApplyPackages(ctx, []*pipeline.Package{pkg}, ApplyOptions{Base: base,
		Message: "proposal " + proposalID, Proposals: map[string][]string{pipelineName: {proposalID}}})
	if err != nil {
		return 0, fmt.Errorf("apply proposal %s: %w", proposalID, err)
	}
	return applied[0].Revision, nil
}

// carried returns the accepted proposals of the pipeline among ids, each validated when parent is its base and the
// applied package (hash) is its patch over it; other ids are logged and left out.
func (e *Engine) carried(ctx context.Context, pipelineName, parent, hash string, ids []string,
) []store.AppliedProposal {
	var out []store.AppliedProposal
	for _, id := range ids {
		if slices.ContainsFunc(out, func(a store.AppliedProposal) bool { return a.ID == id }) {
			continue
		}
		p, err := e.proposal(ctx, id)
		if err != nil || p.Pipeline != pipelineName || p.State != store.ProposalAccepted {
			e.log.Warn("a Millstream-Proposal trailer names no accepted proposal of the pipeline: ignored",
				"pipeline", pipelineName, "proposal", id, "error", err)
			continue
		}
		validated := false
		if parent == p.BaseRevision {
			prep, err := e.prepare(ctx, false, p.Kind, p.BaseRevision, p.Patch)
			validated = err == nil && prep.Patched.Hash == hash
		}
		out = append(out, store.AppliedProposal{ID: id, Validated: validated})
	}
	return out
}

// checkCombination validates a revision carrying proposals in a combination nobody validated against the revision
// before it, while Run runs in the background, else before returning, and raises a regression insight when it
// regressed.
func (e *Engine) checkCombination(ctx context.Context, pipelineName string, rev store.RevisionID, parent,
	hash string, carried []store.AppliedProposal,
) {
	var ids []string
	for _, a := range carried {
		if !a.Validated {
			ids = append(ids, a.ID)
		}
	}
	if len(ids) == 0 || parent == "" {
		return
	}
	e.bg.do(ctx, func(ctx context.Context) {
		if err := e.validateCombination(ctx, pipelineName, rev, parent, hash, ids); err != nil {
			e.log.Error("revision carrying unvalidated proposals: its validation did not run", "pipeline",
				pipelineName, "revision", int64(rev), "proposals", ids, "error", err)
		}
	})
}

// validateCombination validates the change from parent to hash as a proposal's (check, the evals, the backtest), and
// raises a regression insight when the check or an eval fails, or the backtest ran and did not hold.
func (e *Engine) validateCombination(ctx context.Context, pipelineName string, rev store.RevisionID, parent,
	hash string, ids []string,
) error {
	before, err := e.storedPackage(ctx, parent)
	if err != nil {
		return err
	}
	after, err := e.storedPackage(ctx, hash)
	if err != nil {
		return err
	}
	prep, err := proposals.Diff(before, after)
	if err != nil {
		return err
	}
	report, err := e.validator().Validate(ctx, pipelineName, prep)
	if err != nil {
		return err
	}
	evalFailed := slices.ContainsFunc(report.Evals, func(r proposals.EvalReport) bool { return !r.Passed })
	backtestFailed := report.Backtest.Skipped == "" && !report.Backtest.Passed
	e.log.Info("revision carrying unvalidated proposals validated", "pipeline", pipelineName, "revision", int64(rev),
		"proposals", ids, "check", report.Check.Passed, "evals", !evalFailed, "backtest", report.Backtest.Passed,
		"backtestSkipped", report.Backtest.Skipped)
	if report.Check.Passed && !evalFailed && !backtestFailed {
		return nil
	}
	return e.raiseRegression(ctx, pipelineName, rev, parent, hash, ids, report)
}

// raiseRegression opens a regression insight on the pipeline, keeping its other open regression insights, unless
// the pipeline's insights key turns the kind off.
func (e *Engine) raiseRegression(ctx context.Context, pipelineName string, rev store.RevisionID, parent, hash string,
	ids []string, report proposals.Report,
) error {
	r, err := e.st.Revision(ctx, rev)
	if err != nil {
		return err
	}
	p, err := r.Decode()
	if err != nil {
		return err
	}
	if cfg, err := p.InsightsConfig(); err != nil || len(cfg.Kinds([]string{pipeline.InsightRegression})) == 0 {
		return err
	}
	evidence, err := json.Marshal(map[string]any{"revision": rev, "packageHash": hash, "previous": parent,
		"proposals": ids, "report": report})
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(pipeline.InsightRegression + "\x00" + pipelineName + "\x00" + hash))
	now := e.opts.Now().UTC()
	found := store.Insight{Pipeline: pipelineName, ID: "in_" + hex.EncodeToString(sum[:8]),
		Kind: pipeline.InsightRegression, Scope: "pipeline", WindowStart: r.AppliedAt, WindowEnd: now,
		Severity: pipeline.SeverityWarning, Evidence: evidence}
	open, err := e.st.Insights(ctx, store.InsightFilter{Pipeline: pipelineName})
	if err != nil {
		return err
	}
	insights := []store.Insight{found}
	for _, in := range open {
		if in.Kind == pipeline.InsightRegression && in.ID != found.ID && in.State != store.InsightResolved {
			insights = append(insights, in)
		}
	}
	return e.st.RecordInsights(ctx, store.InsightReport{Pipeline: pipelineName,
		Kinds: []string{pipeline.InsightRegression}, Insights: insights})
}

// storedPackage returns the package stored under hash, as its files say, without writing it anywhere.
func (e *Engine) storedPackage(ctx context.Context, hash string) (*pipeline.Package, error) {
	bundle, err := e.st.Package(ctx, hash)
	if err != nil {
		return nil, err
	}
	files, err := pipeline.DecodeBundle(bundle)
	if err != nil {
		return nil, fmt.Errorf("package %.12s: %w", hash, err)
	}
	pkg, err := pipeline.FromFiles("", files)
	if err != nil {
		return nil, fmt.Errorf("package %.12s: %w", hash, err)
	}
	return pkg, nil
}

// background runs work started while Run runs on Run's context, and lets Run wait for it once it stops; outside Run,
// the work runs before do returns.
type background struct {
	mu  sync.Mutex
	ctx context.Context // Run's, nil outside it
	wg  sync.WaitGroup
}

func (b *background) start(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ctx = ctx
}

// stop refuses new background work, then waits for what runs.
func (b *background) stop() {
	b.mu.Lock()
	b.ctx = nil
	b.mu.Unlock()
	b.wg.Wait()
}

// do runs fn on Run's context in the background, or with ctx now when Run is not running.
func (b *background) do(ctx context.Context, fn func(context.Context)) {
	b.mu.Lock()
	runCtx := b.ctx
	if runCtx != nil {
		b.wg.Add(1)
	}
	b.mu.Unlock()
	if runCtx == nil {
		fn(ctx)
		return
	}
	go func() {
		defer b.wg.Done()
		fn(runCtx)
	}()
}
