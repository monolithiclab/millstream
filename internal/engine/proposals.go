package engine

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/proposals"
	"github.com/monolithiclab/millstream/internal/store"
)

// Proposals (spec section 11.3): the engine enforces the rules, never drafts. A proposal's patch is checked against
// its kind's path rules when it is created, rebased and accepted (spec section 11.7), validated by the engine before
// it is ready (check, the base revision's evals before and after, the backtest), and accepted by the store's
// Proposal acceptance unit only on its base revision.

// ProposalOptions are what the engine validates proposals with.
type ProposalOptions struct {
	// Evals runs the evals of the affected actions; its Env defaults to the engine's Env and its Prices to the
	// agents'.
	Evals proposals.Evals
	// Backtest runs the backtests (backtest.ForProposals); without one, every proposal fails its validation.
	Backtest proposals.Backtester
	// Holdout reads the holdout distill proposals are measured on (backtest.Backtester), Distill their thresholds
	// (improve.distill); without a source, every distill proposal fails its validation.
	Holdout proposals.HoldoutSource
	Distill distill.Thresholds
	// ValidationTimeout bounds a validation (improve.validation.timeout); zero means DefaultValidationTimeout. A
	// proposal validating for longer is presumed lost, and the next validation takes it over.
	ValidationTimeout time.Duration
}

// DefaultValidationTimeout is improve.validation.timeout's default.
const DefaultValidationTimeout = 30 * time.Minute

// validationTimeout is ProposalOptions.ValidationTimeout or its default.
func (e *Engine) validationTimeout() time.Duration {
	return cmp.Or(e.opts.Proposals.ValidationTimeout, DefaultValidationTimeout)
}

// ProposalRequest is a proposal to create on a pipeline's active revision.
type ProposalRequest struct {
	Pipeline  string
	Kind      store.ProposalKind
	Patch     string
	Rationale string
	Insights  []string
	// Fingerprint names the idea; empty derives it from the pipeline, the kind, the insights and the paths.
	Fingerprint string
	By          string
}

// ProposalTokenPrefix marks a proposals-only token, as mswh_ marks a webhook trigger's.
const ProposalTokenPrefix = "mspr_"

// NewProposalToken returns a new proposals-only API token and the hash the store keeps of it (spec section 11.3).
func NewProposalToken() (token, hash string) {
	b := make([]byte, 32)
	_, _ = rand.Read(b) // crypto/rand never fails on the platforms Go supports
	token = ProposalTokenPrefix + base64.RawURLEncoding.EncodeToString(b)
	return token, TriggerTokenHash(token)
}

// MintProposalToken mints the proposals-only token, replacing any earlier one, and returns it: shown once, since the
// store keeps only its hash.
func (e *Engine) MintProposalToken(ctx context.Context) (string, error) {
	token, hash := NewProposalToken()
	if err := e.st.SetProposalToken(ctx, hash); err != nil {
		return "", err
	}
	return token, nil
}

// CreateProposal checks a patch against its kind's path rules on the pipeline's active revision, records it
// drafting, and validates it. A patch the rules refuse is store.ErrInvalid; a suppressed fingerprint is
// store.ErrConflict.
func (e *Engine) CreateProposal(ctx context.Context, req ProposalRequest) (store.Proposal, error) {
	rev, err := e.activeRevision(ctx, req.Pipeline)
	if err != nil {
		return store.Proposal{}, err
	}
	prep, err := e.prepare(ctx, false, req.Kind, rev.PackageHash, req.Patch)
	if err != nil {
		return store.Proposal{}, err
	}
	fp := req.Fingerprint
	if fp == "" {
		fp = proposals.Fingerprint(req.Pipeline, req.Kind, req.Insights, prep.Paths())
	}
	id := make([]byte, 8)
	_, _ = rand.Read(id)
	p, err := e.st.CreateProposal(ctx, store.NewProposal{ID: "pr_" + hex.EncodeToString(id), Pipeline: req.Pipeline,
		Kind: req.Kind, BaseRevision: rev.PackageHash, Patch: req.Patch, PatchSHA256: prep.SHA256, Paths: prep.Paths(),
		Steps: prep.Steps, Rationale: req.Rationale, Insights: req.Insights, Fingerprint: fp, CreatedBy: req.By})
	if err != nil {
		return store.Proposal{}, err
	}
	return e.ValidateProposal(ctx, p.ID)
}

// ValidateProposal validates a drafting or ready proposal on its base and records the report: ready when it passes,
// drafting otherwise. A stale proposal is revalidated on the pipeline's active revision: its patch is rebased there
// first, and one that does not rebase stays stale (store.ErrConflict), as does one made stale by an accepted
// proposal while its base is still the active revision: the accepted one is to be applied first.
func (e *Engine) ValidateProposal(ctx context.Context, id string) (store.Proposal, error) {
	p, err := e.proposal(ctx, id)
	if err != nil {
		return p, err
	}
	base := p.BaseRevision
	var rebase *store.Rebase
	if p.State == store.ProposalStale {
		rev, err := e.activeRevision(ctx, p.Pipeline)
		if err != nil {
			return p, err
		}
		if rev.PackageHash == p.BaseRevision {
			return p, fmt.Errorf("proposal %s is stale on the active revision of %s: a proposal accepted over it "+
				"comes first; revalidate it once that one is applied: %w", id, p.Pipeline, store.ErrConflict)
		}
		base = rev.PackageHash
	}
	prep, err := e.prepare(ctx, p.State == store.ProposalStale, p.Kind, base, p.Patch)
	switch {
	case err != nil && p.State == store.ProposalStale:
		return p, fmt.Errorf("proposal %s does not rebase onto the active revision of %s, so it stays stale: %w: %w",
			id, p.Pipeline, store.ErrConflict, err)
	case err != nil:
		return p, err
	case p.State == store.ProposalStale:
		rebase = &store.Rebase{BaseRevision: base, Patch: prep.Patch, PatchSHA256: prep.SHA256, Paths: prep.Paths(),
			Steps: prep.Steps}
	}
	timeout := e.validationTimeout()
	if _, err := e.st.StartValidation(ctx, id, e.opts.Now().Add(-timeout)); err != nil {
		return p, err
	}
	// The validation outlives the request that started it, within its timeout, so the proposal never stays
	// validating because a client went away.
	vctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	report, passed := e.validate(vctx, p.Pipeline, prep)
	return e.st.RecordValidation(context.WithoutCancel(ctx), store.Validation{ID: id, Report: report, Passed: passed,
		Rebase: rebase})
}

// resetValidations moves the proposals left validating by an engine that stopped back to drafting, when this one
// starts or takes the lead: their validations died with it. Reading first leaves the store unwritten when there is
// none, as there almost always is.
func (e *Engine) resetValidations(ctx context.Context) {
	ps, err := e.st.Proposals(ctx, store.ProposalFilter{State: store.ProposalValidating})
	if err == nil && len(ps) > 0 {
		var ids []string
		if ids, err = e.st.ResetValidations(ctx); err == nil {
			e.log.Warn("proposals left validating by a stopped engine are drafting again", "proposals", ids)
		}
	}
	if err != nil && ctx.Err() == nil {
		e.log.Error("reset validating proposals", "error", err.Error())
	}
}

// validate runs the validation and encodes its report; a validation that could not run fails with its error as the
// report, so the proposal goes back to drafting rather than staying validating.
func (e *Engine) validate(ctx context.Context, pipelineName string, prep proposals.Prepared) (json.RawMessage, bool) {
	r, err := e.validator().Validate(ctx, pipelineName, prep)
	if err != nil {
		data, _ := json.Marshal(map[string]any{"passed": false, "baseRevision": prep.Base.Hash,
			"patchSha256": prep.SHA256, "error": err.Error()})
		return data, false
	}
	data, err := json.Marshal(r)
	if err != nil {
		return json.RawMessage(`{"passed":false}`), false
	}
	return data, r.Passed
}

// validator is the engine's proposal validator.
func (e *Engine) validator() proposals.Validator {
	evals := e.opts.Proposals.Evals
	if evals.Env == nil {
		evals.Env = func(string) func(string) (string, bool) { return e.opts.Env }
	}
	if evals.Prices == nil {
		evals.Prices = e.opts.Agents.Prices
	}
	return proposals.Validator{Check: e.checkProposal, Resolver: e.opts.Resolver, Evals: evals,
		Backtest: e.opts.Proposals.Backtest, Holdout: e.opts.Proposals.Holdout, Distill: e.opts.Proposals.Distill,
		Scratch: e.opts.PackageDir}
}

// checkProposal checks a patched package as apply would, against the rest of the deployment.
func (e *Engine) checkProposal(ctx context.Context, pkg *pipeline.Package) []pipeline.Diagnostic {
	findings, err := e.checkApply(ctx, []*pipeline.Package{pkg}, ApplyOptions{})
	if err != nil {
		return []pipeline.Diagnostic{{Severity: pipeline.SeverityError, Path: pipeline.FileName,
			Message: "check could not run: " + err.Error()}}
	}
	var out []pipeline.Diagnostic
	for _, f := range findings {
		if f.Pipeline == pkg.Pipeline.Name {
			out = append(out, f.Diagnostic)
		}
	}
	return out
}

// AcceptProposal accepts a ready proposal whose patch hash is the one the decider saw, its patch checked against its
// kind's path rules again: the store's Proposal acceptance unit then accepts it on its base revision only, making
// the open proposals it overlaps stale, or makes it stale itself (store.ErrConflict).
func (e *Engine) AcceptProposal(ctx context.Context, id, patchSHA256, by string) (store.Proposal, error) {
	p, err := e.proposal(ctx, id)
	if err != nil {
		return p, err
	}
	if _, err := e.prepare(ctx, false, p.Kind, p.BaseRevision, p.Patch); err != nil {
		return p, err
	}
	return e.st.AcceptProposal(ctx, id, patchSHA256, by)
}

// prepare applies a patch to the package stored under hash (proposals.Prepare), or rebases it there
// (proposals.Rebase), under its kind's path rules; a refusal is store.ErrInvalid.
func (e *Engine) prepare(ctx context.Context, rebase bool, kind store.ProposalKind, hash, text string,
) (proposals.Prepared, error) {
	base, err := e.StoredPackage(ctx, hash)
	if err != nil {
		return proposals.Prepared{}, err
	}
	prepare := proposals.Prepare
	if rebase {
		prepare = proposals.Rebase
	}
	prep, err := prepare(kind, base, text)
	if errors.Is(err, patch.ErrPatch) {
		return prep, fmt.Errorf("%w: %w", store.ErrInvalid, err)
	}
	return prep, err
}

// proposal reads a proposal whose stored patch still hashes to its recorded hash.
func (e *Engine) proposal(ctx context.Context, id string) (store.Proposal, error) {
	p, err := e.st.Proposal(ctx, id)
	if err == nil && patch.Hash([]byte(p.Patch)) != p.PatchSHA256 {
		err = fmt.Errorf("proposal %s: its patch no longer hashes to %.12s: %w", id, p.PatchSHA256, store.ErrConflict)
	}
	return p, err
}

// activeRevision returns a pipeline's active revision.
func (e *Engine) activeRevision(ctx context.Context, pipelineName string) (store.Revision, error) {
	id, err := e.st.ActiveRevision(ctx, pipelineName)
	if err != nil {
		return store.Revision{}, err
	}
	return e.st.Revision(ctx, id)
}
