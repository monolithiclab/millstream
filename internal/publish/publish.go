// Package publish holds the publish actions of accepted proposals (spec sections 11.3 and 11.7): apply creates a
// development revision on the proposal's base, patch-file writes the patch to a directory, git-pr-gh and git-pr-glab
// open a pull request with the gh or glab CLI from a container of their own.
//
// Every action publishes the reviewed patch and nothing else (spec section 11.7, rule 3): before it acts, the patch
// must hash to the validated hash (patch.Hash) and pass its kind's path rules again on its base revision
// (proposals.Prepare). New returns the action improve.publish names, with those checks in front of it.
package publish

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/proposals"
	"github.com/monolithiclab/millstream/internal/store"
)

// Accepted is an accepted proposal, as a publish action receives it.
type Accepted struct {
	ProposalID string
	Pipeline   string
	Kind       store.ProposalKind
	// BaseRevision is the package hash of the revision the proposal was validated on.
	BaseRevision string
	// Patch is the validated patch; PatchSHA256 its hash (patch.Hash) as the proposal records it. An action refuses
	// a patch that does not hash to it.
	Patch       string
	PatchSHA256 string
	Rationale   string
	// Report is the validation report (JSON), carried by pull requests.
	Report json.RawMessage
}

// FromProposal is a proposal as a publish action receives it.
func FromProposal(p store.Proposal) Accepted {
	return Accepted{ProposalID: p.ID, Pipeline: p.Pipeline, Kind: p.Kind, BaseRevision: p.BaseRevision,
		Patch: p.Patch, PatchSHA256: p.PatchSHA256, Rationale: p.Rationale, Report: p.Report}
}

// Result is where a publish action put the proposal.
type Result struct {
	// Publisher is the action's name (improve.publish).
	Publisher string
	// RevisionOrURL is the revision apply created (its number), the file patch-file wrote, or the pull request a
	// git action opened.
	RevisionOrURL string
}

// Publisher is a publish action.
type Publisher interface {
	// Name is the action's name, one of pipeline.PublishActions.
	Name() string
	// Publish publishes an accepted proposal. A patch that does not hash to its recorded hash, or that its kind's
	// path rules refuse, wraps ErrRefused.
	Publish(ctx context.Context, a Accepted) (Result, error)
}

// ErrRefused marks a proposal a publish action refuses: a patch other than the validated one, a path its kind does
// not allow, or (apply) a base that is no longer the active revision.
var ErrRefused = fmt.Errorf("publish refused: %w", store.ErrInvalid)

// Packages returns the package of a revision by its package hash.
type Packages func(ctx context.Context, hash string) (*pipeline.Package, error)

// Verify checks that a proposal is the reviewed one (spec section 11.7, rule 3): its patch hashes to its recorded
// hash and passes its kind's path rules on its base revision (rule 1), as proposals.Prepare applies them. It returns
// the patch applied to the base.
func Verify(ctx context.Context, packages Packages, a Accepted) (proposals.Prepared, error) {
	if got := patch.Hash([]byte(a.Patch)); a.PatchSHA256 == "" || got != a.PatchSHA256 {
		return proposals.Prepared{}, fmt.Errorf("%w: proposal %s: the patch hashes to %.12s, not the validated %.12s",
			ErrRefused, a.ProposalID, got, a.PatchSHA256)
	}
	base, err := packages(ctx, a.BaseRevision)
	if err != nil {
		return proposals.Prepared{}, fmt.Errorf("proposal %s: its base revision %.12s: %w", a.ProposalID,
			a.BaseRevision, err)
	}
	prep, err := proposals.Prepare(a.Kind, base, a.Patch)
	if err != nil {
		return prep, fmt.Errorf("%w: proposal %s: %w", ErrRefused, a.ProposalID, err)
	}
	if prep.Patched.Pipeline.Name != a.Pipeline {
		return prep, fmt.Errorf("%w: proposal %s: the patched package is pipeline %s, not %s", ErrRefused,
			a.ProposalID, prep.Patched.Pipeline.Name, a.Pipeline)
	}
	return prep, nil
}

// action is a publish action behind the checks: it receives the verified patch.
type action interface {
	publish(ctx context.Context, a Accepted, prep proposals.Prepared) (string, error)
}

// checked runs Verify before its action.
type checked struct {
	name     string
	packages Packages
	action   action
}

func (c checked) Name() string { return c.name }

func (c checked) Publish(ctx context.Context, a Accepted) (Result, error) {
	prep, err := Verify(ctx, c.packages, a)
	if err != nil {
		return Result{}, err
	}
	where, err := c.action.publish(ctx, a, prep)
	if err != nil {
		return Result{}, fmt.Errorf("publish proposal %s with %s: %w", a.ProposalID, c.name, err)
	}
	return Result{Publisher: c.name, RevisionOrURL: where}, nil
}

// ProposalStore is what Proposal reads and records: the store.
type ProposalStore interface {
	Proposal(ctx context.Context, id string) (store.Proposal, error)
	PipelineEvents(ctx context.Context, f store.EventFilter) ([]store.Event, error)
	RecordPublished(ctx context.Context, id, publisher, where string) error
}

// Proposal publishes the accepted proposal id with p, the patch the caller saw being patchSHA256, and records where
// it went (proposalPublished): what POST /proposals/{id}/publish runs for the improve package's publish step. A
// proposal published already is not published again: its earlier result comes back, again true. So does one apply
// published without the record (the engine stopped in between): its proposalApplied event counts, and is recorded.
func Proposal(ctx context.Context, st ProposalStore, p Publisher, id, patchSHA256 string) (res Result, again bool,
	err error,
) {
	prop, err := st.Proposal(ctx, id)
	switch {
	case err != nil:
		return Result{}, false, err
	case prop.State != store.ProposalAccepted:
		return Result{}, false, fmt.Errorf("proposal %s is %s: only an accepted one is published: %w", id,
			prop.State, store.ErrConflict)
	case patchSHA256 != prop.PatchSHA256:
		return Result{}, false, fmt.Errorf("%w: proposal %s: patch %.12s is not the validated one (%.12s)", ErrRefused,
			id, patchSHA256, prop.PatchSHA256)
	}
	evs, err := st.PipelineEvents(ctx, store.EventFilter{Pipeline: prop.Pipeline,
		Kinds: []string{store.EventProposalPublished, store.EventProposalApplied}})
	if err != nil {
		return Result{}, false, err
	}
	var applied *Result
	for _, ev := range evs {
		var done struct {
			ID, Publisher, Where string
			Revision             json.Number
		}
		if json.Unmarshal(ev.Data, &done) != nil || done.ID != id {
			continue
		}
		switch {
		case ev.Kind == store.EventProposalPublished:
			return Result{Publisher: done.Publisher, RevisionOrURL: done.Where}, true, nil
		case p.Name() == pipeline.PublishApply && applied == nil:
			applied = &Result{Publisher: pipeline.PublishApply, RevisionOrURL: done.Revision.String()}
		}
	}
	if applied != nil {
		return *applied, true, st.RecordPublished(ctx, id, applied.Publisher, applied.RevisionOrURL)
	}
	if res, err = p.Publish(ctx, FromProposal(prop)); err != nil {
		return res, false, err
	}
	return res, false, st.RecordPublished(ctx, id, res.Publisher, res.RevisionOrURL)
}
