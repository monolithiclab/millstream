// Package publish holds the publish actions of accepted proposals (spec sections 11.3 and 11.7): apply creates a
// development revision on the proposal's base, patch-file writes the patch to a directory.
//
// Every action publishes the reviewed patch and nothing else (spec section 11.7, rule 3): before it acts, the patch
// must hash to the validated hash (patch.Hash) and pass its kind's path rules again on its base revision
// (proposals.Prepare). New returns the action improve.publish names, with those checks in front of it.
package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
}

// FromProposal is a proposal as a publish action receives it.
func FromProposal(p store.Proposal) Accepted {
	return Accepted{ProposalID: p.ID, Pipeline: p.Pipeline, Kind: p.Kind, BaseRevision: p.BaseRevision,
		Patch: p.Patch, PatchSHA256: p.PatchSHA256}
}

// Result is where a publish action put the proposal.
type Result struct {
	// Publisher is the action's name (improve.publish).
	Publisher string
	// Where is the revision apply created (its number) or the file patch-file wrote.
	Where string
	// Build is, for a distill proposal, the command that builds the Go action the patch adds, which nothing builds
	// on its own (spec section 11.5); empty for other proposals.
	Build string
}

// BuildCommand is the command that builds the Go action a distill proposal's patch adds, run in the directory of the
// pipeline's package: an exec action resolves its program to bin/<os>-<arch>/<name> (spec section 6.2), and nothing
// builds it for the author. It is empty for any other kind of proposal.
func BuildCommand(kind store.ProposalKind, diff string) string {
	if kind != store.ProposalDistill {
		return ""
	}
	p, err := patch.Parse([]byte(diff))
	if err != nil {
		return ""
	}
	for _, f := range p.Files {
		if rest, ok := strings.CutPrefix(f.Path, "actions/"); ok && f.Op == patch.OpCreate {
			if dir, _, found := strings.Cut(rest, "/"); found && dir != "" {
				return fmt.Sprintf("cd actions/%[1]s && go build -o bin/$(go env GOOS)-$(go env GOARCH)/%[1]s .", dir)
			}
		}
	}
	return ""
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
	return Result{Publisher: c.name, Where: where}, nil
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
	build := BuildCommand(prop.Kind, prop.Patch)
	published, err := Publications(ctx, st, prop.Pipeline)
	if err != nil {
		return Result{}, false, err
	}
	if done, ok := published[id]; ok {
		return Result{Publisher: done.Publisher, Where: done.Where, Build: build}, true, nil
	}
	if p.Name() == pipeline.PublishApply {
		evs, err := st.PipelineEvents(ctx, store.EventFilter{Pipeline: prop.Pipeline,
			Kinds: []string{store.EventProposalApplied}})
		if err != nil {
			return Result{}, false, err
		}
		for _, ev := range evs {
			var applied struct {
				ID       string
				Revision json.Number
			}
			if ev.Kind == store.EventProposalApplied && json.Unmarshal(ev.Data, &applied) == nil && applied.ID == id {
				res = Result{Publisher: pipeline.PublishApply, Where: applied.Revision.String(), Build: build}
				return res, true, st.RecordPublished(ctx, id, res.Publisher, res.Where)
			}
		}
	}
	if res, err = p.Publish(ctx, FromProposal(prop)); err != nil {
		return res, false, err
	}
	res.Build = build
	return res, false, st.RecordPublished(ctx, id, res.Publisher, res.Where)
}

// Publication is where a publish action put a proposal, and when: its proposalPublished event.
type Publication struct {
	Publisher, Where string
	At               time.Time
}

// Publications returns the publications of a pipeline's proposals, by proposal id, the first of each.
func Publications(ctx context.Context, st interface {
	PipelineEvents(ctx context.Context, f store.EventFilter) ([]store.Event, error)
}, pipelineName string,
) (map[string]Publication, error) {
	evs, err := st.PipelineEvents(ctx, store.EventFilter{Pipeline: pipelineName,
		Kinds: []string{store.EventProposalPublished}})
	if err != nil {
		return nil, err
	}
	out := map[string]Publication{}
	for _, ev := range evs {
		var d struct{ ID, Publisher, Where string }
		if ev.Kind != store.EventProposalPublished || json.Unmarshal(ev.Data, &d) != nil {
			continue
		}
		if _, seen := out[d.ID]; !seen {
			out[d.ID] = Publication{Publisher: d.Publisher, Where: d.Where, At: ev.TS}
		}
	}
	return out, nil
}
