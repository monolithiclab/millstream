package publish

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/proposals"
	"github.com/monolithiclab/millstream/internal/store"
)

// Applier creates a pipeline's development revision from an accepted proposal: the engine
// (engine.Engine.ApplyProposal).
type Applier interface {
	// ApplyProposal applies files as the pipeline's new revision only while its active revision is still base, the
	// proposal's (store.ErrConflict otherwise), and records the proposal applied. A production
	// deployment refuses it.
	ApplyProposal(ctx context.Context, proposalID, pipelineName, base string, files map[string][]byte) (
		store.RevisionID, error)
}

// Deps are what the publish actions need besides their settings.
type Deps struct {
	// Packages reads base revisions, for Verify. Required.
	Packages Packages
	// Applier is apply's.
	Applier Applier
}

// New returns the publish action improve.publish names, configured by the rest of improve, with Verify in front of
// it; nil when improve.publish is unset.
func New(im pipeline.Improve, d Deps) (Publisher, error) {
	if im.Publish == "" {
		return nil, nil
	}
	if d.Packages == nil {
		return nil, errors.New("publish: no package source")
	}
	var a action
	switch im.Publish {
	case pipeline.PublishApply:
		if d.Applier == nil {
			return nil, errors.New("publish: apply needs the engine")
		}
		a = applyAction{d.Applier}
	case pipeline.PublishPatchFile:
		if im.PatchFile.Dir == "" {
			return nil, errors.New("publish: patch-file needs improve.patchFile.dir")
		}
		a = patchFile{dir: im.PatchFile.Dir}
	default:
		return nil, fmt.Errorf("publish: unknown action %q: want one of %v", im.Publish, pipeline.PublishActions)
	}
	return checked{name: im.Publish, packages: d.Packages, action: a}, nil
}

// applyAction is apply: a development revision of the patched package, on the condition that the active revision
// is still the proposal's base (spec section 11.3).
type applyAction struct{ applier Applier }

func (a applyAction) publish(ctx context.Context, acc Accepted, prep proposals.Prepared) (string, error) {
	rev, err := a.applier.ApplyProposal(ctx, acc.ProposalID, acc.Pipeline, acc.BaseRevision, prep.Patched.Files)
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(int64(rev), 10), nil
}

// proposalID is the shape of the proposal ids a file name may hold.
var proposalID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// patchFile is patch-file: the validated patch, byte for byte, as <dir>/<proposal id>.patch.
type patchFile struct{ dir string }

func (p patchFile) publish(_ context.Context, acc Accepted, _ proposals.Prepared) (string, error) {
	if !proposalID.MatchString(acc.ProposalID) {
		return "", fmt.Errorf("%w: proposal id %q cannot name a file", ErrRefused, acc.ProposalID)
	}
	if err := os.MkdirAll(p.dir, 0o750); err != nil {
		return "", err
	}
	path := filepath.Join(p.dir, acc.ProposalID+".patch")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- the operator's directory
	if errors.Is(err, fs.ErrExist) {
		// Published already: the same bytes are the same publication, other bytes are refused.
		old, rerr := os.ReadFile(path) // #nosec G304 -- as above
		if rerr == nil && bytes.Equal(old, []byte(acc.Patch)) {
			return path, nil
		}
		return "", fmt.Errorf("%w: %s exists with other content", ErrRefused, path)
	}
	if err != nil {
		return "", err
	}
	if _, err := f.WriteString(acc.Patch); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return "", err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}
