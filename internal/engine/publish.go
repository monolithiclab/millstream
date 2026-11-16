package engine

import (
	"context"
	"fmt"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Publishing (spec sections 11.3 and 11.7): the apply publish action creates a development revision on the
// proposal's base only; and whatever applies a revision that is exactly an accepted proposal's patch over its base
// (the apply action, or millstream apply of the patched package) records the proposal applied.

// ApplyProposal applies an accepted proposal's patched files as the pipeline's new development revision while its
// active revision is still base (store.ErrConflict otherwise), recording the proposal applied: the apply publish
// action (publish.Applier). A production deployment refuses it (pipeline.ErrProduction).
func (e *Engine) ApplyProposal(ctx context.Context, proposalID, pipelineName, base string, files map[string][]byte,
) (store.RevisionID, error) {
	if err := e.developmentOnly("apply proposal " + proposalID); err != nil {
		return 0, err
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
		Message: "proposal " + proposalID})
	if err != nil {
		return 0, fmt.Errorf("apply proposal %s: %w", proposalID, err)
	}
	return applied[0].Revision, nil
}

// applies returns the accepted proposals of the pipeline whose patch over the active revision before, their base,
// is the package hash: those the revision applies. A proposal whose patch no longer prepares is logged and left out.
func (e *Engine) applies(ctx context.Context, pipelineName string, before store.RevisionID, hash string,
) ([]string, error) {
	rev, err := e.st.Revision(ctx, before)
	if err != nil {
		return nil, err
	}
	accepted, err := e.st.Proposals(ctx, store.ProposalFilter{Pipeline: pipelineName, State: store.ProposalAccepted})
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, p := range accepted {
		if p.BaseRevision != rev.PackageHash {
			continue
		}
		prep, err := e.prepare(ctx, false, p.Kind, p.BaseRevision, p.Patch)
		if err != nil {
			e.log.Warn("accepted proposal no longer prepares on its base: not matched", "pipeline", pipelineName,
				"proposal", p.ID, "error", err)
			continue
		}
		if prep.Patched.Hash == hash {
			ids = append(ids, p.ID)
		}
	}
	return ids, nil
}

// StoredPackage returns the package stored under hash, as its files say, without writing it anywhere.
func (e *Engine) StoredPackage(ctx context.Context, hash string) (*pipeline.Package, error) {
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
