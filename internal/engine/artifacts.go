package engine

import (
	"context"
	"errors"
	"io"
	"io/fs"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/store"
)

// errNoArtifactStore fails the artifact calls of an engine started without an artifact store.
var errNoArtifactStore = errors.New("the engine has no artifact store")

// PutArtifact implements node.Artifacts over the engine's artifact store, for its embedded node: the lease names the
// pipeline.
func (e *Engine) PutArtifact(ctx context.Context, lease store.LeaseRef, name string, r io.Reader) (store.ArtifactRef,
	error,
) {
	if e.opts.Artifacts == nil {
		return store.ArtifactRef{}, errNoArtifactStore
	}
	return e.opts.Artifacts.Put(ctx, lease.Pipeline, name, r)
}

// OpenArtifact implements node.Artifacts over the engine's artifact store, for its embedded node: an input of the
// delivery the lease names, whose file is copied from the producer's pipeline on first use when its item is an
// import copy (OpenArtifact the function).
func (e *Engine) OpenArtifact(ctx context.Context, lease store.LeaseRef, ref store.ArtifactRef) (io.ReadCloser, error) {
	if e.opts.Artifacts == nil {
		return nil, errNoArtifactStore
	}
	r, err := e.opts.Artifacts.Open(ctx, lease.Pipeline, ref.SHA256)
	if !errors.Is(err, fs.ErrNotExist) {
		return r, err
	}
	from, err := e.copiedFrom(ctx, lease)
	if err != nil {
		return nil, err
	}
	if err := copyArtifact(ctx, e.opts.Artifacts, lease.Pipeline, from, ref); err != nil {
		return nil, err
	}
	return e.opts.Artifacts.Open(ctx, lease.Pipeline, ref.SHA256)
}

// copiedFrom is the pipeline the item of the delivery in flight under lease was copied from (store.Item's
// ParentPipeline), empty when it is no import copy or the delivery is not in flight.
func (e *Engine) copiedFrom(ctx context.Context, lease store.LeaseRef) (string, error) {
	f := e.lookup(lease)
	if f.itemID == "" {
		return "", nil
	}
	items, err := e.st.Items(ctx, store.ItemFilter{Pipeline: lease.Pipeline, IDs: []string{f.itemID}})
	if err != nil || len(items) == 0 {
		return "", err
	}
	return items[0].ParentPipeline, nil
}

// OpenArtifact opens a pipeline's artifact in arts, made present there first (EnsureArtifact): from is the pipeline
// the artifact's item was copied from (store.Item's ParentPipeline), empty for any other item. Nodes and the API open
// artifacts through it alike.
func OpenArtifact(ctx context.Context, arts artifacts.Store, pipeline, from string, ref store.ArtifactRef,
) (io.ReadSeekCloser, error) {
	r, err := arts.Open(ctx, pipeline, ref.SHA256)
	if !errors.Is(err, fs.ErrNotExist) {
		return r, err
	}
	if err := copyArtifact(ctx, arts, pipeline, from, ref); err != nil {
		return nil, err
	}
	return arts.Open(ctx, pipeline, ref.SHA256)
}

// EnsureArtifact makes a pipeline's artifact present in its prefix of arts. The artifact of an item copied through
// an imported stream (spec section 4.12) has its file in the producer's prefix, from, and the first use copies it
// into the pipeline's own, where the pipeline's retention and erasure find it. Content is never copied from any
// other pipeline (spec section 15.7): an artifact found neither in the pipeline nor in from is an error wrapping
// fs.ErrNotExist. The caller found ref among the item's artifacts.
func EnsureArtifact(ctx context.Context, arts artifacts.Store, pipeline, from string, ref store.ArtifactRef) error {
	if ok, err := arts.Exists(ctx, pipeline, ref.SHA256); err != nil || ok {
		return err
	}
	return copyArtifact(ctx, arts, pipeline, from, ref)
}

// copyArtifact copies an artifact missing from pipeline's prefix from the producer's, from (EnsureArtifact).
func copyArtifact(ctx context.Context, arts artifacts.Store, pipeline, from string, ref store.ArtifactRef) error {
	if from == "" || from == pipeline {
		return errNotExist(pipeline, ref)
	}
	err := arts.Copy(ctx, from, pipeline, ref.SHA256)
	if errors.Is(err, fs.ErrNotExist) {
		return errors.Join(err, errNotExist(pipeline, ref))
	}
	return err
}

// errNotExist is the error of an artifact found nowhere.
func errNotExist(pipeline string, ref store.ArtifactRef) error {
	return &fs.PathError{Op: "open", Path: "artifact " + pipeline + "/" + ref.SHA256, Err: fs.ErrNotExist}
}
