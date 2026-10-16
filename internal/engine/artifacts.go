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

// OpenArtifact implements node.Artifacts over the engine's artifact store (OpenArtifact the function).
func (e *Engine) OpenArtifact(ctx context.Context, lease store.LeaseRef, ref store.ArtifactRef) (io.ReadCloser, error) {
	if e.opts.Artifacts == nil {
		return nil, errNoArtifactStore
	}
	return OpenArtifact(ctx, e.st, e.opts.Artifacts, lease.Pipeline, ref)
}

// OpenArtifact opens a pipeline's artifact in arts, made present there first (EnsureArtifact). Nodes and the API
// open artifacts through it alike.
func OpenArtifact(ctx context.Context, st store.Store, arts artifacts.Store, pipeline string, ref store.ArtifactRef,
) (io.ReadSeekCloser, error) {
	r, err := arts.Open(ctx, pipeline, ref.SHA256)
	if !errors.Is(err, fs.ErrNotExist) {
		return r, err
	}
	if err := EnsureArtifact(ctx, st, arts, pipeline, ref); err != nil {
		return nil, errors.Join(err, errNotExist(pipeline, ref))
	}
	return arts.Open(ctx, pipeline, ref.SHA256)
}

// EnsureArtifact makes a pipeline's artifact present in its prefix of arts. An artifact the pipeline holds a
// reference to but not a file for came with a copy published through an imported stream (spec section 4.12): its
// file is in another pipeline's prefix, and the first use copies it into the pipeline's own, where the pipeline's
// retention and erasure find it. An artifact neither held nor found is an error wrapping fs.ErrNotExist.
func EnsureArtifact(ctx context.Context, st store.Store, arts artifacts.Store, pipeline string,
	ref store.ArtifactRef,
) error {
	if ok, err := arts.Exists(ctx, pipeline, ref.SHA256); err != nil || ok {
		return err
	}
	all, err := st.Artifacts(ctx, "")
	if err != nil {
		return err
	}
	held, sources := false, []string{}
	for _, a := range all {
		switch {
		case a.SHA256 != ref.SHA256:
		case a.Pipeline == pipeline:
			held = true
		default:
			sources = append(sources, a.Pipeline)
		}
	}
	if !held {
		return errNotExist(pipeline, ref)
	}
	for _, from := range sources {
		err := arts.Copy(ctx, from, pipeline, ref.SHA256)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		return err
	}
	return errNotExist(pipeline, ref)
}

// errNotExist is the error of an artifact found nowhere.
func errNotExist(pipeline string, ref store.ArtifactRef) error {
	return &fs.PathError{Op: "open", Path: "artifact " + pipeline + "/" + ref.SHA256, Err: fs.ErrNotExist}
}
