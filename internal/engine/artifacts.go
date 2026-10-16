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

// PutArtifact implements node.Artifacts over the engine's local artifact store.
func (e *Engine) PutArtifact(_ context.Context, pipeline, name string, r io.Reader) (store.ArtifactRef, error) {
	if e.opts.Artifacts == nil {
		return store.ArtifactRef{}, errNoArtifactStore
	}
	return e.opts.Artifacts.Put(pipeline, name, r)
}

// OpenArtifact implements node.Artifacts over the engine's local artifact store (OpenArtifact the function).
func (e *Engine) OpenArtifact(ctx context.Context, pipeline string, ref store.ArtifactRef) (io.ReadCloser, error) {
	if e.opts.Artifacts == nil {
		return nil, errNoArtifactStore
	}
	return OpenArtifact(ctx, e.st, e.opts.Artifacts, pipeline, ref)
}

// OpenArtifact opens a pipeline's artifact in arts. An artifact the pipeline holds a reference to but not a file for
// came with a copy published through an imported stream (spec section 4.12): its file is in another pipeline's
// directory, and the first open copies it into the pipeline's own, where the pipeline's retention and erasure find
// it. Nodes and the API open artifacts through it alike.
func OpenArtifact(ctx context.Context, st store.Store, arts *artifacts.Store, pipeline string, ref store.ArtifactRef,
) (io.ReadSeekCloser, error) {
	r, err := arts.Open(pipeline, ref.SHA256)
	if !errors.Is(err, fs.ErrNotExist) {
		return r, err
	}
	all, lerr := st.Artifacts(ctx, "")
	if lerr != nil {
		return nil, errors.Join(err, lerr)
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
		return nil, err
	}
	for _, from := range sources {
		src, oerr := arts.Open(from, ref.SHA256)
		if oerr != nil {
			continue
		}
		_, perr := arts.Put(pipeline, ref.Name, src)
		_ = src.Close()
		if perr != nil {
			return nil, perr
		}
		return arts.Open(pipeline, ref.SHA256)
	}
	return nil, err
}
