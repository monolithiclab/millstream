package engine

import (
	"context"
	"errors"
	"io"

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

// OpenArtifact implements node.Artifacts over the engine's local artifact store.
func (e *Engine) OpenArtifact(_ context.Context, pipeline string, ref store.ArtifactRef) (io.ReadCloser, error) {
	if e.opts.Artifacts == nil {
		return nil, errNoArtifactStore
	}
	return e.opts.Artifacts.Open(pipeline, ref.SHA256)
}
