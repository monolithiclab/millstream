// Package artifacts is the local artifact store (spec section 9.2): files content-addressed by SHA-256 under
// <dir>/<pipeline>/<sha256>, deduplicated by hash within a pipeline. Put streams a file in through a temporary file
// in the same directory and renames it to its hash once complete, so a reader never sees a partial file and a crash
// leaves at most a temporary file behind. The store holds contents only: which items reference an artifact, and how
// many, is the log store's artifacts table, counted in the Done unit.
package artifacts

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"

	"github.com/monolithiclab/millstream/internal/store"
)

// Ref is an artifact as items carry it.
type Ref = store.ArtifactRef

// sniffLen is how many leading bytes http.DetectContentType looks at.
const sniffLen = 512

// pipelineName is the grammar of a directory name the store accepts for a pipeline: no separators, no dot names.
var pipelineName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// Store is a local artifact store rooted at a directory.
type Store struct {
	dir string
}

// New returns a store rooted at dir, created on the first Put.
func New(dir string) *Store { return &Store{dir: dir} }

// Put stores r's content for pipeline under its SHA-256 and returns its reference with name. Content already
// stored is kept as is: the copy just read is dropped.
func (s *Store) Put(pipeline, name string, r io.Reader) (Ref, error) {
	if !store.ValidArtifactName(name) {
		return Ref{}, fmt.Errorf("artifact name %q: want a file name without a path separator", name)
	}
	dir, err := s.pipelineDir(pipeline)
	if err != nil {
		return Ref{}, err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return Ref{}, fmt.Errorf("create artifact directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".put-*")
	if err != nil {
		return Ref{}, fmt.Errorf("store artifact %s: %w", name, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // a no-op once renamed
	h := sha256.New()
	head := make([]byte, 0, sniffLen)
	n, err := io.Copy(io.MultiWriter(tmp, h, &prefix{buf: &head}), r)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return Ref{}, fmt.Errorf("store artifact %s: %w", name, err)
	}
	ref := Ref{Name: name, SHA256: hex.EncodeToString(h.Sum(nil)), Size: n, Type: http.DetectContentType(head)}
	dst := filepath.Join(dir, ref.SHA256)
	if _, err := os.Stat(dst); err == nil {
		return ref, nil
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return Ref{}, fmt.Errorf("store artifact %s: %w", name, err)
	}
	return ref, nil
}

// Open opens the content of pipeline's artifact sha. A missing artifact is an error wrapping fs.ErrNotExist.
func (s *Store) Open(pipeline, sha string) (io.ReadSeekCloser, error) {
	dir, err := s.pipelineDir(pipeline)
	if err != nil {
		return nil, err
	}
	if !store.ValidSHA256(sha) {
		return nil, fmt.Errorf("open artifact: %q is not a hex SHA-256", sha)
	}
	f, err := os.Open(filepath.Join(dir, sha)) // #nosec G304 -- a validated pipeline name and hex digest
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("artifact %s of %s is not in the store: %w", sha, pipeline, err)
		}
		return nil, fmt.Errorf("open artifact: %w", err)
	}
	return f, nil
}

func (s *Store) pipelineDir(pipeline string) (string, error) {
	if !pipelineName.MatchString(pipeline) {
		return "", fmt.Errorf("artifact store: %q is not a pipeline name", pipeline)
	}
	return filepath.Join(s.dir, pipeline), nil
}

// prefix keeps the first sniffLen bytes written to it.
type prefix struct{ buf *[]byte }

func (p *prefix) Write(b []byte) (int, error) {
	if room := sniffLen - len(*p.buf); room > 0 {
		*p.buf = append(*p.buf, b[:min(room, len(b))]...)
	}
	return len(b), nil
}
