// Package artifacts is the artifact store (spec section 9.2): files content-addressed by SHA-256 under a prefix per
// pipeline, deduplicated by hash within it, in a local directory (Dir) or an S3-compatible bucket (package s3). The
// store holds contents only: which items reference an artifact, and how many, is the log store's artifacts table,
// counted in the Done unit.
//
// Dir keeps <dir>/<pipeline>/<sha256>. Put streams a file in through a temporary file in the same directory and
// renames it to its hash once complete, so a reader never sees a partial file and a crash leaves at most a temporary
// file behind.
package artifacts

import (
	"context"
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
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Ref is an artifact as items carry it.
type Ref = store.ArtifactRef

// Store is an artifact store.
type Store interface {
	// Put stores r's content for pipeline under its SHA-256 and returns its reference with name. Content already
	// stored is kept as is.
	Put(ctx context.Context, pipeline, name string, r io.Reader) (Ref, error)
	// Open opens the content of pipeline's artifact sha. A missing artifact is an error wrapping fs.ErrNotExist.
	Open(ctx context.Context, pipeline, sha string) (io.ReadSeekCloser, error)
	// Exists reports whether pipeline holds the content of artifact sha.
	Exists(ctx context.Context, pipeline, sha string) (bool, error)
	// Copy copies the content of artifact sha from pipeline from into pipeline to: an artifact of a copy published
	// through an imported stream, opened for the importer (spec section 9.2).
	Copy(ctx context.Context, from, to, sha string) error
}

// Presigner is a store nodes move contents to and from directly, through presigned URLs (spec section 9.2): the
// S3-compatible store.
type Presigner interface {
	// PresignGet returns a URL that reads pipeline's artifact sha until ttl passes.
	PresignGet(pipeline, sha string, ttl time.Duration) (string, error)
	// PresignPut returns a URL that writes pipeline's artifact sha with a PUT until ttl passes.
	PresignPut(pipeline, sha string, ttl time.Duration) (string, error)
}

// sniffLen is how many leading bytes http.DetectContentType looks at.
const sniffLen = 512

// pipelineName is the grammar of a prefix the store accepts for a pipeline: no separators, no dot names.
var pipelineName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// CheckPipeline checks that a pipeline name is a prefix the store accepts.
func CheckPipeline(pipeline string) error {
	if !pipelineName.MatchString(pipeline) {
		return fmt.Errorf("artifact store: %q is not a pipeline name", pipeline)
	}
	return nil
}

// Dir is a local artifact store rooted at a directory.
type Dir struct {
	dir string
}

var _ Store = (*Dir)(nil)

// New returns a store rooted at dir, created on the first Put.
func New(dir string) *Dir { return &Dir{dir: dir} }

// Put implements Store. The copy just read is dropped when the content is already stored.
func (s *Dir) Put(_ context.Context, pipeline, name string, r io.Reader) (Ref, error) {
	if !store.ValidArtifactName(name) {
		return Ref{}, fmt.Errorf("artifact name %q: want a file name without a path separator", name)
	}
	return s.put(pipeline, name, r)
}

func (s *Dir) put(pipeline, name string, r io.Reader) (Ref, error) {
	dir, err := s.pipelineDir(pipeline)
	if err != nil {
		return Ref{}, err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return Ref{}, fmt.Errorf("create artifact directory: %w", err)
	}
	ref, tmp, err := Spool(dir, name, r)
	if err != nil {
		return Ref{}, err
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // a no-op once renamed
	if err := tmp.Close(); err != nil {
		return Ref{}, fmt.Errorf("store artifact %s: %w", name, err)
	}
	dst := filepath.Join(dir, ref.SHA256)
	if _, err := os.Stat(dst); err == nil {
		return ref, nil
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return Ref{}, fmt.Errorf("store artifact %s: %w", name, err)
	}
	return ref, nil
}

// Open implements Store.
func (s *Dir) Open(_ context.Context, pipeline, sha string) (io.ReadSeekCloser, error) {
	path, err := s.path(pipeline, sha)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path) // #nosec G304 -- a validated pipeline name and hex digest
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("artifact %s of %s is not in the store: %w", sha, pipeline, err)
		}
		return nil, fmt.Errorf("open artifact: %w", err)
	}
	return f, nil
}

// Exists implements Store.
func (s *Dir) Exists(_ context.Context, pipeline, sha string) (bool, error) {
	path, err := s.path(pipeline, sha)
	if err != nil {
		return false, err
	}
	switch _, err := os.Stat(path); {
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("artifact %s of %s: %w", sha, pipeline, err)
	}
	return true, nil
}

// Copy implements Store.
func (s *Dir) Copy(ctx context.Context, from, to, sha string) error {
	src, err := s.Open(ctx, from, sha)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	ref, err := s.put(to, sha, src)
	if err == nil && ref.SHA256 != sha {
		err = fmt.Errorf("artifact %s of %s hashes to %s", sha, from, ref.SHA256)
	}
	return err
}

func (s *Dir) path(pipeline, sha string) (string, error) {
	dir, err := s.pipelineDir(pipeline)
	if err != nil {
		return "", err
	}
	if !store.ValidSHA256(sha) {
		return "", fmt.Errorf("open artifact: %q is not a hex SHA-256", sha)
	}
	return filepath.Join(dir, sha), nil
}

func (s *Dir) pipelineDir(pipeline string) (string, error) {
	if err := CheckPipeline(pipeline); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, pipeline), nil
}

// Spool writes r's content to a new temporary file in dir (the system's temporary directory when empty) while
// hashing it and sniffing its type, and returns its reference under name and the file, open at its start. The caller
// closes and removes the file.
func Spool(dir, name string, r io.Reader) (Ref, *os.File, error) {
	tmp, err := os.CreateTemp(dir, ".put-*")
	if err != nil {
		return Ref{}, nil, fmt.Errorf("store artifact %s: %w", name, err)
	}
	fail := func(err error) (Ref, *os.File, error) {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return Ref{}, nil, fmt.Errorf("store artifact %s: %w", name, err)
	}
	h := sha256.New()
	head := make([]byte, 0, sniffLen)
	n, err := io.Copy(io.MultiWriter(tmp, h, &prefix{buf: &head}), r)
	if err != nil {
		return fail(err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	return Ref{Name: name, SHA256: hex.EncodeToString(h.Sum(nil)), Size: n, Type: http.DetectContentType(head)}, tmp, nil
}

// prefix keeps the first sniffLen bytes written to it.
type prefix struct{ buf *[]byte }

func (p *prefix) Write(b []byte) (int, error) {
	if room := sniffLen - len(*p.buf); room > 0 {
		*p.buf = append(*p.buf, b[:min(room, len(b))]...)
	}
	return len(b), nil
}
