// Package artifacts is the local artifact store (spec section 9.2): files content-addressed by SHA-256 under
// <dir>/<pipeline>/<sha256>, deduplicated by hash within a pipeline. Put streams a file in through a temporary file
// in the same directory and renames it to its hash once complete, so a reader never sees a partial file and a crash
// leaves at most a temporary file behind. The store holds contents only: which items reference an artifact, and how
// many, is the log store's artifacts table, counted in the Done unit.
package artifacts

import (
	"compress/gzip"
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
	"strings"
	"time"

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
	tmp, err := os.CreateTemp(dir, tempPrefix+"*")
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
		// Stored again: the sweep's grace counts from now, so it never deletes a file a delivery in flight is about
		// to reference while its entry is unreferenced.
		now := time.Now()
		if err := os.Chtimes(dst, now, now); err != nil {
			return Ref{}, fmt.Errorf("store artifact %s: %w", name, err)
		}
		return ref, nil
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return Ref{}, fmt.Errorf("store artifact %s: %w", name, err)
	}
	return ref, nil
}

// Open opens the content of pipeline's artifact sha. A missing artifact is an error wrapping fs.ErrNotExist.
func (s *Store) Open(pipeline, sha string) (io.ReadSeekCloser, error) {
	path, err := s.path(pipeline, sha)
	if err != nil {
		return nil, fmt.Errorf("open %w", err)
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

// File is a file of an artifact store: an artifact, by its SHA-256, or a temporary file a Put left behind.
type File struct {
	Pipeline string
	Name     string
	ModTime  time.Time
}

// Files is what the artifact sweep needs of an artifact store (spec section 9.3): the local directory implements it,
// and so must an S3-compatible store.
type Files interface {
	// List returns every file of the store.
	List(ctx context.Context) ([]File, error)
	// ModTime returns when an artifact was last stored, wrapping fs.ErrNotExist when it is not there.
	ModTime(ctx context.Context, pipeline, sha string) (time.Time, error)
	// Copy copies an artifact of one pipeline into another's prefix.
	Copy(ctx context.Context, from, to, sha string) error
	// Remove removes a file named as List names it; a missing one is no error.
	Remove(ctx context.Context, pipeline, name string) error
}

// tempPrefix starts the name of a file Put is writing.
const tempPrefix = ".put-"

// List implements Files.
func (s *Store) List(context.Context) ([]File, error) {
	pipelines, err := os.ReadDir(s.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list artifacts: %w", err)
	}
	var out []File
	for _, p := range pipelines {
		if !p.IsDir() || !pipelineName.MatchString(p.Name()) {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(s.dir, p.Name()))
		if err != nil {
			return nil, fmt.Errorf("list artifacts of %s: %w", p.Name(), err)
		}
		for _, e := range entries {
			if !e.Type().IsRegular() || !validFileName(e.Name()) {
				continue
			}
			info, err := e.Info()
			if errors.Is(err, fs.ErrNotExist) {
				continue // removed meanwhile
			}
			if err != nil {
				return nil, fmt.Errorf("list artifacts of %s: %w", p.Name(), err)
			}
			out = append(out, File{Pipeline: p.Name(), Name: e.Name(), ModTime: info.ModTime()})
		}
	}
	return out, nil
}

// validFileName reports whether name is an artifact's hash or a temporary file of Put.
func validFileName(name string) bool {
	return store.ValidSHA256(name) || strings.HasPrefix(name, tempPrefix)
}

// ModTime implements Files.
func (s *Store) ModTime(_ context.Context, pipeline, sha string) (time.Time, error) {
	path, err := s.path(pipeline, sha)
	if err != nil {
		return time.Time{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, fmt.Errorf("artifact %s of %s: %w", sha, pipeline, err)
	}
	return info.ModTime(), nil
}

// Copy implements Files.
func (s *Store) Copy(ctx context.Context, from, to, sha string) error {
	r, err := s.Open(from, sha)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()
	ref, err := s.Put(to, sha, r)
	if err == nil && ref.SHA256 != sha {
		err = fmt.Errorf("artifact %s of %s hashes to %s", sha, from, ref.SHA256)
		_ = s.Remove(ctx, to, ref.SHA256)
	}
	return err
}

// Remove implements Files.
func (s *Store) Remove(_ context.Context, pipeline, name string) error {
	if !validFileName(name) {
		return fmt.Errorf("remove artifact: %q is not an artifact file", name)
	}
	dir, err := s.pipelineDir(pipeline)
	if err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove artifact: %w", err)
	}
	return nil
}

// path is where an artifact of a pipeline is stored.
func (s *Store) path(pipeline, sha string) (string, error) {
	dir, err := s.pipelineDir(pipeline)
	if err != nil {
		return "", err
	}
	if !store.ValidSHA256(sha) {
		return "", fmt.Errorf("artifact: %q is not a hex SHA-256", sha)
	}
	return filepath.Join(dir, sha), nil
}

// gzipType is the type http.DetectContentType sniffs from gzip data.
const gzipType = "application/x-gzip"

// Compressed reports whether an artifact holds gzip data under a name that does not say so: a transcript, stored
// compressed and served decompressed (spec section 7.2), its type then read from its name.
func Compressed(ref store.ArtifactRef) bool {
	base, _, _ := strings.Cut(ref.Type, ";")
	if strings.TrimSpace(base) != gzipType {
		return false
	}
	switch strings.ToLower(filepath.Ext(ref.Name)) {
	case ".gz", ".tgz", ".gzip":
		return false
	}
	return true
}

// Decompressed returns r, the content of ref, decompressed when ref is Compressed.
func Decompressed(ref store.ArtifactRef, r io.Reader) (io.Reader, error) {
	if !Compressed(ref) {
		return r, nil
	}
	zr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("artifact %s: %w", ref.Name, err)
	}
	return zr, nil
}
