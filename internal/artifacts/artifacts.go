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
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
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
	// PresignPut returns a URL that writes pipeline's artifact sha, of size bytes, with a PUT until ttl passes. The
	// URL is bound to the content: the PUT sends a Content-Length of size and ChecksumHeader at ChecksumSHA256(sha),
	// and the store refuses any other length or body.
	PresignPut(pipeline, sha string, size int64, ttl time.Duration) (string, error)
}

// ChecksumHeader is the header a PUT to a presigned URL carries the content's SHA-256 in, which the URL signs.
const ChecksumHeader = "X-Amz-Checksum-Sha256"

// ChecksumSHA256 is ChecksumHeader's value for the valid hex SHA-256 sha: the base64 of its 32 bytes.
func ChecksumSHA256(sha string) string {
	b, _ := hex.DecodeString(sha)
	return base64.StdEncoding.EncodeToString(b)
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
		// Stored again: the sweep's grace counts from now, so it never deletes a file a delivery in flight is about
		// to reference while its entry is unreferenced.
		if err := touch(dst); err != nil {
			return Ref{}, fmt.Errorf("store artifact %s: %w", name, err)
		}
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
		_ = s.Remove(ctx, to, ref.SHA256)
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
	tmp, err := os.CreateTemp(dir, tempPrefix+"*")
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

// File is a file of an artifact store: an artifact, by its SHA-256, or a temporary file a Put left behind.
type File struct {
	Pipeline string
	Name     string
	ModTime  time.Time
}

// Files is what the artifact sweep needs of an artifact store (spec section 9.3): the local directory implements it,
// and so does the S3-compatible store (package s3).
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

// Toucher is a store that makes an artifact stored now without its content, as storing the content again would, so
// that the sweep's grace counts from then: what the engine does for an output a remote node finds stored already and
// does not send. The local directory and the S3-compatible store implement it.
type Toucher interface {
	// Touch marks pipeline's artifact sha stored now; a missing one is an error wrapping fs.ErrNotExist.
	Touch(ctx context.Context, pipeline, sha string) error
}

var (
	_ Files   = (*Dir)(nil)
	_ Toucher = (*Dir)(nil)
)

// tempPrefix starts the name of a file Put is writing.
const tempPrefix = ".put-"

// List implements Files.
func (s *Dir) List(context.Context) ([]File, error) {
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
func (s *Dir) ModTime(_ context.Context, pipeline, sha string) (time.Time, error) {
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

// Touch implements Toucher.
func (s *Dir) Touch(_ context.Context, pipeline, sha string) error {
	path, err := s.path(pipeline, sha)
	if err != nil {
		return err
	}
	if err := touch(path); err != nil {
		return fmt.Errorf("artifact %s of %s: %w", sha, pipeline, err)
	}
	return nil
}

// touch sets a file's times to now.
func touch(path string) error {
	now := time.Now()
	return os.Chtimes(path, now, now)
}

// Remove implements Files.
func (s *Dir) Remove(_ context.Context, pipeline, name string) error {
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
