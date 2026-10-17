// Package s3 is the artifact store on an S3-compatible bucket (spec section 9.2): each artifact under
// <prefix>/<pipeline>/<sha256>, addressed path-style (<endpoint>/<bucket>/<key>, which AWS, MinIO, Scaleway, Ceph and
// the other compatible stores serve), every request signed with AWS Signature Version 4 (sigv4.go, written for the
// few requests the store makes rather than pulling in an SDK). Remote nodes move contents directly through
// presigned GET and PUT URLs the engine issues for the leases they hold; the engine itself reads and writes through
// the same store.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/store"
)

// DefaultRegion is the region signed for when the configuration names none: what MinIO and most compatible stores
// accept.
const DefaultRegion = "us-east-1"

// Config is a bucket and its credentials.
type Config struct {
	Bucket string
	// Endpoint is the store's URL; empty means AWS's endpoint for Region.
	Endpoint string
	Region   string
	// Prefix is the key prefix under which artifacts are kept.
	Prefix          string
	AccessKeyID     string
	SecretAccessKey string
	// HTTP makes the requests; nil means a client with a 5-minute timeout.
	HTTP *http.Client
	// Now is the signing clock; nil means time.Now.
	Now func() time.Time
	// TmpDir is where Put spools a content to hash it before sending; empty means the system's temporary directory.
	TmpDir string
}

// Store is an artifact store on a bucket.
type Store struct {
	cfg      Config
	endpoint *url.URL
	sign     signer
}

var (
	_ artifacts.Store     = (*Store)(nil)
	_ artifacts.Presigner = (*Store)(nil)
	_ artifacts.Files     = (*Store)(nil)
	_ artifacts.Toucher   = (*Store)(nil)
)

// New returns the store of cfg.
func New(cfg Config) (*Store, error) {
	if cfg.Bucket == "" || cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errors.New("artifacts.s3: a bucket and credentials are required")
	}
	if cfg.Region == "" {
		cfg.Region = DefaultRegion
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = "https://s3." + cfg.Region + ".amazonaws.com"
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, fmt.Errorf("artifacts.s3.endpoint %q: want an http or https URL", cfg.Endpoint)
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 5 * time.Minute}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	cfg.Prefix = strings.Trim(cfg.Prefix, "/")
	return &Store{cfg: cfg, endpoint: u, sign: signer{accessKey: cfg.AccessKeyID, secretKey: cfg.SecretAccessKey,
		region: cfg.Region}}, nil
}

// key is the object key of pipeline's artifact sha.
func (s *Store) key(pipeline, sha string) (string, error) {
	if err := artifacts.CheckPipeline(pipeline); err != nil {
		return "", err
	}
	if !store.ValidSHA256(sha) {
		return "", fmt.Errorf("artifact %q is not a hex SHA-256", sha)
	}
	k := pipeline + "/" + sha
	if s.cfg.Prefix != "" {
		k = s.cfg.Prefix + "/" + k
	}
	return k, nil
}

// objectURL is the path-style URL of key.
func (s *Store) objectURL(key string) *url.URL {
	u := *s.endpoint
	u.Path = strings.TrimSuffix(u.Path, "/") + "/" + s.cfg.Bucket + "/" + key
	u.RawPath = ""
	return &u
}

// do makes a signed request of key with body (of length size, hashed payloadHash) and the extra headers.
func (s *Store) do(ctx context.Context, method, key string, body io.Reader, size int64, payloadHash string,
	header http.Header,
) (*http.Response, error) {
	return s.doURL(ctx, method, s.objectURL(key), body, size, payloadHash, header)
}

// doURL makes a signed request of u.
func (s *Store) doURL(ctx context.Context, method string, u *url.URL, body io.Reader, size int64, payloadHash string,
	header http.Header,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	if body != nil {
		req.ContentLength = size
	}
	s.sign.sign(req, payloadHash, s.cfg.Now())
	return s.cfg.HTTP.Do(req)
}

// statusErr is a failed request's error; a 404 wraps fs.ErrNotExist.
func statusErr(op, key string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	err := fmt.Errorf("s3 %s %s: %s: %s", op, key, resp.Status, strings.TrimSpace(string(body)))
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %w", err, fs.ErrNotExist)
	}
	return err
}

// Put implements artifacts.Store: the content is spooled to a temporary file to hash it, then sent with its hash
// signed, so the store checks what it received.
func (s *Store) Put(ctx context.Context, pipeline, name string, r io.Reader) (store.ArtifactRef, error) {
	if !store.ValidArtifactName(name) {
		return store.ArtifactRef{}, fmt.Errorf("artifact name %q: want a file name without a path separator", name)
	}
	if err := artifacts.CheckPipeline(pipeline); err != nil {
		return store.ArtifactRef{}, err
	}
	ref, f, err := artifacts.Spool(s.cfg.TmpDir, name, r)
	if err != nil {
		return store.ArtifactRef{}, err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	key, _ := s.key(pipeline, ref.SHA256)
	// Stored already: touched, so the artifact sweep's grace counts from now, as the local directory's Put does.
	switch err := s.touch(ctx, key); {
	case err == nil:
		return ref, nil
	case !errors.Is(err, fs.ErrNotExist):
		return store.ArtifactRef{}, fmt.Errorf("store artifact %s: %w", name, err)
	}
	resp, err := s.do(ctx, http.MethodPut, key, f, ref.Size, ref.SHA256,
		http.Header{"Content-Type": {ref.Type}})
	if err != nil {
		return store.ArtifactRef{}, fmt.Errorf("store artifact %s: %w", name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return store.ArtifactRef{}, statusErr("put", key, resp)
	}
	return ref, nil
}

// head returns the response headers of key, an error wrapping fs.ErrNotExist when it is absent.
func (s *Store) head(ctx context.Context, key string) (*http.Response, error) {
	resp, err := s.do(ctx, http.MethodHead, key, nil, 0, emptySHA256, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, statusErr("head", key, resp)
	}
	return resp, nil
}

// Exists implements artifacts.Store.
func (s *Store) Exists(ctx context.Context, pipeline, sha string) (bool, error) {
	key, err := s.key(pipeline, sha)
	if err != nil {
		return false, err
	}
	switch _, err := s.head(ctx, key); {
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	case err != nil:
		return false, err
	}
	return true, nil
}

// Open implements artifacts.Store: a reader that fetches the object from its current offset, so seeking (a ranged
// download through the API) refetches from there.
func (s *Store) Open(ctx context.Context, pipeline, sha string) (io.ReadSeekCloser, error) {
	key, err := s.key(pipeline, sha)
	if err != nil {
		return nil, err
	}
	resp, err := s.head(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("open artifact %s of %s: %w", sha, pipeline, err)
	}
	return &object{s: s, ctx: ctx, key: key, size: resp.ContentLength}, nil
}

// Copy implements artifacts.Store with a server-side copy.
func (s *Store) Copy(ctx context.Context, from, to, sha string) error {
	src, err := s.key(from, sha)
	if err != nil {
		return err
	}
	dst, err := s.key(to, sha)
	if err != nil {
		return err
	}
	return s.copyObject(ctx, src, dst, nil)
}

// copyObject copies the object src to dst on the server, with the extra headers.
func (s *Store) copyObject(ctx context.Context, src, dst string, header http.Header) error {
	h := http.Header{"X-Amz-Copy-Source": {"/" + s.cfg.Bucket + "/" + uriEncode(src, false)}}
	maps.Copy(h, header)
	resp, err := s.do(ctx, http.MethodPut, dst, nil, 0, emptySHA256, h)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return statusErr("copy", src, resp)
	}
	// A copy can fail after its 200 status: the error is then in the body.
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if strings.Contains(string(body), "<Error>") {
		return fmt.Errorf("s3 copy %s: %s", src, body)
	}
	return nil
}

// PresignGet implements artifacts.Presigner.
func (s *Store) PresignGet(pipeline, sha string, ttl time.Duration) (string, error) {
	key, err := s.key(pipeline, sha)
	if err != nil {
		return "", err
	}
	return s.sign.presign(http.MethodGet, s.objectURL(key), nil, ttl, s.cfg.Now()), nil
}

// PresignPut implements artifacts.Presigner: the URL signs the Content-Length and the x-amz-checksum-sha256 of the
// content, so the server refuses another length, and a body whose hash is not sha.
func (s *Store) PresignPut(pipeline, sha string, size int64, ttl time.Duration) (string, error) {
	key, err := s.key(pipeline, sha)
	if err != nil {
		return "", err
	}
	header := map[string]string{"content-length": strconv.FormatInt(size, 10),
		strings.ToLower(artifacts.ChecksumHeader): artifacts.ChecksumSHA256(sha)}
	return s.sign.presign(http.MethodPut, s.objectURL(key), header, ttl, s.cfg.Now()), nil
}

// object reads an object from its offset with ranged GETs.
type object struct {
	s    *Store
	ctx  context.Context
	key  string
	size int64
	off  int64
	body io.ReadCloser
}

func (o *object) Read(p []byte) (int, error) {
	if o.off >= o.size {
		return 0, io.EOF
	}
	if o.body == nil {
		resp, err := o.s.do(o.ctx, http.MethodGet, o.key, nil, 0, emptySHA256,
			http.Header{"Range": {"bytes=" + strconv.FormatInt(o.off, 10) + "-"}})
		if err != nil {
			return 0, err
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			defer func() { _ = resp.Body.Close() }()
			return 0, statusErr("get", o.key, resp)
		}
		o.body = resp.Body
	}
	n, err := o.body.Read(p)
	o.off += int64(n)
	if errors.Is(err, io.EOF) && o.off < o.size {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

func (o *object) Seek(offset int64, whence int) (int64, error) {
	var off int64
	switch whence {
	case io.SeekStart:
		off = offset
	case io.SeekCurrent:
		off = o.off + offset
	case io.SeekEnd:
		off = o.size + offset
	default:
		return 0, fmt.Errorf("seek: whence %d", whence)
	}
	if off < 0 {
		return 0, errors.New("seek: negative offset")
	}
	if off != o.off && o.body != nil {
		_ = o.body.Close()
		o.body = nil
	}
	o.off = off
	return off, nil
}

func (o *object) Close() error {
	if o.body != nil {
		return o.body.Close()
	}
	return nil
}
