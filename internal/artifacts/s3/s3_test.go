package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// fakeS3 is an S3-compatible bucket in memory, path-style: /<bucket>/<key>.
type fakeS3 struct {
	t        *testing.T
	bucket   string
	objects  map[string][]byte
	modified map[string]time.Time // each object's Last-Modified: clock when it was last written
	clock    time.Time
	puts     int  // uploads, copies left out
	failPut  bool // a PUT answers 500
	failList bool // a list answers 500
	copyBody string
}

// write stores an object at the fake's clock.
func (f *fakeS3) write(key string, b []byte) {
	f.objects[key] = b
	if f.modified == nil {
		f.modified = map[string]time.Time{}
	}
	f.modified[key] = f.clock
}

// list answers a ListObjectsV2 request, two keys a page.
func (f *fakeS3) list(w http.ResponseWriter, q url.Values) {
	if f.failList || q.Get("list-type") != "2" {
		http.Error(w, "<Error>InternalError</Error>", http.StatusInternalServerError)
		return
	}
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, q.Get("prefix")) && k > q.Get("continuation-token") {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	var b strings.Builder
	b.WriteString("<ListBucketResult>")
	if len(keys) > 2 {
		keys = keys[:2]
		b.WriteString("<IsTruncated>true</IsTruncated><NextContinuationToken>" + keys[1] + "</NextContinuationToken>")
	}
	for _, k := range keys {
		b.WriteString("<Contents><Key>" + k + "</Key><LastModified>" + f.modified[k].Format(time.RFC3339Nano) +
			"</LastModified></Contents>")
	}
	b.WriteString("</ListBucketResult>")
	_, _ = io.WriteString(w, b.String())
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if auth := r.Header.Get("Authorization"); !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		f.t.Errorf("%s %s: Authorization = %q", r.Method, r.URL.Path, auth)
	}
	if r.URL.Path == "/"+f.bucket && r.Method == http.MethodGet {
		f.list(w, r.URL.Query())
		return
	}
	key, ok := strings.CutPrefix(r.URL.Path, "/"+f.bucket+"/")
	if !ok {
		http.Error(w, "no such bucket", http.StatusNotFound)
		return
	}
	switch r.Method {
	case http.MethodPut:
		if f.failPut {
			http.Error(w, "<Error>InternalError</Error>", http.StatusInternalServerError)
			return
		}
		if src := r.Header.Get("X-Amz-Copy-Source"); src != "" {
			src, _ = url.PathUnescape(src)
			b, ok := f.objects[strings.TrimPrefix(src, "/"+f.bucket+"/")]
			if !ok {
				http.Error(w, "<Error>NoSuchKey</Error>", http.StatusNotFound)
				return
			}
			if src == "/"+f.bucket+"/"+key && r.Header.Get("X-Amz-Metadata-Directive") != "REPLACE" {
				http.Error(w, "<Error>InvalidRequest</Error>", http.StatusBadRequest) // as S3: a copy must change something
				return
			}
			f.write(key, b)
			_, _ = io.WriteString(w, f.copyBody)
			return
		}
		f.puts++
		b, _ := io.ReadAll(r.Body)
		f.write(key, b)
	case http.MethodHead:
		b, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		w.Header().Set("Content-Type", "text/plain")
		if mt := f.modified[key]; !mt.IsZero() { // zeroed by a test: no Last-Modified
			w.Header().Set("Last-Modified", mt.Format(http.TimeFormat))
		}
	case http.MethodDelete:
		if f.failPut {
			http.Error(w, "<Error>InternalError</Error>", http.StatusInternalServerError)
			return
		}
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		b, ok := f.objects[key]
		if !ok {
			http.Error(w, "<Error>NoSuchKey</Error>", http.StatusNotFound)
			return
		}
		from, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(r.Header.Get("Range"), "bytes="), "-"))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(b[min(from, len(b)):])
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handlerTransport serves requests with a handler, in-process.
type handlerTransport struct {
	h    http.Handler
	down bool
}

func (rt *handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if rt.down {
		return nil, errors.New("connection refused")
	}
	rec := httptest.NewRecorder()
	rt.h.ServeHTTP(rec, r)
	return rec.Result(), nil
}

func hexSum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func TestStore(t *testing.T) {
	// New's validation and defaults.
	for _, cfg := range []Config{
		{AccessKeyID: "k", SecretAccessKey: "s"},
		{Bucket: "b", SecretAccessKey: "s"},
		{Bucket: "b", AccessKeyID: "k"},
		{Bucket: "b", AccessKeyID: "k", SecretAccessKey: "s", Endpoint: "ftp://host"},
		{Bucket: "b", AccessKeyID: "k", SecretAccessKey: "s", Endpoint: "http://"},
		{Bucket: "b", AccessKeyID: "k", SecretAccessKey: "s", Endpoint: "://bad"},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("New(%+v): no error", cfg)
		}
	}
	def, err := New(Config{Bucket: "b", AccessKeyID: "k", SecretAccessKey: "s", Prefix: "/p/"})
	if err != nil {
		t.Fatal(err)
	}
	if def.endpoint.String() != "https://s3.us-east-1.amazonaws.com" || def.sign.region != DefaultRegion ||
		def.cfg.HTTP == nil || def.cfg.Now == nil || def.cfg.Prefix != "p" {
		t.Errorf("defaults: endpoint %s, region %s, prefix %q", def.endpoint, def.sign.region, def.cfg.Prefix)
	}

	fake := &fakeS3{t: t, bucket: "bucket", objects: map[string][]byte{}}
	rt := &handlerTransport{h: fake}
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	cfg := Config{
		Bucket: "bucket", Endpoint: "http://s3.test", AccessKeyID: "AK", SecretAccessKey: "SK", Prefix: "pre",
		HTTP: &http.Client{Transport: rt}, Now: func() time.Time { return now }, TmpDir: testdir.New(t),
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	const content = "margin 18%\n"
	sha := hexSum(content)
	key := "pre/listings/" + sha

	// Put stores the content once: a second Put finds it and sends nothing.
	ref, err := s.Put(ctx, "listings", "report.txt", strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if ref.SHA256 != sha || ref.Size != int64(len(content)) || ref.Name != "report.txt" ||
		string(fake.objects[key]) != content || fake.puts != 1 {
		t.Fatalf("Put: ref %+v, objects %v, %d PUTs", ref, fake.objects, fake.puts)
	}
	if _, err := s.Put(ctx, "listings", "again.txt", strings.NewReader(content)); err != nil || fake.puts != 1 {
		t.Fatalf("Put of a stored content: %v, %d PUTs", err, fake.puts)
	}

	// Exists.
	if ok, err := s.Exists(ctx, "listings", sha); !ok || err != nil {
		t.Errorf("Exists = %v, %v", ok, err)
	}
	if ok, err := s.Exists(ctx, "listings", hexSum("absent")); ok || err != nil {
		t.Errorf("Exists of an absent artifact = %v, %v", ok, err)
	}

	// Open, Read and Seek.
	r, err := s.Open(ctx, "listings", sha)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	if n, err := io.ReadFull(r, buf); n != 3 || err != nil || string(buf) != "mar" {
		t.Fatalf("Read = %d %q %v", n, buf, err)
	}
	seeks := []struct {
		offset int64
		whence int
		want   int64
		rest   string
	}{
		{2, io.SeekCurrent, 5, "n 18%\n"},
		{-4, io.SeekEnd, int64(len(content)) - 4, "18%\n"},
		{0, io.SeekStart, 0, content},
		{int64(len(content)), io.SeekStart, int64(len(content)), ""},
	}
	for _, sk := range seeks {
		off, err := r.Seek(sk.offset, sk.whence)
		if err != nil || off != sk.want {
			t.Fatalf("Seek(%d, %d) = %d, %v; want %d", sk.offset, sk.whence, off, err, sk.want)
		}
		if got, err := io.ReadAll(r); err != nil || string(got) != sk.rest {
			t.Fatalf("after Seek(%d, %d): %q, %v; want %q", sk.offset, sk.whence, got, err, sk.rest)
		}
	}
	if _, err := r.Seek(-1, io.SeekStart); err == nil {
		t.Error("Seek to a negative offset: no error")
	}
	if _, err := r.Seek(0, 7); err == nil {
		t.Error("Seek with a bad whence: no error")
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// An object shortened under a reader is an unexpected EOF; one deleted, a missing artifact.
	r, err = s.Open(ctx, "listings", sha)
	if err != nil {
		t.Fatal(err)
	}
	fake.objects[key] = []byte("mar")
	if _, err := io.ReadAll(r); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("Read of a shortened object: %v", err)
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	delete(fake.objects, key)
	if _, err := r.Read(buf); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Read of a deleted object: %v", err)
	}
	_ = r.Close()
	fake.objects[key] = []byte(content)
	if _, err := s.Open(ctx, "listings", hexSum("absent")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Open of a missing artifact: %v, want fs.ErrNotExist", err)
	}

	// Copy: server-side, a missing source is fs.ErrNotExist, and an error in a 200 body is an error.
	if err := s.Copy(ctx, "listings", "importer", sha); err != nil || string(fake.objects["pre/importer/"+sha]) != content {
		t.Fatalf("Copy: %v, objects %v", err, fake.objects)
	}
	if err := s.Copy(ctx, "listings", "importer", hexSum("absent")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Copy of a missing artifact: %v, want fs.ErrNotExist", err)
	}
	fake.copyBody = "<Error><Code>InternalError</Code></Error>"
	if err := s.Copy(ctx, "listings", "importer", sha); err == nil || !strings.Contains(err.Error(), "InternalError") {
		t.Errorf("Copy failing in its body: %v", err)
	}
	fake.copyBody = ""

	// Presigned URLs.
	for name, presign := range map[string]func(string, string, time.Duration) (string, error){
		"PresignGet": s.PresignGet, "PresignPut": s.PresignPut,
	} {
		u, err := presign("listings", sha, time.Hour)
		if err != nil || !strings.Contains(u, "X-Amz-Signature=") || !strings.HasPrefix(u, "http://s3.test/bucket/"+key+"?") {
			t.Errorf("%s = %q, %v", name, u, err)
		}
		if _, err := presign("a/b", sha, time.Hour); err == nil {
			t.Errorf("%s of an invalid pipeline: no error", name)
		}
	}

	// Invalid names and digests.
	invalid := map[string]func() error{
		"Put name":        func() error { _, err := s.Put(ctx, "listings", "a/b", strings.NewReader("x")); return err },
		"Put pipeline":    func() error { _, err := s.Put(ctx, "..", "x", strings.NewReader("x")); return err },
		"Exists pipeline": func() error { _, err := s.Exists(ctx, "a/b", sha); return err },
		"Exists sha":      func() error { _, err := s.Exists(ctx, "listings", "nothex"); return err },
		"Open sha":        func() error { _, err := s.Open(ctx, "listings", "nothex"); return err },
		"Copy from":       func() error { return s.Copy(ctx, ".x", "importer", sha) },
		"Copy to":         func() error { return s.Copy(ctx, "listings", ".x", sha) },
		"PresignPut sha":  func() error { _, err := s.PresignPut("listings", strings.ToUpper(sha), time.Hour); return err },
	}
	for name, call := range invalid {
		if err := call(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}

	// Failures: a PUT answering 500, an unreachable store, a temporary directory that does not exist.
	fake.failPut = true
	if _, err := s.Put(ctx, "listings", "new.txt", strings.NewReader("new")); err == nil ||
		!strings.Contains(err.Error(), "500") {
		t.Errorf("Put answered 500: %v", err)
	}
	if err := s.Copy(ctx, "listings", "other", sha); err == nil {
		t.Error("Copy answered 500: no error")
	}
	fake.failPut = false
	rt.down = true
	if _, err := s.Exists(ctx, "listings", sha); err == nil {
		t.Error("Exists of an unreachable store: no error")
	}
	if _, err := s.Put(ctx, "listings", "x", strings.NewReader("x")); err == nil {
		t.Error("Put to an unreachable store: no error")
	}
	if err := s.Copy(ctx, "listings", "other", sha); err == nil {
		t.Error("Copy on an unreachable store: no error")
	}
	if _, err := s.Open(ctx, "listings", sha); err == nil {
		t.Error("Open on an unreachable store: no error")
	}
	rt.down = false
	cfg.TmpDir = filepath.Join(cfg.TmpDir, "missing")
	noTmp, _ := New(cfg)
	if _, err := noTmp.Put(ctx, "listings", "x", strings.NewReader("x")); err == nil {
		t.Error("Put without its temporary directory: no error")
	}
}

// The artifact sweep's operations on a bucket (artifacts.Files): a paged listing of the store's keys only, an
// object's Last-Modified, a touch that rewrites it (also when Put finds the content stored), and deletes, a missing
// object being no error.
func TestFiles(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	fake := &fakeS3{t: t, bucket: "bucket", objects: map[string][]byte{}, clock: t0}
	rt := &handlerTransport{h: fake}
	s, err := New(Config{Bucket: "bucket", Endpoint: "http://s3.test", AccessKeyID: "AK", SecretAccessKey: "SK",
		Prefix: "pre", HTTP: &http.Client{Transport: rt}, TmpDir: testdir.New(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	a, b, c := hexSum("a"), hexSum("b"), hexSum("c")
	for _, k := range []string{"pre/src/" + a, "pre/src/" + b, "pre/deal/" + c, "pre/src/notes.txt", "pre/x/y/" + a,
		"other/src/" + a} {
		fake.write(k, []byte("x"))
	}
	files, err := s.List(ctx)
	want := []artifacts.File{{Pipeline: "deal", Name: c, ModTime: t0}, {Pipeline: "src", Name: a, ModTime: t0},
		{Pipeline: "src", Name: b, ModTime: t0}}
	byName := func(x, y artifacts.File) int { return strings.Compare(x.Pipeline+x.Name, y.Pipeline+y.Name) }
	slices.SortFunc(files, byName)
	slices.SortFunc(want, byName)
	if err != nil || !slices.Equal(files, want) {
		t.Fatalf("List = %+v, %v", files, err)
	}

	if mt, err := s.ModTime(ctx, "src", a); err != nil || !mt.Equal(t0) {
		t.Fatalf("ModTime = %v, %v", mt, err)
	}
	t1 := t0.Add(48 * time.Hour)
	fake.clock = t1
	if err := s.Touch(ctx, "src", a); err != nil {
		t.Fatal(err)
	}
	if mt, _ := s.ModTime(ctx, "src", a); !mt.Equal(t1) || string(fake.objects["pre/src/"+a]) != "x" {
		t.Fatalf("ModTime after Touch = %v", mt)
	}
	fake.write("pre/src/"+hexSum("stored"), []byte("stored"))
	fake.clock = t1.Add(time.Hour)
	if _, err := s.Put(ctx, "src", "again.txt", strings.NewReader("stored")); err != nil || fake.puts != 0 {
		t.Fatalf("Put of a stored content: %v, %d uploads", err, fake.puts)
	}
	if mt, _ := s.ModTime(ctx, "src", hexSum("stored")); !mt.Equal(fake.clock) {
		t.Fatalf("Put of a stored content did not touch it: %v", mt)
	}

	for range 2 { // the second time, already gone
		if err := s.Remove(ctx, "src", a); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.ModTime(ctx, "src", a); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ModTime of a removed object = %v", err)
	}
	if err := s.Touch(ctx, "src", a); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Touch of a removed object = %v", err)
	}

	// Refused names, a bad Last-Modified, failing requests.
	for name, call := range map[string]func() error{
		"Remove name":  func() error { return s.Remove(ctx, "src", "notes.txt") },
		"ModTime name": func() error { _, err := s.ModTime(ctx, "src", "nope"); return err },
		"Touch name":   func() error { return s.Touch(ctx, "a/b", b) },
		"ModTime parse": func() error {
			fake.modified["pre/src/"+b] = time.Time{}
			_, err := s.ModTime(ctx, "src", b)
			return err
		},
	} {
		if err := call(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	fake.failPut, fake.failList = true, true
	if err := s.Remove(ctx, "src", b); err == nil {
		t.Error("Remove answered 500: no error")
	}
	if _, err := s.List(ctx); err == nil {
		t.Error("List answered 500: no error")
	}
	if _, err := s.Put(ctx, "src", "again.txt", strings.NewReader("stored")); err == nil {
		t.Error("Put whose touch answered 500: no error")
	}
	fake.failPut, fake.failList = false, false
	rt.down = true
	if _, err := s.List(ctx); err == nil {
		t.Error("List of an unreachable store: no error")
	}
	if err := s.Remove(ctx, "src", b); err == nil {
		t.Error("Remove on an unreachable store: no error")
	}
}
