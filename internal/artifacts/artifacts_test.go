package artifacts

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

func sum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestPutAndOpen(t *testing.T) {
	t.Parallel()
	png := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0}, 600)...)
	tests := []struct {
		name, content, wantType string
	}{
		{"report.txt", "margin 18%\n", "text/plain; charset=utf-8"},
		{"page.html", "<!DOCTYPE html><html><body>listing</body></html>", "text/html; charset=utf-8"},
		{"photo.png", string(png), "image/png"},
		{"empty", "", "text/plain; charset=utf-8"},
	}
	s := New(t.TempDir()) // one store: the contents differ, so do their files
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := s.Put("listings", tt.name, strings.NewReader(tt.content))
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			want := Ref{Name: tt.name, SHA256: sum([]byte(tt.content)), Size: int64(len(tt.content)), Type: tt.wantType}
			if ref != want {
				t.Fatalf("ref = %+v, want %+v", ref, want)
			}
			r, err := s.Open("listings", ref.SHA256)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = r.Close() }()
			if got, _ := io.ReadAll(r); string(got) != tt.content {
				t.Fatalf("content = %q", got)
			}
		})
	}
}

// TestPutDedupes: the same content stored twice, under two names, is one file, and no temporary file is left.
func TestPutDedupes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := New(dir)
	a, err := s.Put("listings", "a.txt", strings.NewReader("same"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Put("listings", "b.txt", strings.NewReader("same"))
	if err != nil {
		t.Fatal(err)
	}
	if a.SHA256 != b.SHA256 || a.Name != "a.txt" || b.Name != "b.txt" {
		t.Fatalf("refs %+v and %+v", a, b)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "listings"))
	if err != nil || len(entries) != 1 || entries[0].Name() != a.SHA256 {
		t.Fatalf("store holds %v (%v), want the one file %s", entries, err, a.SHA256)
	}
	// Another pipeline stores its own copy: artifacts are scoped by pipeline (spec section 9.2).
	if _, err := s.Put("other", "a.txt", strings.NewReader("same")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "other", a.SHA256)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open("other", a.SHA256); err != nil {
		t.Fatal(err)
	}
}

func TestPutAndOpenErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := New(dir)
	for _, name := range []string{"", "..", "out/a.txt", `a\b`} {
		if _, err := s.Put("listings", name, strings.NewReader("x")); err == nil {
			t.Errorf("Put name %q: no error", name)
		}
	}
	for _, p := range []string{"", "..", "a/b", ".hidden"} {
		if _, err := s.Put(p, "x", strings.NewReader("x")); err == nil {
			t.Errorf("Put pipeline %q: no error", p)
		}
		if _, err := s.Open(p, sum(nil)); err == nil {
			t.Errorf("Open pipeline %q: no error", p)
		}
	}
	if _, err := s.Open("listings", "../../etc/passwd"); err == nil {
		t.Error("Open of a path: no error")
	}
	if _, err := s.Open("listings", sum([]byte("absent"))); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Open of a missing artifact: %v, want fs.ErrNotExist", err)
	}
	// A failing reader stores nothing and leaves no temporary file.
	boom := errors.New("boom")
	if _, err := s.Put("listings", "x", iotest.ErrReader(boom)); !errors.Is(err, boom) {
		t.Fatalf("Put of a failing reader: %v", err)
	}
	if entries, _ := os.ReadDir(filepath.Join(dir, "listings")); len(entries) != 0 {
		t.Fatalf("left %v behind", entries)
	}
	// A store whose directory cannot be created fails Put.
	blocked := filepath.Join(dir, "file")
	if err := os.WriteFile(blocked, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(blocked).Put("listings", "x", strings.NewReader("x")); err == nil {
		t.Fatal("Put under a file: no error")
	}
}

// The sweep's operations (Files): list, a file's time, copy into another prefix, remove; a missing file is no error
// to remove, and a name that is no artifact is refused.
func TestFiles(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := New(t.TempDir())
	if files, err := s.List(ctx); err != nil || len(files) != 0 {
		t.Fatalf("List of a store never written = %v, %v", files, err)
	}
	ref, err := s.Put("src", "a.txt", strings.NewReader("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Copy(ctx, "src", "deal", ref.SHA256); err != nil {
		t.Fatal(err)
	}
	files, err := s.List(ctx)
	if err != nil || len(files) != 2 || files[0].Pipeline != "deal" || files[1].Name != ref.SHA256 {
		t.Fatalf("List = %+v, %v", files, err)
	}
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	path := filepath.Join(s.dir, "src", ref.SHA256)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put("src", "again.txt", strings.NewReader("a")); err != nil { // stored again: touched
		t.Fatal(err)
	}
	if mt, err := s.ModTime(ctx, "src", ref.SHA256); err != nil || !mt.After(old) {
		t.Fatalf("ModTime after storing again = %v, %v", mt, err)
	}
	for _, p := range []string{"src", "src"} { // the second time, already gone
		if err := s.Remove(ctx, p, ref.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.ModTime(ctx, "src", ref.SHA256); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ModTime of a removed file = %v", err)
	}
	if err := s.Copy(ctx, "src", "other", ref.SHA256); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Copy of a missing file = %v", err)
	}
	for _, name := range []string{"../x", "notes.txt"} {
		if err := s.Remove(ctx, "src", name); err == nil {
			t.Fatalf("Remove %q was accepted", name)
		}
	}
	if _, err := s.ModTime(ctx, "src", "nope"); err == nil {
		t.Fatal("ModTime of a name that is no hash was accepted")
	}
}

// A gzip artifact named as something else is stored compressed and read decompressed; one named .gz is gzip.
func TestCompressed(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write([]byte(`{"turns":[]}`))
	_ = zw.Close()
	s := New(t.TempDir())
	ref, err := s.Put("p", "transcript.json", bytes.NewReader(buf.Bytes()))
	if err != nil || !Compressed(ref) || Compressed(Ref{Name: "logs.gz", Type: ref.Type}) ||
		Compressed(Ref{Name: "a.json", Type: "text/plain"}) {
		t.Fatalf("Compressed(%+v) = %v, %v", ref, Compressed(ref), err)
	}
	f, err := s.Open("p", ref.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	r, err := Decompressed(ref, f)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := io.ReadAll(r); err != nil || string(b) != `{"turns":[]}` {
		t.Fatalf("decompressed = %q, %v", b, err)
	}
	if _, err := Decompressed(ref, strings.NewReader("not gzip")); err == nil {
		t.Fatal("Decompressed accepted data that is not gzip")
	}
	if r, err := Decompressed(Ref{Name: "a.txt", Type: "text/plain"}, strings.NewReader("a")); err != nil || r == nil {
		t.Fatalf("Decompressed of a plain artifact = %v, %v", r, err)
	}
}
