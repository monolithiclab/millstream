package artifacts

import (
	"bytes"
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
			ref, err := s.Put(t.Context(), "listings", tt.name, strings.NewReader(tt.content))
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			want := Ref{Name: tt.name, SHA256: sum([]byte(tt.content)), Size: int64(len(tt.content)), Type: tt.wantType}
			if ref != want {
				t.Fatalf("ref = %+v, want %+v", ref, want)
			}
			r, err := s.Open(t.Context(), "listings", ref.SHA256)
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
	a, err := s.Put(t.Context(), "listings", "a.txt", strings.NewReader("same"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Put(t.Context(), "listings", "b.txt", strings.NewReader("same"))
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
	if _, err := s.Put(t.Context(), "other", "a.txt", strings.NewReader("same")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "other", a.SHA256)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(t.Context(), "other", a.SHA256); err != nil {
		t.Fatal(err)
	}
}

func TestPutAndOpenErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := New(dir)
	for _, name := range []string{"", "..", "out/a.txt", `a\b`} {
		if _, err := s.Put(t.Context(), "listings", name, strings.NewReader("x")); err == nil {
			t.Errorf("Put name %q: no error", name)
		}
	}
	for _, p := range []string{"", "..", "a/b", ".hidden"} {
		if _, err := s.Put(t.Context(), p, "x", strings.NewReader("x")); err == nil {
			t.Errorf("Put pipeline %q: no error", p)
		}
		if _, err := s.Open(t.Context(), p, sum(nil)); err == nil {
			t.Errorf("Open pipeline %q: no error", p)
		}
	}
	if _, err := s.Open(t.Context(), "listings", "../../etc/passwd"); err == nil {
		t.Error("Open of a path: no error")
	}
	if _, err := s.Open(t.Context(), "listings", sum([]byte("absent"))); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Open of a missing artifact: %v, want fs.ErrNotExist", err)
	}
	// A failing reader stores nothing and leaves no temporary file.
	boom := errors.New("boom")
	if _, err := s.Put(t.Context(), "listings", "x", iotest.ErrReader(boom)); !errors.Is(err, boom) {
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
	if _, err := New(blocked).Put(t.Context(), "listings", "x", strings.NewReader("x")); err == nil {
		t.Fatal("Put under a file: no error")
	}
}

func TestDirExistsCopyAndSpool(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	s := New(dir)
	ref, err := s.Put(ctx, "listings", "a.txt", strings.NewReader("content"))
	if err != nil {
		t.Fatal(err)
	}
	exists := []struct {
		pipeline, sha string
		want, err     bool
	}{
		{"listings", ref.SHA256, true, false},
		{"listings", sum([]byte("absent")), false, false},
		{"other", ref.SHA256, false, false},
		{"listings", "nothex", false, true},
		{"a/b", ref.SHA256, false, true},
	}
	for _, tt := range exists {
		if ok, err := s.Exists(ctx, tt.pipeline, tt.sha); ok != tt.want || (err != nil) != tt.err {
			t.Errorf("Exists(%q, %q) = %v, %v", tt.pipeline, tt.sha, ok, err)
		}
	}
	// A stat failure other than absence is an error: the pipeline "directory" is a file.
	if err := os.WriteFile(filepath.Join(dir, "blocked"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exists(ctx, "blocked", ref.SHA256); err == nil {
		t.Error("Exists under a file: no error")
	}

	if err := s.Copy(ctx, "listings", "importer", ref.SHA256); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Exists(ctx, "importer", ref.SHA256); !ok {
		t.Error("Copy: the importer does not hold the artifact")
	}
	if err := s.Copy(ctx, "listings", "importer", sum([]byte("absent"))); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Copy of a missing artifact: %v, want fs.ErrNotExist", err)
	}

	for _, p := range []string{"listings", "a-b.c_d", "9"} {
		if err := CheckPipeline(p); err != nil {
			t.Errorf("CheckPipeline(%q): %v", p, err)
		}
	}
	for _, p := range []string{"", ".", "..", ".x", "a/b", "-x", "a b"} {
		if err := CheckPipeline(p); err == nil {
			t.Errorf("CheckPipeline(%q): no error", p)
		}
	}

	spooled, f, err := Spool(t.TempDir(), "page.html", strings.NewReader("<html><body>x</body></html>"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	want := Ref{Name: "page.html", SHA256: sum([]byte("<html><body>x</body></html>")), Size: 27,
		Type: "text/html; charset=utf-8"}
	if spooled != want {
		t.Errorf("Spool ref = %+v, want %+v", spooled, want)
	}
	if got, err := io.ReadAll(f); err != nil || string(got) != "<html><body>x</body></html>" {
		t.Errorf("spooled file reads %q, %v", got, err)
	}
	if _, _, err := Spool(filepath.Join(dir, "missing"), "x", strings.NewReader("x")); err == nil {
		t.Error("Spool into a missing directory: no error")
	}
}
