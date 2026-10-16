package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

// A bundle decodes back to its files, refusing paths that leave the package; Materialize writes a package once,
// read-only, under its hash, and reuses it.
func TestBundleAndMaterialize(t *testing.T) {
	for _, bundle := range []string{`{"../x":""}`, `{"/etc/x":""}`, `{"a/../b":""}`, `[]`} {
		if _, err := DecodeBundle([]byte(bundle)); err == nil {
			t.Errorf("DecodeBundle(%s): no error", bundle)
		}
	}
	files := map[string][]byte{FileName: []byte("pipeline: p\ntriggers: { go: { manual: {} } }\n" +
		"steps: { a: { action: filter, in: trigger.go, config: { expr: \"true\" } } }\n"), "docs/notes.md": []byte("x")}
	bundle, err := EncodeBundle(files)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeBundle(bundle)
	if err != nil || string(back["docs/notes.md"]) != "x" {
		t.Fatalf("round trip = %v, %v", back, err)
	}
	parent := t.TempDir()
	p, err := Materialize(parent, files)
	if err != nil || p.Dir != filepath.Join(parent, p.Hash) {
		t.Fatalf("Materialize = %+v, %v", p, err)
	}
	if info, err := os.Stat(filepath.Join(p.Dir, "docs", "notes.md")); err != nil || info.Mode().Perm() != 0o500 {
		t.Fatalf("written file: %v, %v", info, err)
	}
	if again, err := Materialize(parent, files); err != nil || again.Dir != p.Dir {
		t.Fatalf("again = %+v, %v", again, err)
	}
	if _, err := Materialize(parent, map[string][]byte{"other.yaml": nil}); err == nil {
		t.Fatal("no pipeline file: no error")
	}
	blocked := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocked, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Materialize(blocked, files); err == nil {
		t.Fatal("a parent that is a file: no error")
	}
}
