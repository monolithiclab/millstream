package millstream

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// annotate reads its input artifact page.html, writes notes.txt from it and attaches it.
func annotate(c Context, _ map[string]any) ([]string, error) {
	in, err := c.InputArtifact("page.html")
	if err != nil {
		return nil, err
	}
	page, err := os.ReadFile(in) // #nosec G304 -- the path the SDK resolved inside the working directory
	if err != nil {
		return nil, err
	}
	out := filepath.Join(os.Getenv("MILLSTREAM_WORKDIR"), "notes.txt")
	if err := os.WriteFile(out, append([]byte("notes on "), page...), 0o600); err != nil {
		return nil, err
	}
	if err := c.Artifact("notes.txt", out); err != nil {
		return nil, err
	}
	return []string{"annotated"}, nil
}

// TestArtifactRoundTrip runs an action process that reads an input artifact from the working directory and
// attaches the file it writes: the emit names it by its path relative to the working directory.
func TestArtifactRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "in"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "in", "page.html"), []byte("listing 42"), 0o600); err != nil {
		t.Fatal(err)
	}
	itemLine := `{"type":"item","delivery":"dl_1","id":"it_1","payload":{},"artifacts":[{"name":"page.html","path":"in/page.html"}]}`
	cmd := exec.Command(os.Args[0], "-test.run=^$") // #nosec G204 -- the test binary itself
	cmd.Env = append(os.Environ(), helperEnv+"=artifact", "MILLSTREAM_WORKDIR="+dir)
	cmd.Stdin = strings.NewReader(hello + "\n" + itemLine + "\n")
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%v\nstderr:\n%s", err, stderr.String())
	}
	want := `{"type":"ready"}` + "\n" +
		`{"type":"emit","delivery":"dl_1","payload":"annotated","artifacts":[{"name":"notes.txt","path":"notes.txt"}]}` + "\n" +
		`{"type":"done","delivery":"dl_1"}` + "\n"
	if stdout.String() != want {
		t.Fatalf("stdout:\n%s\nwant:\n%s\nstderr:\n%s", stdout.String(), want, stderr.String())
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "notes.txt")); string(b) != "notes on listing 42" {
		t.Fatalf("notes.txt = %q", b)
	}
}

func TestArtifactErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, f := range []string{"a.txt", "sub/b.txt"} {
		p := filepath.Join(dir, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("a.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	c := &itemContext{workdir: dir}
	if err := c.Artifact("a", "a.txt"); err != nil {
		t.Fatalf("relative path: %v", err)
	}
	if err := c.Artifact("b", filepath.Join(dir, "sub", "b.txt")); err != nil {
		t.Fatalf("absolute path inside: %v", err)
	}
	if got := c.artifacts; len(got) != 2 || got[1] != (artifactMsg{Name: "b", Path: "sub/b.txt"}) {
		t.Fatalf("artifacts = %+v", got)
	}
	tests := []struct{ what, name, path string }{
		{"duplicate name", "a", "a.txt"},
		{"empty name", "", "a.txt"},
		{"name with a slash", "x/y", "a.txt"},
		{"dot dot name", "..", "a.txt"},
		{"name too long", strings.Repeat("n", 256), "a.txt"},
		{"traversal", "c", "../outside.txt"},
		{"absolute outside", "c", "/etc/passwd"},
		{"missing", "c", "absent.txt"},
		{"directory", "c", "sub"},
		{"symlink", "c", "link"},
	}
	for _, tt := range tests {
		if err := c.Artifact(tt.name, tt.path); err == nil {
			t.Errorf("%s: no error", tt.what)
		}
	}
	if err := (&itemContext{}).Artifact("a", "a.txt"); err == nil {
		t.Error("no MILLSTREAM_WORKDIR: no error")
	}
	in := &itemContext{workdir: dir, item: inMsg{Artifacts: []artifactMsg{{Name: "page.html", Path: "in/page.html"}}}}
	if p, err := in.InputArtifact("page.html"); err != nil || p != filepath.Join(dir, "in", "page.html") {
		t.Fatalf("InputArtifact = %q, %v", p, err)
	}
	if _, err := in.InputArtifact("absent"); err == nil {
		t.Fatal("InputArtifact of an absent artifact: no error")
	}
}
