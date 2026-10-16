package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const minimalPipeline = "pipeline: p\ntriggers:\n  go: { manual: {} }\nsteps:\n  s: { action: ./actions/a, in: trigger.go }\n"

// writeTree writes files (slash paths relative to dir) in the given order.
func writeTree(t *testing.T, dir string, files [][2]string) {
	t.Helper()
	for _, f := range files {
		p := filepath.Join(dir, filepath.FromSlash(f[0]))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(f[1]), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLoadPackage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeTree(t, dir, [][2]string{
		{"pipeline.yaml", minimalPipeline},
		{"actions/a/action.yaml", "name: a\n"},
		{"lessons/s.yaml", "[]\n"},
		{".git/HEAD", "ref: main\n"},
		{".millstream/millstream.db", "state"},
		{"assets/.git/keep", "nested .git dirs are skipped too\n"},
	})
	if err := os.Symlink("pipeline.yaml", filepath.Join(dir, "link.yaml")); err != nil {
		t.Fatal(err)
	}
	pkg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var keys []string
	for k := range pkg.Files {
		keys = append(keys, k)
	}
	if len(keys) != 3 || pkg.Files["actions/a/action.yaml"] == nil || pkg.Files["lessons/s.yaml"] == nil {
		t.Errorf("files = %v, want pipeline.yaml, actions/a/action.yaml, lessons/s.yaml", keys)
	}
	if pkg.Dir != dir || pkg.PipelineFile != FileName || pkg.Pipeline.Name != "p" {
		t.Errorf("pkg = %+v", pkg)
	}
	if got := pkg.Pipeline.Steps["s"].Lessons; got != "lessons/s.yaml" {
		t.Errorf("default lessons = %q", got)
	}
	// The hash formula, computed by hand: path NUL content NUL in path order.
	h := sha256.New()
	for _, f := range [][2]string{
		{"actions/a/action.yaml", "name: a\n"}, {"lessons/s.yaml", "[]\n"}, {"pipeline.yaml", minimalPipeline},
	} {
		h.Write([]byte(f[0] + "\x00" + f[1] + "\x00"))
	}
	if want := hex.EncodeToString(h.Sum(nil)); pkg.Hash != want {
		t.Errorf("hash = %s, want %s", pkg.Hash, want)
	}
}

func TestLoadHash(t *testing.T) {
	t.Parallel()
	base := [][2]string{
		{"pipeline.yaml", minimalPipeline},
		{"actions/a/run.sh", "#!/bin/sh\necho hi\n"},
		{"assets/t.txt", "template"},
	}
	hashOf := func(files [][2]string) string {
		t.Helper()
		return memPackage(t, files...).Hash
	}
	want := hashOf(base)
	reversed := [][2]string{base[2], base[1], base[0]}
	if got := hashOf(reversed); got != want {
		t.Errorf("hash depends on file order: %s != %s", got, want)
	}
	tests := []struct {
		name  string
		files [][2]string
	}{
		{"one byte of an asset", [][2]string{base[0], base[1], {"assets/t.txt", "templatE"}}},
		{"one byte of the pipeline", [][2]string{{"pipeline.yaml", minimalPipeline + " "}, base[1], base[2]}},
		{"a renamed file", [][2]string{base[0], base[1], {"assets/u.txt", "template"}}},
		{"an added empty file", append(append([][2]string{}, base...), [2]string{"assets/empty", ""})},
		{"content moved across the separator", [][2]string{base[0], base[1], {"assets/t.tx", "ttemplate"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hashOf(tt.files); got == want {
				t.Errorf("hash unchanged by %s", tt.name)
			}
		})
	}
}

func TestLoadSingleFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeTree(t, dir, [][2]string{{"daily.yaml", minimalPipeline}, {"other.txt", "not part of it"}})
	pkg, err := Load(filepath.Join(dir, "daily.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(pkg.Files) != 1 || pkg.PipelineFile != "daily.yaml" || pkg.Dir != dir {
		t.Errorf("pkg = %+v", pkg)
	}
	sum := sha256.Sum256([]byte("daily.yaml\x00" + minimalPipeline + "\x00"))
	if pkg.Hash != hex.EncodeToString(sum[:]) {
		t.Errorf("hash = %s", pkg.Hash)
	}
}

func TestLoadErrors(t *testing.T) {
	t.Parallel()
	missing := t.TempDir()
	writeTree(t, missing, [][2]string{{"README.md", "no pipeline"}})
	broken := t.TempDir()
	writeTree(t, broken, [][2]string{{"pipeline.yaml", "pipeline: p\nbogus: 1\n"}})
	unreadable := t.TempDir()
	writeTree(t, unreadable, [][2]string{{"pipeline.yaml", minimalPipeline}, {"secret/x", "x"}})
	if err := os.Chmod(filepath.Join(unreadable, "secret"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(unreadable, "secret"), 0o700) }) // #nosec G302 -- test cleanup
	tests := []struct {
		name, path, want string
	}{
		{"no such path", filepath.Join(missing, "nope"), "load package"},
		{"no pipeline file", missing, "no pipeline.yaml"},
		{"parse error names the file", broken, filepath.Join(broken, "pipeline.yaml") + ": line 2: bogus: unknown key"},
		{"unreadable directory", unreadable, "permission denied"},
		{"unreadable file", filepath.Join(unreadable, "secret", "x"), "load package"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if os.Geteuid() == 0 && strings.HasPrefix(tt.name, "unreadable") {
				t.Skip("root reads everything")
			}
			_, err := Load(tt.path)
			assertErr(t, err, tt.want)
		})
	}
}

// TestActionDigest: an action's digest covers the files of its directory only, not those of a directory whose name
// it prefixes.
func TestActionDigest(t *testing.T) {
	t.Parallel()
	pkg := &Package{Files: map[string][]byte{"actions/score/action.yaml": []byte("a"), "actions/scorer/x": []byte("b")}}
	d := pkg.ActionDigest("actions/score")
	pkg.Files["actions/scorer/x"] = []byte("c")
	if !strings.HasPrefix(d, "sha256:") || pkg.ActionDigest("actions/score") != d {
		t.Fatalf("digest %s changed with another directory", d)
	}
	pkg.Files["actions/score/prompt.md"] = []byte("p")
	if pkg.ActionDigest("actions/score") == d {
		t.Fatal("digest unchanged by a file of the action")
	}
}

// memPackage builds a package from files (slash paths) in memory: the tests of what follows the reading need no
// directory.
func memPackage(t *testing.T, files ...[2]string) *Package {
	t.Helper()
	m := map[string][]byte{}
	for _, f := range files {
		m[f[0]] = []byte(f[1])
	}
	pkg, err := FromFiles("/pkg", m)
	if err != nil {
		t.Fatal(err)
	}
	return pkg
}

func TestFromFilesWithoutPipeline(t *testing.T) {
	t.Parallel()
	if _, err := FromFiles("/pkg", map[string][]byte{"README.md": nil}); err == nil ||
		!strings.Contains(err.Error(), "no pipeline.yaml") {
		t.Fatalf("err = %v", err)
	}
}
