package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// Test_initPipeline: the scaffolded package passes check and test pipeline as generated, and init refuses to write
// over it or under a name that is not an identifier.
func Test_initPipeline(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	out, _ := mustRun(t, 0, "init", "pipeline", "greetings", "--dir", parent)
	pkg := filepath.Join(parent, "greetings")
	if !strings.HasPrefix(out, "created "+pkg+"\n") {
		t.Fatalf("stdout = %q", out)
	}
	for _, f := range []string{".gitignore", "actions/greet/main.go", "actions/greet/go.mod",
		"actions/greet/evals/example.yaml", "actions/greet/tests/example.yaml"} {
		if _, err := os.Stat(filepath.Join(pkg, f)); err != nil {
			t.Error(err)
		}
	}
	if _, errOut := mustRun(t, 0, "check", pkg); !strings.Contains(errOut, "greetings: 0 errors") {
		t.Fatalf("check: %s", errOut)
	}
	if out, _ := mustRun(t, 0, "test", "pipeline", pkg); out != "PASS one name greeted\n" {
		t.Fatalf("test pipeline: %q", out)
	}

	tests := []struct {
		argv []string
		want string
	}{
		{[]string{"init", "pipeline", "greetings", "--dir", parent}, "the directory exists and is not empty"},
		{[]string{"init", "pipeline", "Greetings", "--dir", parent}, "lowercase letters, digits and single dashes"},
		{[]string{"init", "action", "a--b", "--dir", parent}, "lowercase letters, digits and single dashes"},
		{[]string{"init", "action", "x", "--lang", "sh", "--dir", parent}, "--lang"},
	}
	for _, tt := range tests {
		if _, errOut := mustRun(t, 2, tt.argv...); !strings.Contains(errOut, tt.want) {
			t.Errorf("%v: stderr %q, want %q", tt.argv, errOut, tt.want)
		}
	}
	empty := filepath.Join(parent, "empty")
	if err := os.Mkdir(empty, 0o750); err != nil {
		t.Fatal(err)
	}
	mustRun(t, 0, "init", "action", "empty", "--dir", parent) // an empty directory is fine
}

// Test_initAction: both languages scaffold a manifest that resolves, with the files of the language.
func Test_initAction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		lang    string
		files   []string
		command []string
	}{
		{"go", []string{"main.go", "go.mod", "evals/example.yaml", "tests/example.yaml"}, []string{"scorer"}},
		{"raw", []string{"main.py", "README.md", "tests/example.yaml"}, []string{"python3", "main.py"}},
	}
	for _, tt := range tests {
		t.Run(tt.lang, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			mustRun(t, 0, "init", "action", "scorer", "--lang", tt.lang, "--dir", parent)
			dir := filepath.Join(parent, "scorer")
			for _, f := range tt.files {
				if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
					t.Error(err)
				}
			}
			if fi, err := os.Stat(filepath.Join(dir, "main.py")); err == nil && fi.Mode().Perm()&0o100 == 0 {
				t.Errorf("main.py is not executable: %v", fi.Mode())
			}
			pkg, ref, err := pipeline.LoadAction(dir)
			if err != nil {
				t.Fatal(err)
			}
			a, err := pipeline.Resolver{}.Resolve(pkg, ref)
			if err != nil {
				t.Fatal(err)
			}
			if a.Name != "scorer" || a.Input == nil || a.Output == nil || a.Config == nil ||
				strings.Join(a.Command, " ") != strings.Join(tt.command, " ") {
				t.Fatalf("action = %+v", a)
			}
		})
	}
}
