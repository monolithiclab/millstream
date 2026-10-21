package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/testdir"
)

// Test_initPipeline: the scaffolded package passes check and test pipeline as generated (e2e build), and init
// refuses to write over it or under a name that is not an identifier.
func Test_initPipeline(t *testing.T) {
	t.Parallel()
	parent := testdir.New(t)
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
	if e2eBuild { // check and test pipeline on the generated package: an engine run, e2e build only
		if _, errOut := mustRun(t, 0, "check", pkg); !strings.Contains(errOut, "greetings: 0 errors") {
			t.Fatalf("check: %s", errOut)
		}
		if out, _ := mustRun(t, 0, "test", "pipeline", pkg); out != "PASS one name greeted\n" {
			t.Fatalf("test pipeline: %q", out)
		}
	}

	if !e2eBuild { // the in-process action stands in for the unbuilt one: what matters is the engine's input check
		// run --once fires the manual trigger with {}: the scaffolded action takes it, nothing is dead-lettered.
		out, _ := newEmptyState(t).must(0, "run", "--once", pkg)
		if !strings.Contains(out, " succeeded\n") || strings.Contains(out, "dlq") {
			t.Fatalf("run --once of the scaffold:\n%s", out)
		}
	}

	if _, errOut := mustRun(t, 2, "init", "action", "x", "--lang", "sh", "--dir", parent); !strings.Contains(errOut,
		"--lang") {
		t.Errorf("--lang sh: stderr %q", errOut)
	}
	(&state{t: t}).checkRefusals(t, nil, []refusal{
		{InitPipelineCmd{Name: "greetings", Dir: parent}, true, "it exists"},
		{InitPipelineCmd{Name: "Greetings", Dir: parent}, true, "lowercase letters, digits and single dashes"},
		{InitActionCmd{Name: "a--b", Dir: parent, Lang: "go"}, true, "lowercase letters, digits and single dashes"},
	})
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
			parent := testdir.New(t)
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

// Test_initEnvironment: init environment writes the default file, says what is next, and refuses to overwrite it.
func Test_initEnvironment(t *testing.T) {
	t.Parallel()
	dir := testdir.New(t)
	out, _ := mustRun(t, 0, "init", "environment", "--dir", dir)
	path := filepath.Join(dir, "millstream.yaml")
	if !strings.HasPrefix(out, "created "+path+"\nnext: ") {
		t.Fatalf("stdout = %q", out)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(pipeline.DefaultFile()) {
		t.Fatalf("file: %v, %d bytes", err, len(got))
	}
	if _, errOut := mustRun(t, 2, "init", "environment", "--dir", dir); !strings.Contains(errOut, "it exists") {
		t.Errorf("second run: stderr %q", errOut)
	}
}
