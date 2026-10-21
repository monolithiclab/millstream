package pipeline

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// loadFile loads src as the environment file, with no variables.
func loadFile(t *testing.T, src []byte) (*Environment, error) {
	t.Helper()
	path := filepath.Join(testdir.New(t), EnvironmentFile)
	if err := os.WriteFile(path, src, 0o600); err != nil {
		t.Fatal(err)
	}
	return LoadEnvironment(path, true, func(string) (string, bool) { return "", false }, nil)
}

// TestDefaultFileLoadsAsDefaults: the generated file is all comments, so loading it changes nothing.
func TestDefaultFileLoadsAsDefaults(t *testing.T) {
	t.Parallel()
	got, err := loadFile(t, DefaultFile())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, Defaults()) {
		t.Errorf("loaded %+v, want the defaults", got)
	}
}

// TestDefaultFileUncommented: with its comment marker removed, every line of the file parses and validates, so every
// example in it is valid.
func TestDefaultFileUncommented(t *testing.T) {
	t.Parallel()
	var lines []string
	for l := range strings.Lines(string(DefaultFile())) {
		lines = append(lines, strings.TrimPrefix(l, "# ")) // "## " prose lines keep their marker
	}
	got, err := loadFile(t, []byte(strings.Join(lines, "")))
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Validate(); err != nil {
		t.Error(err)
	}
	if reflect.DeepEqual(got, Defaults()) {
		t.Error("uncommenting changed nothing")
	}
}

// TestDefaultFileHasEveryKey: every leaf has a doc and a default or an example, and its exact key line names its variable.
func TestDefaultFileHasEveryKey(t *testing.T) {
	t.Parallel()
	lines := strings.Split(string(DefaultFile()), "\n")
	vars := map[string]bool{}
	for _, s := range Settings(Defaults()) {
		if s.Doc == "" || s.Default == "" && s.Example == "" {
			t.Errorf("%s: needs a doc and a default or an example", s.Key)
		}
		if vars[s.Var] {
			t.Errorf("%s: variable %s is used twice", s.Key, s.Var)
		}
		vars[s.Var] = true
		prefix := "# " + strings.Repeat("  ", strings.Count(s.Key, ".")) + s.Key[strings.LastIndex(s.Key, ".")+1:] + ": "
		suffix := "   # " + s.Var
		if s.Default == "" {
			suffix += ", example"
		}
		if !slices.ContainsFunc(lines, func(l string) bool {
			return strings.HasPrefix(l, prefix) && strings.HasSuffix(l, suffix)
		}) {
			t.Errorf("%s: no key line %q ... %q", s.Key, prefix, suffix)
		}
	}
}

// TestDefaultFileGolden: the file as written, rewritten by -update.
func TestDefaultFileGolden(t *testing.T) {
	t.Parallel()
	path := filepath.Join("testdata", "millstream.yaml.golden")
	if *update {
		if err := os.WriteFile(path, DefaultFile(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != string(DefaultFile()) {
		t.Errorf("differs from %s (rerun with -update)", path)
	}
}
