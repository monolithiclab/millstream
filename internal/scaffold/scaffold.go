// Package scaffold writes new pipeline packages and actions from templates embedded in the binary (spec section 13,
// millstream init). Every scaffold works out of the box: a package passes millstream check and millstream test
// pipeline, an action's fixtures pass millstream test action once it is built. Go actions require the published Go SDK.
package scaffold

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"text/template"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// Action languages.
const (
	LangGo  = "go"
	LangRaw = "raw"
)

// Step is the step, and the action directory, of a scaffolded package.
const Step = "greet"

//go:embed templates
var templates embed.FS

// listings is the listings example, millstream init pipeline <name> --from listings: the reference pipeline on
// sample data, with the recordings its agent replays.
//
//go:embed all:listings
var listings embed.FS

// ListingsName is the --from name of the listings example and its pipeline's name as embedded.
const ListingsName = "listings"

// actionFiles are the files of an action in each language; pipelineFiles those of a package besides its action.
var (
	actionFiles = map[string][]string{
		LangGo:  {"action.yaml", "main.go", "go.mod", "evals/example.yaml", "tests/example.yaml"},
		LangRaw: {"action.yaml", "main.py", "README.md", "tests/example.yaml"},
	}
	pipelineFiles = []string{"pipeline.yaml", "tests/example.yaml", ".gitignore"}
)

// data is what the templates see.
type data struct {
	// Name is the action's manifest name, Bin its program, Module its Go module path, Lang its language.
	Name, Bin, Module, Lang string
	// Pipeline is the package's pipeline and Step its step, for a package.
	Pipeline, Step string
}

// Pipeline writes a package named name in a new directory under parent and returns its path: a manual trigger and
// one step running a Go action, with the action's fixture and eval, a package test that stubs the step, and a
// .gitignore.
func Pipeline(parent, name string) (string, error) {
	dir, err := target(parent, name)
	if err != nil {
		return "", err
	}
	d := data{Name: name + "/" + Step, Bin: Step, Module: name + "/" + Step, Lang: LangGo, Pipeline: name, Step: Step}
	if err := write(dir, "pipeline", pipelineFiles, d); err != nil {
		return "", err
	}
	return dir, write(filepath.Join(dir, "actions", Step), "action", actionFiles[LangGo], d)
}

// Action writes an action named name, in lang, in a new directory under parent and returns its path.
func Action(parent, name, lang string) (string, error) {
	files, ok := actionFiles[lang]
	if !ok {
		return "", fmt.Errorf("language %q: want %s or %s", lang, LangGo, LangRaw)
	}
	dir, err := target(parent, name)
	if err != nil {
		return "", err
	}
	return dir, write(dir, "action", files, data{Name: name, Bin: name, Module: name, Lang: lang})
}

// Files writes files, by slash-separated path, in a new directory named name under parent and returns its path: a
// copy of a package to fork, such as the improve package.
func Files(parent, name string, files map[string][]byte) (string, error) {
	dir, err := target(parent, name)
	if err != nil {
		return "", err
	}
	for _, p := range slices.Sorted(maps.Keys(files)) {
		dst := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return "", err
		}
		// #nosec G306 -- source files of the user's new package
		if err := os.WriteFile(dst, files[p], 0o644); err != nil {
			return "", err
		}
	}
	return dir, nil
}

// ReadFS returns the files of fsys by slash-separated path: an embedded package, to copy with Files.
func ReadFS(fsys fs.FS) (map[string][]byte, error) {
	files := map[string][]byte{}
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		files[p], err = fs.ReadFile(fsys, p)
		return err
	})
	return files, err
}

// ListingsFiles returns the files of the listings example as embedded.
func ListingsFiles() (map[string][]byte, error) {
	sub, err := fs.Sub(listings, ListingsName)
	if err != nil {
		return nil, err
	}
	return ReadFS(sub)
}

// Listings writes the listings example in a new directory named name under parent, its pipeline named name, and
// returns its path.
func Listings(parent, name string) (string, error) {
	files, err := ListingsFiles()
	if err != nil {
		return "", err
	}
	files[pipeline.FileName] = bytes.Replace(files[pipeline.FileName], []byte("\npipeline: "+ListingsName+"\n"),
		[]byte("\npipeline: "+name+"\n"), 1)
	return Files(parent, name, files)
}

// Environment writes the default environment file as dir/millstream.yaml and returns its path, refusing to
// overwrite one.
func Environment(dir string) (string, error) {
	path := filepath.Join(dir, pipeline.EnvironmentFile)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	// #nosec G302 G304 -- the user's configuration file, in the directory they name
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if errors.Is(err, fs.ErrExist) {
		return "", fmt.Errorf("%s: %w", path, ErrExists)
	}
	if err != nil {
		return "", err
	}
	_, err = f.Write(pipeline.DefaultFile())
	return path, errors.Join(err, f.Close())
}

// ErrName and ErrExists are the scaffold's refusals.
var (
	ErrName   = errors.New("a name is lowercase letters, digits and single dashes")
	ErrExists = errors.New("it exists")
)

// target validates name and returns parent/name, refusing a directory that exists and is not empty.
func target(parent, name string) (string, error) {
	if !pipeline.ValidName(name) {
		return "", fmt.Errorf("%q: %w", name, ErrName)
	}
	dir := filepath.Join(parent, name)
	f, err := os.Open(dir) // #nosec G304 -- the directory the user asks to scaffold
	if errors.Is(err, fs.ErrNotExist) {
		return dir, nil
	}
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Readdirnames(1); !errors.Is(err, io.EOF) {
		if err == nil {
			err = ErrExists
		}
		return "", fmt.Errorf("%s: %w", dir, err)
	}
	return dir, nil
}

// write renders templates/<set>/<file>.tmpl to dir/<file> for each file; a dot file's template drops the dot.
func write(dir, set string, files []string, d data) error {
	for _, name := range files {
		src := "templates/" + set + "/" + name + ".tmpl"
		if name[0] == '.' {
			src = "templates/" + set + "/" + name[1:] + ".tmpl"
		}
		t, err := template.ParseFS(templates, src)
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		if err := t.Execute(&buf, d); err != nil {
			return fmt.Errorf("render %s: %w", name, err)
		}
		dst := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return err
		}
		mode := os.FileMode(0o644)
		if filepath.Ext(name) == ".py" {
			mode = 0o755
		}
		// #nosec G306 -- source files of the user's new project, the script executable
		if err := os.WriteFile(dst, buf.Bytes(), mode); err != nil {
			return err
		}
	}
	return nil
}
