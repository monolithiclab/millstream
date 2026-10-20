package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// FileName is the pipeline file at the root of a package.
const FileName = "pipeline.yaml"

// skippedDirs are directories that are never part of a package: version control and local engine state.
var skippedDirs = []string{".git", ".millstream"}

// Package is a pipeline package (spec section 4.9): a directory holding the pipeline file and everything it uses.
type Package struct {
	// Dir is the package directory's absolute path; for a lone pipeline file, the directory holding it. Absolute,
	// because actions run in their own directory and find their command under it.
	Dir      string
	Pipeline *Pipeline
	// Hash is the hex SHA-256 identifying the package's content: the revision.
	Hash string
	// Files maps each slash-separated path, relative to Dir, to its content.
	Files map[string][]byte
	// PipelineFile is the key of the pipeline file in Files.
	PipelineFile string
}

// Load reads the package at path. A directory is a package; a path to a single file is a package of that one
// file. Every regular file outside .git and .millstream is read, except an environment file at the root, and the hash is SHA-256 over path NUL content NUL
// for each file in path order, so it is independent of directory listing order.
func Load(path string) (*Package, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("load package: %w", err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("load package: %w", err)
	}
	pkg := &Package{Files: map[string][]byte{}}
	if info.IsDir() {
		pkg.Dir, pkg.PipelineFile = abs, FileName
		if err := readTree(path, pkg.Files); err != nil {
			return nil, fmt.Errorf("load package %s: %w", path, err)
		}
	} else {
		pkg.Dir, pkg.PipelineFile = filepath.Dir(abs), filepath.Base(path)
		data, err := os.ReadFile(path) // #nosec G304 -- the user names the package to load
		if err != nil {
			return nil, fmt.Errorf("load package: %w", err)
		}
		pkg.Files[pkg.PipelineFile] = data
	}
	if _, ok := pkg.Files[pkg.PipelineFile]; !ok {
		return nil, fmt.Errorf("load package %s: no %s", path, FileName)
	}
	if err := pkg.parse(); err != nil {
		return nil, err
	}
	return pkg, nil
}

// parse parses the pipeline file of a package whose Dir, PipelineFile and Files are read, and sets Pipeline and
// Hash.
func (p *Package) parse() error {
	var err error
	if p.Pipeline, err = Parse(p.Files[p.PipelineFile]); err != nil {
		return fmt.Errorf("%s: %w", filepath.Join(p.Dir, p.PipelineFile), err)
	}
	p.Hash = hashFiles(p.Files)
	defaultLessons(p)
	return nil
}

// LoadAction reads a lone action directory, outside any package, as a package of that directory alone: Dir is its
// parent, ref (./<base>) names the action, and the pipeline is empty. Tools that run one action use it.
func LoadAction(dir string) (pkg *Package, ref string, err error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, "", fmt.Errorf("load action: %w", err)
	}
	files := map[string][]byte{}
	if err := readTree(abs, files); err != nil {
		return nil, "", fmt.Errorf("load action %s: %w", dir, err)
	}
	base := filepath.Base(abs)
	pkg = &Package{Dir: filepath.Dir(abs), Pipeline: &Pipeline{}, Files: map[string][]byte{}}
	for p, data := range files {
		pkg.Files[base+"/"+p] = data
	}
	pkg.Hash = hashFiles(pkg.Files)
	return pkg, "./" + base, nil
}

// readTree reads every regular file under dir into files, keyed by slash-separated relative path. It reads
// through an os.Root, so a symbolic link swapped in during the walk cannot lead outside the package; symbolic
// links themselves are not regular files and are skipped.
func readTree(dir string, files map[string][]byte) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return readRoot(root, "", files)
}

// readRoot reads the regular files under root into files, keyed by prefix and their slash-separated path. Each
// directory opens once, as a root of its own inside its parent, and its files by name: walking the tree through the
// top root alone would open every directory on a file's path again for each file.
func readRoot(root *os.Root, prefix string, files map[string][]byte) error {
	d, err := root.Open(".")
	if err != nil {
		return err
	}
	entries, err := d.ReadDir(-1)
	_ = d.Close()
	if err != nil {
		return err
	}
	for _, e := range entries {
		p := prefix + e.Name()
		switch {
		case e.IsDir():
			if slices.Contains(skippedDirs, e.Name()) {
				continue
			}
			sub, err := root.OpenRoot(e.Name())
			if err != nil {
				return err
			}
			err = readRoot(sub, p+"/", files)
			_ = sub.Close()
			if err != nil {
				return err
			}
		case e.Type().IsRegular() && p != EnvironmentFile: // the environment file describes where packages run
			data, err := root.ReadFile(e.Name())
			if err != nil {
				return err
			}
			files[p] = data
		}
	}
	return nil
}

// ActionDigest is the content hash of the action directory dir, slash-separated and relative to the package:
// "sha256:" and the SHA-256 over path NUL content NUL of each of its files, in path order. It identifies what an
// action process runs (its reuse key) and which version of an action an eval scored.
func (p *Package) ActionDigest(dir string) string {
	files := map[string][]byte{}
	for path, data := range p.Files {
		if strings.HasPrefix(path, dir+"/") {
			files[path] = data
		}
	}
	return "sha256:" + hashFiles(files)
}

// hashFiles is SHA-256 over path NUL content NUL for each file, in path order.
func hashFiles(files map[string][]byte) string {
	h := sha256.New()
	for _, p := range slices.Sorted(maps.Keys(files)) {
		h.Write([]byte(p))
		h.Write([]byte{0})
		h.Write(files[p])
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// defaultLessons points each step without a lessons key at lessons/<step>.yaml when the package has that file.
func defaultLessons(pkg *Package) {
	for name, s := range pkg.Pipeline.Steps {
		if s.Lessons != "" {
			continue
		}
		p := "lessons/" + name + ".yaml"
		if _, ok := pkg.Files[p]; ok {
			s.Lessons = p
			pkg.Pipeline.Steps[name] = s
		}
	}
}

// FromFiles builds the package of the directory dir from its files, already read: what Load does once it has read a
// directory. files maps slash-separated paths relative to dir to their content.
func FromFiles(dir string, files map[string][]byte) (*Package, error) {
	if _, ok := files[FileName]; !ok {
		return nil, fmt.Errorf("package %s: no %s", dir, FileName)
	}
	pkg := &Package{Dir: dir, PipelineFile: FileName, Files: files}
	if err := pkg.parse(); err != nil {
		return nil, err
	}
	return pkg, nil
}
