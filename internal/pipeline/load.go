package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
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
	// Dir is the package directory; for a lone pipeline file, the directory holding it.
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
	pkg := &Package{Files: map[string][]byte{}}
	if info.IsDir() {
		pkg.Dir, pkg.PipelineFile = path, FileName
		if err := readTree(path, pkg.Files); err != nil {
			return nil, fmt.Errorf("load package %s: %w", path, err)
		}
	} else {
		pkg.Dir, pkg.PipelineFile = filepath.Dir(path), filepath.Base(path)
		data, err := os.ReadFile(path) // #nosec G304 -- the user names the package to load
		if err != nil {
			return nil, fmt.Errorf("load package: %w", err)
		}
		pkg.Files[pkg.PipelineFile] = data
	}
	data, ok := pkg.Files[pkg.PipelineFile]
	if !ok {
		return nil, fmt.Errorf("load package %s: no %s", path, FileName)
	}
	if pkg.Pipeline, err = Parse(data); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(pkg.Dir, pkg.PipelineFile), err)
	}
	pkg.Hash = hashFiles(pkg.Files)
	defaultLessons(pkg)
	return pkg, nil
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
	fsys := root.FS()
	return fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != "." && slices.Contains(skippedDirs, d.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() || p == EnvironmentFile {
			return nil // the environment file describes where packages run, not the package
		}
		data, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		files[p] = data
		return nil
	})
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
