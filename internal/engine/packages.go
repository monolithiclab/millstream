package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Packages live in the store (spec section 4.9): ApplyPackages stores each one's files as a bundle under its content
// hash, with the revision. An engine started from the store (serve) loads a revision's package from its bundle, into
// Options.PackageDir, the first time it meets the revision, as a node caches a package it fetches.

// ApplyOptions are what apply says beyond its packages.
type ApplyOptions struct {
	// Force lets through what only --force accepts: breaking an importer's contract (spec section 4.12).
	Force bool
	// Message says why, kept with each revision.
	Message string
}

// Applied is one package ApplyPackages applied.
type Applied struct {
	Pipeline    string
	Revision    store.RevisionID
	PackageHash string
	// Created is false when the package was the pipeline's active one already: applying it changed nothing.
	Created bool
}

// Finding is a check finding of one pipeline.
type Finding struct {
	Pipeline string
	pipeline.Diagnostic
}

func (f Finding) String() string { return f.Pipeline + ": " + f.Diagnostic.String() }

// CheckError refuses an apply whose check found errors; it wraps store.ErrInvalid.
type CheckError struct {
	Findings []Finding // the errors and the warnings, by pipeline then path
}

func (e *CheckError) Error() string {
	var lines []string
	for _, f := range e.Findings {
		if f.Severity == pipeline.SeverityError {
			lines = append(lines, f.String())
		}
	}
	return "check failed: " + strings.Join(lines, "; ")
}

func (e *CheckError) Unwrap() error { return store.ErrInvalid }

// ApplyPackages validates the packages, each on its own and with every other pipeline of the store as its active
// revision has it (pipeline.CheckInstance), then applies each one as a new revision of its pipeline, unless it is the
// active one already. Any check error refuses the whole apply with a *CheckError; the warnings come back with the
// revisions. Each package is applied in a unit of its own: a store failure part-way leaves the earlier ones applied.
func (e *Engine) ApplyPackages(ctx context.Context, pkgs []*pipeline.Package, opts ApplyOptions) ([]Applied, []Finding,
	error,
) {
	if len(pkgs) == 0 {
		return nil, nil, fmt.Errorf("apply: no package: %w", store.ErrInvalid)
	}
	findings, err := e.checkApply(ctx, pkgs, opts)
	if err != nil {
		return nil, nil, err
	}
	if slices.ContainsFunc(findings, func(f Finding) bool { return f.Severity == pipeline.SeverityError }) {
		return nil, nil, &CheckError{Findings: findings}
	}
	out := make([]Applied, 0, len(pkgs))
	for _, pkg := range pkgs {
		before, err := e.st.ActiveRevision(ctx, pkg.Pipeline.Name)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return out, findings, err
		}
		id, err := e.apply(ctx, pkg, opts.Message)
		if err != nil {
			return out, findings, err
		}
		out = append(out, Applied{Pipeline: pkg.Pipeline.Name, Revision: id, PackageHash: pkg.Hash, Created: id != before})
	}
	return out, findings, nil
}

// checkApply checks the packages and the active package of every other pipeline, then what spans them.
func (e *Engine) checkApply(ctx context.Context, pkgs []*pipeline.Package, opts ApplyOptions) ([]Finding, error) {
	states, err := e.st.Pipelines(ctx)
	if err != nil {
		return nil, err
	}
	applied := map[string]bool{}
	for _, pkg := range pkgs {
		applied[pkg.Pipeline.Name] = true
	}
	var others []*pipeline.Package
	for _, ps := range states {
		if applied[ps.Name] {
			continue
		}
		rev, err := e.st.Revision(ctx, ps.ActiveRevision)
		if err != nil {
			return nil, err
		}
		pkg, err := e.pkg(ctx, rev.PackageHash)
		if err != nil {
			return nil, fmt.Errorf("apply: the active package of %s: %w", ps.Name, err)
		}
		others = append(others, pkg)
	}
	var findings []Finding
	for _, pkg := range slices.Concat(pkgs, others) {
		for _, d := range pipeline.Check(pkg, e.opts.Resolver, e.opts.Env, e.opts.Check) {
			findings = append(findings, Finding{Pipeline: pkg.Pipeline.Name, Diagnostic: d})
		}
	}
	across := pipeline.CheckInstance(pkgs, others, opts.Force)
	for _, name := range slices.Sorted(maps.Keys(across)) {
		for _, d := range across[name] {
			findings = append(findings, Finding{Pipeline: name, Diagnostic: d})
		}
	}
	return findings, nil
}

// pkg returns the package applied under hash: the one this engine applied or loaded, else, with a PackageDir, the
// store's bundle written there.
func (e *Engine) pkg(ctx context.Context, hash string) (*pipeline.Package, error) {
	e.mu.Lock()
	p := e.packages[hash]
	e.mu.Unlock()
	if p != nil {
		return p, nil
	}
	if e.opts.PackageDir == "" {
		return nil, fmt.Errorf("package %s is not loaded in this engine", hash)
	}
	bundle, err := e.st.Package(ctx, hash)
	if err != nil {
		return nil, err
	}
	files, err := DecodeBundle(bundle)
	if err != nil {
		return nil, fmt.Errorf("package %s: %w", hash, err)
	}
	p, err = e.PackageFromFiles(files)
	if err != nil {
		return nil, err
	}
	if p.Hash != hash {
		return nil, fmt.Errorf("package %s: its files hash to %s", hash, p.Hash)
	}
	e.mu.Lock()
	e.packages[hash] = p
	e.mu.Unlock()
	return p, nil
}

// DecodeBundle decodes a package bundle: its files as a JSON object of base64 contents by slash-separated path.
// A path that is absolute, or climbs out of the package, is refused.
func DecodeBundle(bundle []byte) (map[string][]byte, error) {
	var files map[string][]byte
	if err := json.Unmarshal(bundle, &files); err != nil {
		return nil, fmt.Errorf("decode bundle: %w", err)
	}
	for p := range files {
		if !filepath.IsLocal(filepath.FromSlash(p)) || path.Clean(p) != p {
			return nil, fmt.Errorf("bundle path %q is not a clean path inside the package", p)
		}
	}
	return files, nil
}

// EncodeBundle is the bundle of a package's files, which DecodeBundle reads.
func EncodeBundle(files map[string][]byte) ([]byte, error) { return json.Marshal(files) }

// PackageFromFiles writes a package's files under PackageDir/<hash>, read-only, unless they are there already, and
// returns the package of that directory: what an action process runs from.
func (e *Engine) PackageFromFiles(files map[string][]byte) (*pipeline.Package, error) {
	if e.opts.PackageDir == "" {
		return nil, errors.New("this engine has no package directory")
	}
	p, err := pipeline.FromFiles(e.opts.PackageDir, files)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(e.opts.PackageDir, p.Hash)
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		if err := writePackage(e.opts.PackageDir, dir, files); err != nil {
			return nil, fmt.Errorf("write package %s: %w", p.Hash, err)
		}
	} else if err != nil {
		return nil, err
	}
	p.Dir = dir
	return p, nil
}

// writePackage writes files into a temporary directory under parent, then renames it to dir: a crash leaves no
// partial package. The files are read-only and executable, as a package's binaries must be.
func writePackage(parent, dir string, files map[string][]byte) error {
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, ".tmp-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	root, err := os.OpenRoot(tmp)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	for _, p := range slices.Sorted(maps.Keys(files)) {
		if d := path.Dir(p); d != "." {
			if err := root.MkdirAll(d, 0o750); err != nil {
				return err
			}
		}
		f, err := root.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o500) // #nosec G302 -- a package's binaries run
		if err != nil {
			return err
		}
		_, werr := f.Write(files[p])
		if err := errors.Join(werr, f.Close()); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, dir); err != nil && !errors.Is(err, os.ErrExist) {
		if _, serr := os.Stat(dir); serr == nil {
			return nil // another engine of this host wrote it first
		}
		return err
	}
	return nil
}
