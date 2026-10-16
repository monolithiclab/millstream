package engine

import (
	"context"
	"errors"
	"fmt"
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

// Applied is one package applied.
type Applied struct {
	Pipeline    string
	Revision    store.RevisionID
	PackageHash string
	// Created is false when the package was the pipeline's active one already: applying it changed nothing.
	Created bool
	// Tokens are the tokens of the webhook triggers that had none, by trigger: shown once, since the store keeps
	// only their hashes (spec section 4.6).
	Tokens map[string]string
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
		a, err := e.apply(ctx, pkg, opts.Message)
		if err != nil {
			return out, findings, err
		}
		out = append(out, a)
	}
	return out, findings, nil
}

// checkApply checks the packages and the active package of every other pipeline, each alone and against each other:
// the applied ones are the changed pipelines, and the set is the whole deployment (pipeline.InstanceOptions).
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
	all, changed := slices.Concat(pkgs, others), make([]string, 0, len(pkgs))
	for _, pkg := range pkgs {
		changed = append(changed, pkg.Pipeline.Name)
	}
	across := pipeline.CheckInstance(all, e.opts.Resolver,
		pipeline.InstanceOptions{Changed: changed, Complete: true, Force: opts.Force})
	var findings []Finding
	for _, pkg := range all {
		name := pkg.Pipeline.Name
		diags := pipeline.Check(pkg, e.opts.Resolver, e.opts.Env, e.opts.Check)
		if d, ok := across[name]; ok {
			diags = pipeline.SortDiagnostics(append(diags, d...))
			delete(across, name) // a pipeline given twice gets them once
		}
		for _, d := range diags {
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
	files, err := pipeline.DecodeBundle(bundle)
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

// PackageFromFiles writes a package's files under PackageDir/<hash>, read-only, unless they are there already, and
// returns the package of that directory: what an action process runs from.
func (e *Engine) PackageFromFiles(files map[string][]byte) (*pipeline.Package, error) {
	if e.opts.PackageDir == "" {
		return nil, errors.New("this engine has no package directory")
	}
	return pipeline.Materialize(e.opts.PackageDir, files)
}
