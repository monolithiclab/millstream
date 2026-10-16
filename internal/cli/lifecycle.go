package cli

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// ApplyCmd is "millstream apply <dir…>" (spec section 4.9).
type ApplyCmd struct {
	Dirs    []string `arg:"" type:"path" help:"Package directories."`
	Force   bool     `help:"Apply even when a package breaks the contract of a stream another pipeline imports."`
	Message string   `help:"Why: kept with each revision."`
}

// Run checks the packages, each on its own and with every other pipeline of the deployment, then applies each as a
// new revision of its pipeline: through the served engine's API with --server, else to the store from this process.
// It prints each pipeline's revision, and the check's warnings on stderr.
func (c ApplyCmd) Run(rt *Runtime) error {
	pkgs := make([]*pipeline.Package, 0, len(c.Dirs))
	for _, dir := range c.Dirs {
		pkg, err := pipeline.Load(dir)
		if err != nil {
			return err
		}
		pkgs = append(pkgs, pkg)
	}
	return changeState(rt, func(cl *apiClient) error { return c.remote(rt, cl, pkgs) }, func(st store.Store) error {
		env, err := environment(rt)
		if err != nil {
			return err
		}
		e, err := engine.New(engine.Options{Store: st, Env: rt.Env, Secrets: secrets(rt, env),
			Resolver: resolver(rt, env, false), Check: checkOptions(rt, env), PackageDir: packageDir(rt, env)})
		if err != nil {
			return err
		}
		applied, findings, err := e.ApplyPackages(rt.Ctx, pkgs, engine.ApplyOptions{Force: c.Force, Message: c.Message})
		if ce := (*engine.CheckError)(nil); errors.As(err, &ce) {
			findings, err = ce.Findings, errFailed
		}
		for _, f := range findings {
			fmt.Fprintln(rt.IO.Err, f.String())
		}
		if err != nil {
			return err
		}
		for _, a := range applied {
			printApplied(rt, api.AppliedOf(a))
		}
		return nil
	})
}

func (c ApplyCmd) remote(rt *Runtime, cl *apiClient, pkgs []*pipeline.Package) error {
	req := api.ApplyRequest{Force: c.Force, Message: c.Message}
	for _, pkg := range pkgs {
		req.Packages = append(req.Packages, api.PackageFiles{Files: pkg.Files})
	}
	var res api.ApplyResponse
	if _, err := cl.post(rt.Ctx, "/revisions", req, &res); err != nil {
		return err
	}
	for _, f := range res.Warnings {
		fmt.Fprintln(rt.IO.Err, engine.Finding{Pipeline: f.Pipeline,
			Diagnostic: pipeline.Diagnostic{Severity: f.Severity, Path: f.Path, Message: f.Message}}.String())
	}
	for _, a := range res.Revisions {
		printApplied(rt, a)
	}
	return nil
}

// printApplied prints a revision an apply applied, and the new tokens of its webhook triggers.
func printApplied(rt *Runtime, a api.AppliedRevision) {
	suffix := ""
	if !a.Created {
		suffix = " (already active)"
	}
	fmt.Fprintf(rt.IO.Out, "%s revision %d %s%s\n", a.Pipeline, a.Revision, shortHash(a.PackageHash), suffix)
	printTokens(rt, a.Pipeline, a.Tokens)
}

// shortHash is a package hash's first 12 characters.
func shortHash(h string) string { return h[:min(len(h), 12)] }

// RollbackCmd is "millstream rollback <pipeline> [revision]".
type RollbackCmd struct {
	Pipeline string `arg:"" help:"Pipeline name."`
	Revision int64  `arg:"" optional:"" help:"The revision to reactivate (default: the one before the active one)."`
}

// Run reactivates the revision: runs started from now on use it; runs in flight finish on their own.
func (c RollbackCmd) Run(rt *Runtime) error {
	if c.Revision < 0 {
		return usageError("revision: want a revision number")
	}
	var id int64
	err := changeState(rt, func(cl *apiClient) error {
		var rev api.Revision
		_, err := cl.post(rt.Ctx, pipelinePath(c.Pipeline, "rollback"), api.RollbackRequest{Revision: c.Revision}, &rev)
		id = rev.ID
		return err
	}, func(st store.Store) error {
		rev, err := st.Rollback(rt.Ctx, c.Pipeline, store.RevisionID(c.Revision))
		id = int64(rev)
		return err
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s rolled back to revision %d\n", c.Pipeline, id)
	return err
}

// DisableCmd is "millstream disable <pipeline>".
type DisableCmd struct {
	Pipeline string `arg:"" help:"Pipeline name."`
}

// Run disables the pipeline until "millstream resume" enables it again.
func (c DisableCmd) Run(rt *Runtime) error {
	err := changeState(rt, func(cl *apiClient) error {
		_, err := cl.post(rt.Ctx, pipelinePath(c.Pipeline, "disable"), nil, nil)
		return err
	}, func(st store.Store) error { return st.Disable(rt.Ctx, c.Pipeline) })
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s disabled\n", c.Pipeline)
	return err
}

// changeState runs viaAPI with the served engine's client when a server is named, else direct on the store opened
// to change state from this process.
func changeState(rt *Runtime, viaAPI func(*apiClient) error, direct func(store.Store) error) error {
	cl, err := remote(rt)
	if err != nil {
		return err
	}
	if cl != nil {
		return viaAPI(cl)
	}
	return withWriter(rt, direct)
}

// created reports whether an action's answer says it started something (201) rather than finding it done (200).
func created(status int) bool { return status == http.StatusCreated }

// PurgeCmd is "millstream purge" (spec section 9.3).
type PurgeCmd struct {
	Key       string `help:"Erase every item with this key." xor:"what" required:""`
	Item      string `help:"Erase the item with this id." xor:"what" required:""`
	Pipeline  string `help:"Only in this pipeline (the copies imports made are followed all the same)."`
	RequestID string `name:"request-id" help:"Names the purge: repeating it after a crash finishes the purge it started."`
}

// Run erases what the purge names, to its end: through the served engine's API with --server, else from this
// process, which first finishes any purge a crash interrupted. It prints the purge and what it erased.
func (c PurgeCmd) Run(rt *Runtime) error {
	var (
		p       api.Purge
		started bool
	)
	err := changeState(rt, func(cl *apiClient) error {
		status, err := cl.post(rt.Ctx, "/purges", api.PurgeRequest{Key: c.Key, Item: c.Item, Pipeline: c.Pipeline,
			RequestID: c.RequestID}, &p)
		started = created(status)
		return err
	}, func(st store.Store) error {
		env, err := environment(rt)
		if err != nil {
			return err
		}
		arts, err := artifactStore(rt, env)
		if err != nil {
			return err
		}
		e, err := engine.New(engine.Options{Store: st, Env: rt.Env, Artifacts: arts})
		if err != nil {
			return err
		}
		if err := e.ResumePurges(rt.Ctx); err != nil {
			return err
		}
		sp, ok, err := e.Purge(rt.Ctx, engine.PurgeRequest{Key: c.Key, Item: c.Item, Pipeline: c.Pipeline,
			RequestID: c.RequestID})
		p, started = api.ToPurge(sp), ok
		return err
	})
	if err != nil {
		return err
	}
	suffix := ""
	if !started {
		suffix = " (already started)"
	}
	where := ""
	if len(p.Pipelines) > 0 {
		where = " in " + strings.Join(p.Pipelines, ", ")
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s %s: %s erased%s%s\n", p.ID, p.State, plural(int(p.Erased), "item"), where, suffix)
	return err
}
