package cli

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// LockCmd is "millstream lock".
type LockCmd struct {
	Dir string `arg:"" optional:"" type:"path" help:"Package directory (default: the current one)."`
}

// Run resolves every container and registry action of the package to an exact version and image digest, ignoring
// the current lock file, and writes them to the package's millstream.lock (spec section 6.2). It prints one line per
// pinned action. A package without such actions gets no lock file.
func (c LockCmd) Run(rt *Runtime) error {
	pkg, err := pipeline.Load(dirOrCurrent(c.Dir))
	if err != nil {
		return err
	}
	env, err := environment(rt)
	if err != nil {
		return err
	}
	r := resolver(rt, env, true)
	lock := &pipeline.Lock{Actions: map[string]pipeline.LockEntry{}}
	p := pkg.Pipeline
	for _, name := range slices.Sorted(maps.Keys(p.Steps)) {
		ref := p.Steps[name].Action
		if _, done := lock.Actions[ref]; done {
			continue
		}
		a, err := r.Resolve(pkg, ref)
		if err != nil {
			return fmt.Errorf("steps.%s.action: %w", name, err)
		}
		if a.Kind != pipeline.KindContainer {
			continue
		}
		lock.Actions[ref] = pipeline.LockEntry{Version: registryVersion(a), Image: a.Image, Digest: a.ImageDigest}
	}
	if len(lock.Actions) == 0 {
		fmt.Fprintf(rt.IO.Err, "%s: no container or registry actions to lock\n", p.Name)
		return nil
	}
	path := filepath.Join(pkg.Dir, pipeline.LockFile)
	if err := os.WriteFile(path, lock.Marshal(), 0o644); err != nil { // #nosec G306 -- a package file, read by all
		return fmt.Errorf("write %s: %w", path, err)
	}
	for _, ref := range slices.Sorted(maps.Keys(lock.Actions)) {
		e := lock.Actions[ref]
		fmt.Fprintf(rt.IO.Out, "%s %s %s\n", ref, e.Image, e.Digest)
	}
	return nil
}

// registryVersion is the version a registry reference resolved to; local actions, which have a directory in the
// package, record none.
func registryVersion(a *pipeline.Action) string {
	if a.Dir != "" {
		return ""
	}
	return a.Version
}
