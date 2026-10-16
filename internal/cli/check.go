package cli

import (
	"fmt"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// CheckCmd is "millstream check".
type CheckCmd struct {
	Dirs []string `arg:"" optional:"" type:"path" help:"Package directories (default: the current one); several are also checked against each other: imports, contracts and cycles across pipelines."`
}

// Run validates the packages and prints one "severity path: message" line per diagnostic on stdout (prefixed with
// the pipeline's name when several are checked), then a summary per pipeline on stderr. It fails when a package
// does not load or any diagnostic is an error.
func (c CheckCmd) Run(rt *Runtime) error {
	dirs := c.Dirs
	if len(dirs) == 0 {
		dirs = []string{dirOrCurrent("")}
	}
	pkgs := make([]*pipeline.Package, 0, len(dirs))
	for _, dir := range dirs {
		pkg, err := pipeline.Load(dir)
		if err != nil {
			return err
		}
		pkgs = append(pkgs, pkg)
	}
	env, err := environment(rt)
	if err != nil {
		return err
	}
	diags := checkPackages(rt, pkgs, resolver(rt, env, false), checkOptions(rt, env))
	failed := false
	for _, pkg := range pkgs {
		name, errs := pkg.Pipeline.Name, 0
		prefix := ""
		if len(pkgs) > 1 {
			prefix = name + " "
		}
		for _, d := range diags[name] {
			if d.Severity == pipeline.SeverityError {
				errs++
			}
			if _, err := fmt.Fprintln(rt.IO.Out, prefix+d.String()); err != nil {
				return err
			}
		}
		fmt.Fprintf(rt.IO.Err, "%s: %s, %s\n", name, plural(errs, "error"), plural(len(diags[name])-errs, "warning"))
		failed = failed || errs > 0
	}
	if failed {
		return errFailed
	}
	return nil
}

// checkPackages checks each package (pipeline.Check) and the packages against each other (pipeline.CheckInstance,
// every one of them changed), and returns each pipeline's diagnostics by name, sorted as Check sorts them.
func checkPackages(rt *Runtime, pkgs []*pipeline.Package, actions pipeline.ActionResolver, opts pipeline.CheckOptions,
) map[string][]pipeline.Diagnostic {
	out := pipeline.CheckInstance(pkgs, actions, pipeline.InstanceOptions{})
	for _, pkg := range pkgs {
		name := pkg.Pipeline.Name
		out[name] = pipeline.SortDiagnostics(append(pipeline.Check(pkg, actions, rt.Env, opts), out[name]...))
	}
	return out
}

// plural formats a count with its noun: 1 error, 2 errors.
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
