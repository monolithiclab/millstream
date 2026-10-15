package cli

import (
	"fmt"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// CheckCmd is "millstream check".
type CheckCmd struct {
	Dir string `arg:"" optional:"" type:"path" help:"Package directory (default: the current one)."`
}

// Run validates the package and prints one "severity path: message" line per diagnostic on stdout, then a summary
// on stderr. It fails when the package does not load or any diagnostic is an error.
func (c CheckCmd) Run(rt *Runtime) error {
	pkg, err := pipeline.Load(dirOrCurrent(c.Dir))
	if err != nil {
		return err
	}
	env, err := environment(rt)
	if err != nil {
		return err
	}
	diags := pipeline.Check(pkg, resolver(rt, env, false), rt.Env, checkOptions(env))
	errs := 0
	for _, d := range diags {
		if d.Severity == pipeline.SeverityError {
			errs++
		}
		if _, err := fmt.Fprintln(rt.IO.Out, d); err != nil {
			return err
		}
	}
	fmt.Fprintf(rt.IO.Err, "%s: %s, %s\n", pkg.Pipeline.Name, plural(errs, "error"), plural(len(diags)-errs, "warning"))
	if errs > 0 {
		return errFailed
	}
	return nil
}

// plural formats a count with its noun: 1 error, 2 errors.
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
