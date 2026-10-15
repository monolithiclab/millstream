package cli

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"text/tabwriter"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// VarsCmd is "millstream vars".
type VarsCmd struct {
	Dir string            `arg:"" optional:"" type:"path" help:"Package directory (default: the current one)."`
	Var map[string]string `help:"Set a variable (name=value)."`
}

// Run prints each variable with its value as JSON, its source and its environment variable. A variable that does
// not resolve is listed as unset, and the command then fails with every resolution problem on stderr. Secrets are
// not variables: they are left to check and run.
func (c VarsCmd) Run(rt *Runtime) error {
	pkg, err := pipeline.Load(dirOrCurrent(c.Dir))
	if err != nil {
		return err
	}
	p := *pkg.Pipeline
	p.Secrets = nil
	res, resolveErr := pipeline.ResolveVars(&p, c.Var, rt.Env)

	tw := tabwriter.NewWriter(rt.IO.Out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tVALUE\tSOURCE\tENVIRONMENT")
	for _, name := range slices.Sorted(maps.Keys(p.Vars)) {
		value, source := "-", "unset"
		if v, ok := res.Vars[name]; ok {
			b, err := json.Marshal(v)
			if err != nil {
				return fmt.Errorf("variable %s: %w", name, err)
			}
			value, source = string(b), res.Sources[name]
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", name, value, source, pipeline.VarEnv(name))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	return resolveErr
}
