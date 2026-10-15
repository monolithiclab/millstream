package cli

import (
	"fmt"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// GraphCmd is "millstream graph".
type GraphCmd struct {
	Dir    string `arg:"" optional:"" default:"." type:"path" help:"Package directory."`
	Format string `enum:"mermaid,dot" default:"mermaid" help:"Output format: mermaid or dot."`
}

// Run prints the package's step graph.
func (c GraphCmd) Run(rt *Runtime) error {
	pkg, err := pipeline.Load(c.Dir)
	if err != nil {
		return err
	}
	out, err := pipeline.Graph(pkg.Pipeline, c.Format)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(rt.IO.Out, out)
	return err
}
