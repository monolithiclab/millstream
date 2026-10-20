package cli

import (
	"fmt"
	"slices"

	"github.com/alecthomas/kong"
)

// helpPrinter is Kong's help, with the default commands tagged implicit:"" (run [dir…], trigger <pipeline>
// <trigger>, node --join) shown under their parent's name, since naming them is optional. A parent's help is its
// implicit command's: usage, arguments and flags, followed by the parent's other commands.
func helpPrinter(opts kong.HelpOptions, ctx *kong.Context) error {
	defer unnameImplicit(ctx.Model.Node)()
	sel := ctx.Selected()
	if sel == nil || sel.DefaultCmd == nil || !sel.DefaultCmd.Tag.Has("implicit") || opts.Summary {
		return kong.DefaultHelpPrinter(opts, ctx)
	}
	dctx := *ctx
	dctx.Path = append(slices.Clone(ctx.Path), &kong.Path{Command: sel.DefaultCmd})
	if err := kong.DefaultHelpPrinter(opts, &dctx); err != nil {
		return err
	}
	fmt.Fprint(ctx.Stdout, "\nCommands:\n")
	for _, c := range sel.Children {
		if c != sel.DefaultCmd && !c.Hidden {
			fmt.Fprintf(ctx.Stdout, "  %s\n    %s\n", c.Summary(), c.Help)
		}
	}
	return nil
}

// unnameImplicit blanks the names of the implicit commands under n, so that help prints their paths as their
// parent's, and returns the function restoring them.
func unnameImplicit(n *kong.Node) (restore func()) {
	saved := map[*kong.Node]string{}
	var walk func(*kong.Node)
	walk = func(n *kong.Node) {
		for _, c := range n.Children {
			if c.Tag.Has("implicit") {
				saved[c], c.Name = c.Name, ""
			}
			walk(c)
		}
	}
	walk(n)
	return func() {
		for c, name := range saved {
			c.Name = name
		}
	}
}
