package cli

import (
	"bytes"
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
	var buf bytes.Buffer
	dctx := *ctx
	dctx.Path = append(slices.Clone(ctx.Path), &kong.Path{Command: sel.DefaultCmd})
	out := ctx.Stdout
	ctx.Stdout = &buf
	err := kong.DefaultHelpPrinter(opts, &dctx)
	ctx.Stdout = out
	if err != nil {
		return err
	}
	buf.WriteString("\nCommands:\n")
	for _, c := range sel.Children {
		if c != sel.DefaultCmd && !c.Hidden {
			fmt.Fprintf(&buf, "  %s\n    %s\n", c.Summary(), c.Help)
		}
	}
	_, err = out.Write(buf.Bytes())
	return err
}

// unnameImplicit blanks the names of the implicit commands under n, so that help prints their paths as their
// parent's, and returns the function restoring them.
func unnameImplicit(n *kong.Node) (restore func()) {
	var named []*kong.Node
	var names []string
	var walk func(*kong.Node)
	walk = func(n *kong.Node) {
		for _, c := range n.Children {
			if c.Tag.Has("implicit") {
				named, names = append(named, c), append(names, c.Name)
				c.Name = ""
			}
			walk(c)
		}
	}
	walk(n)
	return func() {
		for i, c := range named {
			c.Name = names[i]
		}
	}
}
