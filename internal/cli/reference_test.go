package cli

import (
	"os"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

// referencePath is the committed CLI reference, generated from the command model.
const referencePath = "../../docs/reference/cli.md"

// Test_cliReference renders the CLI reference from the Kong model, with the walkers of the help, and compares it with
// docs/reference/cli.md; -update rewrites the file.
func Test_cliReference(t *testing.T) {
	p, err := takeParser(&Runtime{})
	if err != nil {
		t.Fatal(err)
	}
	got := renderReference(p.k.Model)
	p.release()
	if *update {
		if err := os.WriteFile(referencePath, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(referencePath)
	if err != nil {
		t.Fatalf("%v (rerun with -update)", err)
	}
	if got != string(want) {
		t.Errorf("%s differs from the commands (rerun with -update)", referencePath)
	}
}

// renderReference is the Markdown reference of every visible command, grouped as the top-level help groups them.
func renderReference(app *kong.Application) string {
	implicit := unnameImplicit(app.Node)
	defer implicit.restore()
	h := &helpWriter{implicit: implicit}
	var b strings.Builder
	b.WriteString("# CLI reference\n\n")
	b.WriteString("<!-- Generated from the commands by `go test ./internal/cli -run Test_cliReference -update`. -->\n\n")
	b.WriteString(wrapped(app.Help+" The same text is printed by `millstream <command> --help`, and "+
		"`millstream help` prints the guide.") + "\n")

	b.WriteString("\n## Global flags\n\nEvery command takes these.\n\n")
	b.WriteString(flagTable(app.Flags))

	var titles []string
	groups := map[string][]*kong.Node{}
	for _, c := range visible(app.Children) {
		title := "Commands"
		if c.Group != nil {
			title = c.Group.Title
		}
		if _, ok := groups[title]; !ok {
			titles = append(titles, title)
		}
		groups[title] = append(groups[title], c)
	}
	for _, t := range titles {
		b.WriteString("\n## " + t + "\n")
		for _, c := range groups[t] {
			h.command(&b, c, 3)
		}
	}
	return b.String()
}

// command writes a command and, under it, its commands.
func (h *helpWriter) command(b *strings.Builder, n *kong.Node, level int) {
	b.WriteString("\n" + strings.Repeat("#", level) + " `" + h.realPath(n) + "`\n\n")
	b.WriteString(wrapped(n.Help) + "\n\n")
	forms := []string{"millstream " + summary(n)}
	if def := n.DefaultCmd; def != nil && h.implicit.name(def) != "" {
		forms = append([]string{"millstream " + summary(def)}, forms...)
	}
	b.WriteString("```\n" + strings.Join(forms, "\n") + "\n```\n")
	if n.Detail != "" {
		b.WriteString("\n" + detailMarkdown(n.Detail))
	}
	if len(n.Positional) > 0 {
		b.WriteString("\n| Argument | Help |\n| --- | --- |\n")
		for _, a := range n.Positional {
			b.WriteString("| `" + a.Summary() + "` | " + cell(a.Help) + " |\n")
		}
	}
	if own := n.Flags; len(own) > 0 && flagTable(own) != "" {
		b.WriteString("\n" + flagTable(own))
	}
	for _, c := range visible(n.Children) {
		h.command(b, c, level+1)
	}
}

// flagTable is a table of the flags help lists, or "" when there are none.
func flagTable(flags []*kong.Flag) string {
	var b strings.Builder
	for _, f := range flags {
		if !shown(f) {
			continue
		}
		name := "--" + f.Name
		if f.Short != 0 {
			name = "-" + string(f.Short) + ", " + name
		}
		if !f.IsBool() && !f.IsCounter() {
			name += "=" + placeholder(f)
		}
		help := f.Help
		if f.HasDefault && f.Default != "" && !strings.Contains(strings.ToLower(help), "default") {
			help = strings.TrimSuffix(help, ".") + " (default: " + f.Default + ")."
		}
		b.WriteString("| `" + cell(name) + "` | " + cell(help) + " |\n")
	}
	if b.Len() == 0 {
		return ""
	}
	return "| Flag | Help |\n| --- | --- |\n" + b.String()
}

// cell escapes the pipes of a table cell.
func cell(s string) string { return strings.ReplaceAll(s, "|", `\|`) }

// wrapped is text wrapped at 120 columns.
func wrapped(text string) string { return strings.Join(wrap(text, 120), "\n") }

// detailMarkdown renders a command's detail: unindented lines ending in a colon are bold headings, other unindented
// lines prose, and runs of indented lines code blocks.
func detailMarkdown(text string) string {
	var b strings.Builder
	inCode := false
	closeCode := func() {
		if inCode {
			b.WriteString("```\n")
			inCode = false
		}
	}
	for l := range strings.SplitSeq(strings.TrimSpace(text), "\n") {
		switch {
		case strings.HasPrefix(l, " "):
			if !inCode {
				b.WriteString("```\n")
				inCode = true
			}
			b.WriteString(strings.TrimPrefix(l, "  ") + "\n")
		case l == "":
			closeCode()
			if !strings.HasSuffix(b.String(), "\n\n") {
				b.WriteString("\n")
			}
		case strings.HasSuffix(l, ":"):
			closeCode()
			b.WriteString("**" + l + "**\n\n")
		default:
			closeCode()
			b.WriteString(wrapped(l) + "\n")
		}
	}
	closeCode()
	return b.String()
}
