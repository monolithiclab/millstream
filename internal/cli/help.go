package cli

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/alecthomas/kong"
)

// helpWidth is the column help wraps at: fixed, so the help reads the same on every terminal and in the golden files.
const helpWidth = 100

// printHelp is the CLI's help, in three levels: the top level lists the commands by group with one line each, a
// group lists its commands, and a command (a leaf) shows its usage, arguments, flags and examples. A default command
// tagged implicit:"" (trigger <pipeline> <trigger>) prints under its parent's name, since naming it is optional; its
// parent's help shows it as its default form. Summary (the help after a
// usage error) shortens a command's help to its usage line.
func printHelp(rt *Runtime, opts kong.HelpOptions, ctx *kong.Context) error {
	implicit := unnameImplicit(ctx.Model.Node)
	defer implicit.restore()
	h := &helpWriter{style: helpStyle(rt, ctx.Stdout), implicit: implicit}
	switch sel := ctx.Selected(); {
	case sel == nil:
		h.root(ctx.Model)
	case len(visible(sel.Children)) > 0:
		h.group(sel)
	case opts.Summary:
		h.usage(sel)
		h.line("")
		h.linef("Run %s for its arguments and flags.", h.paintUsage(`"millstream `+h.realPath(sel)+` --help"`))
	default:
		h.leaf(sel)
	}
	_, err := io.WriteString(ctx.Stdout, h.String())
	return err
}

// helpWriter accumulates a help text.
type helpWriter struct {
	strings.Builder
	style
	implicit implicitNames
}

func (h *helpWriter) line(s string) { h.WriteString(s + "\n") }

// usageLine prints the Usage: line of a command line, and further forms aligned under it.
func (h *helpWriter) usageLine(forms ...string) {
	for i, f := range forms {
		label := "       "
		if i == 0 {
			label = h.head("Usage:") + " "
		}
		h.line(label + h.paintUsage(f))
	}
}

func (h *helpWriter) linef(format string, args ...any) { h.line(fmt.Sprintf(format, args...)) }

// section starts a titled block after a blank line.
func (h *helpWriter) section(title string) {
	h.line("")
	h.line(h.head(title))
}

// root lists the top-level commands under their groups, then where to read more.
func (h *helpWriter) root(app *kong.Application) {
	h.usageLine("millstream <command> [<args>] [flags]")
	h.line("")
	h.line(app.Help)
	var titles []string
	groups := map[string][]row{}
	for _, c := range visible(app.Children) {
		title := "Commands:"
		if c.Group != nil {
			title = c.Group.Title + ":"
		}
		if _, ok := groups[title]; !ok {
			titles = append(titles, title)
		}
		groups[title] = append(groups[title], h.row(h.cmd(c.Name), c.Name, c.Help))
	}
	width := 0
	for _, rows := range groups {
		width = max(width, columnWidth(rows))
	}
	for _, t := range titles {
		h.section(t)
		h.columns(groups[t], width)
	}
	var global []string
	for _, f := range app.Flags {
		if shown(f) {
			global = append(global, h.flag("--"+f.Name))
		}
	}
	h.line("")
	h.linef("Run %s for a guide to Millstream, %s for a command's help.", h.paintUsage(`"millstream help"`),
		h.paintUsage(`"millstream <command> --help"`))
	h.linef("Global flags, listed in every command's help: %s.", strings.Join(global, ", "))
}

// group shows a command that has commands of its own: its default form when one is implicit, and each command with
// its one-line help. No flags: each command's help has them.
func (h *helpWriter) group(n *kong.Node) {
	path := h.realPath(n)
	def := n.DefaultCmd
	if def != nil && h.implicit.name(def) == "" {
		def = nil // a default named in full, as inbox list: listed with the others
	}
	forms := []string{"millstream " + path + " <command> [<args>] [flags]"}
	if def != nil {
		forms = append([]string{"millstream " + summary(def)}, forms...)
	}
	h.usageLine(forms...)
	h.line("")
	h.line(n.Help)
	var defRows, rows []row
	if def != nil {
		form := strings.TrimSuffix(summary(def), " [flags]")
		defRows = []row{
			h.row(h.paintUsage(form), form, def.Help),
			{text: "Its arguments and flags: " + `"millstream ` + h.realPath(def) + ` --help"`},
		}
	}
	for _, c := range visible(n.Children) {
		if c == def {
			continue
		}
		form := strings.TrimSpace(c.Name + " " + operands(c))
		rows = append(rows, h.row(h.paintUsage(form), form, c.Help))
	}
	width := max(columnWidth(defRows), columnWidth(rows))
	if def != nil {
		h.section("Default command:")
		h.columns(defRows, width)
	}
	h.section("Commands:")
	h.columns(rows, width)
	h.line("")
	h.linef("Run %s for a command's arguments and flags.", h.paintUsage(`"millstream `+path+` <command> --help"`))
}

// usage prints a command's usage line and its one-line help.
func (h *helpWriter) usage(n *kong.Node) {
	h.usageLine("millstream " + summary(n))
	h.line("")
	h.line(n.Help)
}

// leaf shows a command in full: usage, detail and examples, arguments, its flags with their defaults, and the
// global flags.
func (h *helpWriter) leaf(n *kong.Node) {
	h.usage(n)
	if n.Detail != "" {
		h.line("")
		h.detail(n.Detail)
	}
	if len(n.Positional) > 0 {
		var rows []row
		for _, a := range n.Positional {
			rows = append(rows, h.row(h.arg(a.Summary()), a.Summary(), a.Help))
		}
		h.section("Arguments:")
		h.columns(rows, columnWidth(rows))
	}
	var own, global []row
	for p := n; p != nil; p = p.Parent {
		for _, f := range p.Flags {
			switch {
			case !shown(f):
			case p.Parent == nil:
				global = append(global, h.flagRow(f))
			default:
				own = append(own, h.flagRow(f))
			}
		}
	}
	width := max(columnWidth(own), columnWidth(global))
	if len(own) > 0 {
		h.section("Flags:")
		h.columns(own, width)
	}
	h.section("Global flags:")
	h.columns(global, width)
}

// detail prints a command's detail: unindented lines ending in a colon are headings, indented lines starting with
// millstream are commands, and indented lines starting with # their comments.
func (h *helpWriter) detail(text string) {
	for l := range strings.SplitSeq(strings.TrimSpace(text), "\n") {
		trimmed := strings.TrimLeft(l, " ")
		indent := l[:len(l)-len(trimmed)]
		switch {
		case indent == "" && strings.HasSuffix(l, ":"):
			h.line(h.head(l))
		case strings.HasPrefix(trimmed, "millstream "):
			h.line(indent + h.paintUsage(trimmed))
		case strings.HasPrefix(trimmed, "# "):
			h.line(indent + h.arg(trimmed))
		default:
			h.line(l)
		}
	}
}

// flagRow is a flag's row: its name and placeholder, and its help with its default.
func (h *helpWriter) flagRow(f *kong.Flag) row {
	name := "--" + f.Name
	if f.Short != 0 {
		name = "-" + string(f.Short) + ", " + name
	}
	plain, painted := name, h.flag(name)
	if !f.IsBool() && !f.IsCounter() {
		ph := placeholder(f)
		plain += "=" + ph
		painted += h.flag("=") + h.arg(ph)
	}
	help := f.Help
	if f.HasDefault && f.Default != "" && !strings.Contains(strings.ToLower(help), "default") {
		help = strings.TrimSuffix(help, ".") + " (default: " + f.Default + ")."
	}
	return h.row(painted, plain, help)
}

// summary is a command's usage after the program name: its path, its required flags (one of each exclusive set),
// its arguments or <command>, and [flags] when it has optional ones (every command has: the global flags).
func summary(n *kong.Node) string {
	parts := []string{n.Path()}
	var oneOf []string
	optional := false
	for p := n; p != nil; p = p.Parent {
		for _, f := range p.Flags {
			switch {
			case !shown(f):
			case !f.Required:
				optional = true
			case len(f.Xor) > 0:
				oneOf = append(oneOf, flagForm(f))
			default:
				parts = append(parts, flagForm(f))
			}
		}
	}
	if len(oneOf) > 0 {
		parts = append(parts, "("+strings.Join(oneOf, " | ")+")")
	}
	if o := operands(n); o != "" {
		parts = append(parts, o)
	}
	if optional {
		parts = append(parts, "[flags]")
	}
	return strings.Join(parts, " ")
}

// operands is what a command line gives a command after its name: its arguments, or <command> when it has
// commands of its own.
func operands(n *kong.Node) string {
	var parts []string
	for _, a := range n.Positional {
		parts = append(parts, a.Summary())
	}
	if len(parts) == 0 && len(visible(n.Children)) > 0 {
		parts = append(parts, "<command>")
	}
	return strings.Join(parts, " ")
}

// shown says whether help lists a flag: not hidden, and not --help, which every command has.
func shown(f *kong.Flag) bool { return !f.Hidden && f.Name != "help" }

// flagForm is a flag as a command line gives it: its name, and a placeholder for its value.
func flagForm(f *kong.Flag) string {
	if f.IsBool() || f.IsCounter() {
		return "--" + f.Name
	}
	return "--" + f.Name + "=" + placeholder(f)
}

// placeholder names a flag's value: its choices, its placeholder tag, its type for numbers and durations, or its
// name in capitals.
func placeholder(f *kong.Flag) string {
	if choices := slices.DeleteFunc(f.EnumSlice(), func(s string) bool { return s == "" }); len(choices) > 0 {
		return strings.Join(choices, "|")
	}
	if f.PlaceHolder != "" || f.IsMap() {
		return f.FormatPlaceHolder()
	}
	switch t := f.Target.Type(); {
	case t == reflect.TypeFor[time.Duration]():
		return "DURATION"
	case t.Kind() >= reflect.Int && t.Kind() <= reflect.Uint64:
		return "N"
	}
	ph := strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
	if f.IsSlice() {
		ph += ",..."
	}
	return ph
}

// row is a two-column row: its left cell, possibly painted, the cell's printed width, and the text beside it.
type row struct {
	left  string
	width int
	text  string
}

// row makes a row of a left cell painted from plain.
func (h *helpWriter) row(painted, plain, text string) row {
	return row{painted, utf8.RuneCountInString(plain), text}
}

// maxLeft is the widest left cell the text starts beside; a wider one puts its text on the next line.
const maxLeft = 32

// columnWidth is the width of the widest left cell, up to maxLeft.
func columnWidth(rows []row) int {
	w := 0
	for _, r := range rows {
		if r.width <= maxLeft {
			w = max(w, r.width)
		}
	}
	return w
}

// columns prints rows in two columns, the text wrapped beside a left column of the given width.
func (h *helpWriter) columns(rows []row, width int) {
	const indent, gap = "  ", "   "
	pad := strings.Repeat(" ", len(indent)+width+len(gap))
	for _, r := range rows {
		lines := wrap(r.text, helpWidth-len(pad))
		switch {
		case r.width <= width && len(lines) > 0:
			h.line(strings.TrimRight(indent+r.left+strings.Repeat(" ", width-r.width)+gap+lines[0], " "))
			lines = lines[1:]
		default:
			h.line(indent + r.left)
		}
		for _, l := range lines {
			h.line(pad + l)
		}
	}
}

// wrap breaks text into lines of at most width columns, at spaces; a longer word stands on a line of its own.
func wrap(text string, width int) []string {
	var lines []string
	cur := ""
	for w := range strings.FieldsSeq(text) {
		switch {
		case cur == "":
			cur = w
		case utf8.RuneCountInString(cur)+1+utf8.RuneCountInString(w) > width:
			lines = append(lines, cur)
			cur = w
		default:
			cur += " " + w
		}
	}
	if cur != "" {
		lines = append(lines, cur)
	}
	return lines
}

// realPath is a node's path with the names of implicit commands restored: the way to name one explicitly.
func (h *helpWriter) realPath(n *kong.Node) string {
	var names []string
	for p := n; p != nil && p.Type == kong.CommandNode; p = p.Parent {
		name := p.Name
		if named := h.implicit.name(p); named != "" {
			name = named
		}
		names = append(names, name)
	}
	slices.Reverse(names)
	return strings.Join(names, " ")
}

// visible returns the nodes that are not hidden.
func visible(nodes []*kong.Node) []*kong.Node {
	return slices.DeleteFunc(slices.Clone(nodes), func(n *kong.Node) bool { return n.Hidden })
}

// implicitNames maps the implicit commands to their names while help blanks them.
type implicitNames map[*kong.Node]string

func (m implicitNames) name(n *kong.Node) string { return m[n] }

// restore gives the implicit commands their names back.
func (m implicitNames) restore() {
	for c, name := range m {
		c.Name = name
	}
}

// unnameImplicit blanks the names of the implicit commands under n, so that help prints their paths as their
// parent's, and returns their names.
func unnameImplicit(n *kong.Node) implicitNames {
	saved := implicitNames{}
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
	return saved
}

// style colors help on a terminal; its zero value writes plain text.
type style struct{ on bool }

// SGR codes of the help's styles: restrained, one color per kind of word.
const (
	sgrBold = "1"
	sgrDim  = "2"
	sgrCmd  = "36" // cyan
	sgrFlag = "32" // green
)

func (s style) paint(code, text string) string {
	if !s.on || text == "" {
		return text
	}
	return "\x1b[" + code + "m" + text + "\x1b[0m"
}

func (s style) head(t string) string { return s.paint(sgrBold, t) }
func (s style) cmd(t string) string  { return s.paint(sgrCmd, t) }
func (s style) arg(t string) string  { return s.paint(sgrDim, t) }
func (s style) flag(t string) string { return s.paint(sgrFlag, t) }

// paintUsage paints the words of a command line: flags, placeholders and arguments, and command words.
func (s style) paintUsage(line string) string {
	if !s.on {
		return line
	}
	words := strings.Split(line, " ")
	for i, w := range words {
		bare := strings.Trim(w, `"()`)
		switch {
		case bare == "|":
		case strings.HasPrefix(bare, "-"):
			name, value, ok := strings.Cut(w, "=")
			words[i] = s.flag(name)
			if ok {
				words[i] += s.flag("=") + s.arg(value)
			}
		case strings.ContainsAny(bare, "<>[]") || bare == "...":
			words[i] = s.arg(w)
		default:
			words[i] = s.cmd(w)
		}
	}
	return strings.Join(words, " ")
}

// helpStyle is the style of help written to w: --color, else $MILLSTREAM_COLOR, decides when it says always or
// never; otherwise (auto) help is colored on a terminal, unless $NO_COLOR is set or $TERM is dumb.
func helpStyle(rt *Runtime, w io.Writer) style {
	mode := rt.color
	if mode == "" || mode == "auto" {
		if v, _ := rt.Env("MILLSTREAM_COLOR"); v == "always" || v == "never" {
			mode = v
		}
	}
	switch mode {
	case "always":
		return style{on: true}
	case "never":
		return style{}
	}
	if v, _ := rt.Env("NO_COLOR"); v != "" {
		return style{}
	}
	if v, _ := rt.Env("TERM"); v == "dumb" {
		return style{}
	}
	isTerminal := rt.IsTerminal
	if isTerminal == nil {
		isTerminal = isCharDevice
	}
	return style{on: isTerminal(w)}
}

// isCharDevice says whether w is a file on a character device: a terminal, not a pipe or a regular file.
func isCharDevice(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// colorFlag is the --color value of a parse, for help printed before the parse applies it.
func colorFlag(kctx *kong.Context) string {
	for _, e := range kctx.Path {
		if e.Flag != nil && e.Flag.Name == "color" {
			if v, ok := kctx.FlagValue(e.Flag).(string); ok {
				return v
			}
		}
	}
	return ""
}
