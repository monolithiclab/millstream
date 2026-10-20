package cli

import (
	_ "embed"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/alecthomas/kong"
)

// guide is the guide millstream help prints, and its topics: Markdown that reads as documentation as well
// (docs/tutorial.md links it). The text before the first ### heading is the guide; each ### section is a topic.
//
//go:embed guide.md
var guide string

// HelpCmd is "millstream help [topic | command…]".
type HelpCmd struct {
	Subject []string `arg:"" optional:"" name:"topic-or-command" help:"A topic (help topics lists them), or a command: help run report prints run report --help."`
}

// Run prints the guide, the list of topics, a topic, or a command's help as its --help does.
func (c HelpCmd) Run(rt *Runtime, k *kong.Kong) error {
	s := helpStyle(rt, rt.IO.Out)
	main, topics := guideSections()
	switch {
	case len(c.Subject) == 0:
		_, err := io.WriteString(rt.IO.Out, renderMarkdown(s, main))
		return err
	case len(c.Subject) == 1 && c.Subject[0] == "topics":
		h := &helpWriter{style: s}
		h.line(h.head("Topics:"))
		var rows []row
		for _, t := range topics {
			rows = append(rows, h.row(h.cmd(t.name), t.name, t.summary))
		}
		h.columns(rows, columnWidth(rows))
		h.line("")
		h.linef("Run %s to read one.", s.paintUsage(`"millstream help <topic>"`))
		_, err := io.WriteString(rt.IO.Out, h.String())
		return err
	case len(c.Subject) == 1:
		for _, t := range topics {
			if t.name == c.Subject[0] {
				_, err := io.WriteString(rt.IO.Out, renderMarkdown(s, t.text))
				return err
			}
		}
	}
	trace, err := kong.Trace(k, append(slices.Clone(c.Subject), "--help"))
	if err != nil || trace.Error != nil || trace.Selected() == nil {
		return usageError(fmt.Sprintf("help: no command or topic %q (millstream help topics lists the topics)",
			strings.Join(c.Subject, " ")))
	}
	return trace.PrintUsage(false)
}

// topic is a section of the guide millstream help <name> prints.
type topic struct {
	name, summary, text string
}

// guideSections splits the guide into its main text and its topics.
func guideSections() (main string, topics []topic) {
	parts := strings.Split(guide, "\n### ")
	for _, p := range parts[1:] {
		name, body, _ := strings.Cut(p, "\n")
		body = strings.TrimSpace(body)
		summary, _, _ := strings.Cut(body, "\n")
		topics = append(topics, topic{name: name, summary: summary, text: "### " + name + "\n\n" + body + "\n"})
	}
	return parts[0], topics
}

// renderMarkdown renders the guide's Markdown for a terminal: headings bold without their marks, code spans as
// commands without their backticks, emphasis unmarked, fenced code indented with its fences dropped.
func renderMarkdown(s style, md string) string {
	var b strings.Builder
	fenced := false
	for l := range strings.SplitSeq(strings.TrimSpace(md), "\n") {
		switch {
		case strings.HasPrefix(l, "```"):
			fenced = !fenced
			continue
		case fenced:
			if i := strings.Index(l, " #"); i >= 0 {
				l = "  " + s.paintUsage(l[:i+1]) + s.arg(l[i+1:])
			} else {
				l = "  " + s.paintUsage(l)
			}
		case strings.HasPrefix(l, "#"):
			l = s.head(strings.TrimLeft(l, "# "))
		default:
			l = renderInline(s, l)
		}
		b.WriteString(l + "\n")
	}
	return b.String()
}

// renderInline drops the marks of code spans, painting them as commands, and of strong emphasis.
func renderInline(s style, l string) string {
	l = strings.ReplaceAll(l, "**", "")
	var b strings.Builder
	for i, part := range strings.Split(l, "`") {
		if i%2 == 1 {
			part = s.cmd(part)
		}
		b.WriteString(part)
	}
	return b.String()
}
