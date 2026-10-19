// Package patch is the patch format of backtests and proposals (spec section 11.3): a unified diff against a
// package's files, in the shape git writes it, applied in memory to the files of the revision it names as its base.
//
// A patch is text. Lines before the first file are its preamble, which may state the base revision as a trailer,
// "Millstream-Base: <package hash>"; a patch applies only to the revision whose package has that hash. Each file
// starts at an optional "diff --git a/<path> b/<path>" line, its extended headers ("index", "new file mode 100644",
// "deleted file mode 100644"), then "--- a/<path>" or "--- /dev/null" and "+++ b/<path>" or "+++ /dev/null", then
// its hunks. Parse is strict, since what it lets through reaches a package:
//
//   - a path is relative, slash-separated and clean, never climbing out of the package, never under .git or
//     .millstream, never the environment file at the root (none of them is part of a package, spec section 4.9);
//   - a file is created, modified or deleted under one path: renames, copies, mode changes, symlinks and binary
//     contents are refused, and a created file's mode is 100644;
//   - a path appears once, and a hunk's line counts match its lines.
//
// Apply is as strict: each hunk applies at the lines it names, its context and removed lines matching exactly, with
// no offset and no fuzz, since the patch names the very revision it was written against.
package patch

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// BaseTrailer is the preamble trailer naming the patch's base revision by its package hash.
const BaseTrailer = "Millstream-Base"

// devNull names the missing side of a created or deleted file.
const devNull = "/dev/null"

// fileMode is the one mode a created or deleted file may have: a regular file, not executable.
const fileMode = "100644"

// ErrPatch marks a patch that does not parse, breaks a path rule, or does not apply to the files given.
var ErrPatch = errors.New("invalid patch")

// Op is what a patch does to a file.
type Op string

// Ops.
const (
	OpModify Op = "modify"
	OpCreate Op = "create"
	OpDelete Op = "delete"
)

// Patch is a parsed patch.
type Patch struct {
	// Base is the package hash of the revision the patch applies to, from its preamble; empty when it states none.
	Base string
	// Files are the files it changes, in the order it lists them.
	Files []File
}

// File is one file's change.
type File struct {
	// Path is slash-separated and relative to the package.
	Path  string
	Op    Op
	Hunks []Hunk
}

// Hunk is one "@@ -OldStart,OldLines +NewStart,NewLines @@" block.
type Hunk struct {
	OldStart, OldLines, NewStart, NewLines int
	Lines                                  []Line
}

// Line is a hunk's line: Kind is ' ' (context), '-' (removed) or '+' (added); Text holds its line terminator
// unless the patch marks it "\ No newline at end of file".
type Line struct {
	Kind byte
	Text string
}

// Hash is the patch's identity: the hex SHA-256 of its bytes, which a proposal records and publishing checks.
func Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Paths returns the paths the patch touches, sorted.
func (p *Patch) Paths() []string {
	out := make([]string, len(p.Files))
	for i, f := range p.Files {
		out[i] = f.Path
	}
	slices.Sort(out)
	return out
}

var (
	hunkHeader = regexp.MustCompile(`^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@`)
	hexHash    = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// errorf wraps ErrPatch.
func errorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrPatch, fmt.Sprintf(format, args...))
}

// Parse reads a patch.
func Parse(data []byte) (*Patch, error) {
	if !utf8.Valid(data) {
		return nil, errorf("not UTF-8 text")
	}
	lines := strings.SplitAfter(string(data), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	p := &parser{lines: lines}
	return p.parse()
}

// parser holds Parse's position.
type parser struct {
	lines []string
	i     int
}

func (ps *parser) peek() (string, bool) {
	if ps.i >= len(ps.lines) {
		return "", false
	}
	return strings.TrimRight(ps.lines[ps.i], "\r\n"), true
}

func (ps *parser) parse() (*Patch, error) {
	p := &Patch{}
	// The preamble: everything before the first file, where the base trailer may be.
	for {
		l, ok := ps.peek()
		if !ok || strings.HasPrefix(l, "diff --git ") || strings.HasPrefix(l, "--- ") {
			break
		}
		if v, found := strings.CutPrefix(l, BaseTrailer+":"); found {
			v = strings.TrimSpace(v)
			if !hexHash.MatchString(v) {
				return nil, errorf("line %d: %s must be a package hash (64 lowercase hex characters)", ps.i+1,
					BaseTrailer)
			}
			if p.Base != "" && p.Base != v {
				return nil, errorf("line %d: a second %s", ps.i+1, BaseTrailer)
			}
			p.Base = v
		}
		ps.i++
	}
	seen := map[string]bool{}
	for ps.i < len(ps.lines) {
		f, err := ps.file()
		if err != nil {
			return nil, err
		}
		if seen[f.Path] {
			return nil, errorf("%s: changed twice", f.Path)
		}
		seen[f.Path] = true
		p.Files = append(p.Files, f)
	}
	if len(p.Files) == 0 {
		return nil, errorf("no file changed")
	}
	return p, nil
}

// file reads one file's headers and hunks.
func (ps *parser) file() (File, error) {
	start := ps.i + 1
	var gitA, gitB string
	newMode, deletedMode := "", ""
	if l, _ := ps.peek(); strings.HasPrefix(l, "diff --git ") {
		var err error
		if gitA, gitB, err = gitPaths(strings.TrimPrefix(l, "diff --git ")); err != nil {
			return File{}, errorf("line %d: %v", start, err)
		}
		ps.i++
		for {
			l, ok := ps.peek()
			if !ok || strings.HasPrefix(l, "--- ") || strings.HasPrefix(l, "diff --git ") {
				break
			}
			switch {
			case strings.HasPrefix(l, "index "):
			case strings.HasPrefix(l, "new file mode "):
				newMode = strings.TrimPrefix(l, "new file mode ")
			case strings.HasPrefix(l, "deleted file mode "):
				deletedMode = strings.TrimPrefix(l, "deleted file mode ")
			case strings.HasPrefix(l, "old mode "), strings.HasPrefix(l, "new mode "):
				return File{}, errorf("line %d: %s: a mode change is refused", ps.i+1, gitB)
			case strings.HasPrefix(l, "similarity index "), strings.HasPrefix(l, "rename "),
				strings.HasPrefix(l, "copy "), strings.HasPrefix(l, "dissimilarity index "):
				return File{}, errorf("line %d: %s: renames and copies are refused", ps.i+1, gitB)
			case strings.HasPrefix(l, "Binary files "), l == "GIT binary patch":
				return File{}, errorf("line %d: %s: binary contents are refused", ps.i+1, gitB)
			default:
				return File{}, errorf("line %d: unexpected header %q", ps.i+1, l)
			}
			ps.i++
		}
		for _, m := range []string{newMode, deletedMode} {
			if m != "" && m != fileMode {
				return File{}, errorf("%s: mode %s is refused (only %s: symlinks and executables are refused)", gitB,
					m, fileMode)
			}
		}
	}
	oldPath, err := ps.header("--- ", "a/")
	if err != nil {
		return File{}, err
	}
	newPath, err := ps.header("+++ ", "b/")
	if err != nil {
		return File{}, err
	}
	f := File{Path: newPath, Op: OpModify}
	switch {
	case oldPath == devNull && newPath == devNull:
		return File{}, errorf("line %d: both sides are %s", start, devNull)
	case oldPath == devNull:
		f.Op = OpCreate
	case newPath == devNull:
		f.Op, f.Path = OpDelete, oldPath
	case oldPath != newPath:
		return File{}, errorf("line %d: %s becomes %s: renames are refused", start, oldPath, newPath)
	}
	if gitA != "" && (gitA != f.Path || gitB != f.Path) {
		return File{}, errorf("line %d: the diff --git paths do not name %s", start, f.Path)
	}
	if gitA != "" && ((newMode != "") != (f.Op == OpCreate) || (deletedMode != "") != (f.Op == OpDelete)) {
		return File{}, errorf("%s: its mode headers do not match a %s", f.Path, f.Op)
	}
	if err := CheckPath(f.Path); err != nil {
		return File{}, err
	}
	for {
		l, ok := ps.peek()
		if !ok || !strings.HasPrefix(l, "@@") {
			break
		}
		h, err := ps.hunk()
		if err != nil {
			return File{}, fmt.Errorf("%s: %w", f.Path, err)
		}
		f.Hunks = append(f.Hunks, h)
	}
	if len(f.Hunks) == 0 && f.Op == OpModify {
		return File{}, errorf("%s: no hunk", f.Path)
	}
	if f.Op != OpModify {
		if len(f.Hunks) > 1 || len(f.Hunks) == 1 && (f.Op == OpCreate && f.Hunks[0].OldLines != 0 ||
			f.Op == OpDelete && f.Hunks[0].NewLines != 0) {
			return File{}, errorf("%s: a %s holds one hunk from or to nothing", f.Path, f.Op)
		}
	}
	return f, nil
}

// gitPaths splits "a/<path> b/<path>", the two paths being equal: a rename is refused anyway.
func gitPaths(s string) (string, string, error) {
	a, b, ok := strings.Cut(s, " b/")
	if !ok || !strings.HasPrefix(a, "a/") {
		return "", "", errors.New("diff --git needs a/<path> b/<path>")
	}
	return strings.TrimPrefix(a, "a/"), b, nil
}

// header reads a "--- " or "+++ " line: /dev/null, or the path under its a/ or b/ prefix, a trailing tab and what
// follows it (diff -u's timestamp) ignored.
func (ps *parser) header(prefix, side string) (string, error) {
	l, ok := ps.peek()
	if !ok || !strings.HasPrefix(l, prefix) {
		return "", errorf("line %d: expected %q", ps.i+1, strings.TrimSpace(prefix)+" "+side+"<path>")
	}
	ps.i++
	p, _, _ := strings.Cut(strings.TrimPrefix(l, prefix), "\t")
	if p == devNull {
		return p, nil
	}
	rel, found := strings.CutPrefix(p, side)
	if !found {
		return "", errorf("line %d: %q must start with %s", ps.i, p, side)
	}
	return rel, nil
}

// hunk reads one hunk, checking its counts.
func (ps *parser) hunk() (Hunk, error) {
	l, _ := ps.peek()
	m := hunkHeader.FindStringSubmatch(l)
	if m == nil {
		return Hunk{}, errorf("line %d: malformed hunk header %q", ps.i+1, l)
	}
	num := func(s string) int {
		if s == "" {
			return 1
		}
		n, _ := strconv.Atoi(s)
		return n
	}
	h := Hunk{OldStart: num(m[1]), OldLines: num(m[2]), NewStart: num(m[3]), NewLines: num(m[4])}
	ps.i++
	oldN, newN := 0, 0
	for oldN < h.OldLines || newN < h.NewLines {
		if ps.i >= len(ps.lines) {
			return Hunk{}, errorf("line %d: the hunk ends early", ps.i)
		}
		raw := ps.lines[ps.i]
		text := strings.TrimSuffix(raw, "\n")
		kind, body := byte(' '), ""
		switch {
		case text == "": // an empty context line whose space an editor dropped
		case text[0] == ' ' || text[0] == '-' || text[0] == '+':
			kind, body = text[0], text[1:]
		default:
			return Hunk{}, errorf("line %d: %q is not a hunk line", ps.i+1, text)
		}
		ps.i++
		line := Line{Kind: kind, Text: body + "\n"}
		if l, ok := ps.peek(); ok && strings.HasPrefix(l, `\ `) {
			line.Text = body
			ps.i++
		}
		if kind != '+' {
			oldN++
		}
		if kind != '-' {
			newN++
		}
		h.Lines = append(h.Lines, line)
	}
	if oldN != h.OldLines || newN != h.NewLines {
		return Hunk{}, errorf("line %d: the hunk's lines do not match its header", ps.i)
	}
	return h, nil
}

// CheckPath refuses a path a patch may not touch: absolute, unclean, climbing out of the package, under .git or
// .millstream, or the environment file at the root.
func CheckPath(p string) error {
	switch {
	case p == "" || strings.ContainsAny(p, "\\\x00") || strings.HasPrefix(p, "/") || path.Clean(p) != p:
		return errorf("%q is not a clean relative path", p)
	case p == ".." || strings.HasPrefix(p, "../"):
		return errorf("%q climbs out of the package", p)
	}
	first, _, _ := strings.Cut(p, "/")
	if first == ".git" || first == ".millstream" || p == "millstream.yaml" {
		return errorf("%q is not part of a package", p)
	}
	return nil
}

// Apply returns files with the patch applied, files left unchanged.
func (p *Patch) Apply(files map[string][]byte) (map[string][]byte, error) {
	out := maps.Clone(files)
	for _, f := range p.Files {
		old, exists := files[f.Path]
		switch f.Op {
		case OpCreate:
			if exists {
				return nil, errorf("%s: created, but it exists", f.Path)
			}
		case OpDelete, OpModify:
			if !exists {
				return nil, errorf("%s: %s, but it does not exist", f.Path, map[Op]string{OpDelete: "deleted",
					OpModify: "modified"}[f.Op])
			}
		}
		body, err := apply(old, f.Hunks)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f.Path, err)
		}
		if f.Op == OpDelete {
			if len(body) > 0 {
				return nil, errorf("%s: deleted, but the patch removes only part of it", f.Path)
			}
			delete(out, f.Path)
			continue
		}
		out[f.Path] = body
	}
	return out, nil
}

// apply applies hunks to a file's content, each at the lines it names.
func apply(content []byte, hunks []Hunk) ([]byte, error) {
	lines := strings.SplitAfter(string(content), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	var b bytes.Buffer
	at := 0 // lines consumed so far
	for _, h := range hunks {
		start := h.OldStart - 1
		if h.OldLines == 0 { // an insertion after line OldStart
			start = h.OldStart
		}
		if start < at || start > len(lines) {
			return nil, errorf("the hunk at line %d is out of order or past the end", h.OldStart)
		}
		for _, l := range lines[at:start] {
			b.WriteString(l)
		}
		at = start
		for _, l := range h.Lines {
			if l.Kind == '+' {
				b.WriteString(l.Text)
				continue
			}
			if at >= len(lines) || lines[at] != l.Text {
				return nil, errorf("the hunk at line %d does not match line %d", h.OldStart, at+1)
			}
			if l.Kind == ' ' {
				b.WriteString(l.Text)
			}
			at++
		}
	}
	for _, l := range lines[at:] {
		b.WriteString(l)
	}
	return b.Bytes(), nil
}
