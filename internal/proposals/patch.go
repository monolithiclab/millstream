package proposals

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// A proposal's patch is a unified diff of the package, as git diff writes it (a/ and b/ prefixes, /dev/null for a
// file added or deleted), applied in memory to the base revision's files. Hunks apply only where their context
// matches exactly, at their line or the nearest offset: no fuzz. A patch that changes a file mode, adds a symbolic
// link or a binary file, renames or copies, or names a path outside the package is refused (spec section 11.7).
//
// ApplyPatch is the one entry point the rest of the engine uses, so the patch format can change behind it.

// ErrPatch is wrapped by every refusal of a patch: malformed, outside the package, a mode change or a symbolic
// link, or a hunk whose context does not match.
var ErrPatch = errors.New("patch refused")

// Change is one file a patch touches.
type Change struct {
	Path string
	Op   Op
}

// Op is what a patch does to a file.
type Op string

const (
	OpAdd    Op = "add"
	OpModify Op = "modify"
	OpDelete Op = "delete"
)

// Applied is a patch applied to a package's files: the files after it, what it changed, and Rebased, the patch
// rendered again with each hunk at the line where it applied: the patch rebased onto those files.
type Applied struct {
	Files   map[string][]byte
	Changes []Change // by path
	Rebased string
}

// Paths returns the paths the patch touches, sorted.
func (a Applied) Paths() []string {
	out := make([]string, len(a.Changes))
	for i, c := range a.Changes {
		out[i] = c.Path
	}
	return out
}

// ApplyPatch applies a unified diff to a package's files, which it does not change.
func ApplyPatch(files map[string][]byte, patch string) (Applied, error) {
	fps, err := parsePatch(patch)
	if err != nil {
		return Applied{}, err
	}
	out := Applied{Files: maps.Clone(files)}
	var rendered strings.Builder
	seen := map[string]bool{}
	for _, fp := range fps {
		p := cmp.Or(fp.newPath, fp.oldPath)
		if seen[p] {
			return Applied{}, refusef("%s is patched twice", p)
		}
		seen[p] = true
		old, exists := files[p]
		op := OpModify
		switch {
		case fp.oldPath == "":
			op = OpAdd
			if exists {
				return Applied{}, refusef("%s is added but exists", p)
			}
		case fp.newPath == "":
			op = OpDelete
			if !exists {
				return Applied{}, refusef("%s is deleted but does not exist", p)
			}
		case !exists:
			return Applied{}, refusef("%s does not exist", p)
		}
		content, err := fp.apply(old)
		if err != nil {
			return Applied{}, fmt.Errorf("%s: %w", p, err)
		}
		if op == OpDelete {
			if len(content) > 0 {
				return Applied{}, refusef("%s is deleted but the patch leaves lines in it", p)
			}
			delete(out.Files, p)
		} else {
			out.Files[p] = content
		}
		out.Changes = append(out.Changes, Change{Path: p, Op: op})
		fp.render(&rendered)
	}
	slices.SortFunc(out.Changes, func(a, b Change) int { return strings.Compare(a.Path, b.Path) })
	out.Rebased = rendered.String()
	return out, nil
}

func refusef(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrPatch}, args...)...)
}

// filePatch is one file's part of a patch. A path is empty for /dev/null.
type filePatch struct {
	oldPath, newPath string
	hunks            []hunk
}

// hunk is one @@ section: its lines keep their ' ', '-' or '+' prefix. oldNoEOL and newNoEOL say the last line of
// that side has no newline.
type hunk struct {
	oldStart, oldLines, newStart, newLines int
	lines                                  []string
	oldNoEOL, newNoEOL                     bool
}

// regularMode is the only file mode a patch may name: a regular, non-executable file.
const regularMode = "100644"

// parsePatch parses a unified diff into its files, refusing what the path rules refuse whatever the kind.
func parsePatch(patch string) ([]filePatch, error) {
	lines := strings.Split(patch, "\n")
	if n := len(lines); n > 0 && lines[n-1] == "" {
		lines = lines[:n-1]
	}
	var (
		out []filePatch
		cur *filePatch
		git bool // in a git header, before its ---/+++ lines
	)
	flush := func() {
		if cur != nil {
			out = append(out, *cur)
			cur = nil
		}
	}
	for i := 0; i < len(lines); i++ {
		l := lines[i]
		switch {
		case strings.HasPrefix(l, "diff --git "):
			flush()
			p, ok := gitPath(strings.TrimPrefix(l, "diff --git "))
			if !ok {
				return nil, refusef("line %d: %q: want diff --git a/<path> b/<path>, the same path", i+1, l)
			}
			cur, git = &filePatch{oldPath: p, newPath: p}, true
		case git && (strings.HasPrefix(l, "old mode ") || strings.HasPrefix(l, "new mode ")):
			return nil, refusef("%s: a file mode change", cur.newPath)
		case git && (strings.HasPrefix(l, "new file mode ") || strings.HasPrefix(l, "deleted file mode ")):
			mode := l[strings.LastIndexByte(l, ' ')+1:]
			if err := checkMode(cur.newPath, mode); err != nil {
				return nil, err
			}
			if strings.HasPrefix(l, "new") {
				cur.oldPath = ""
			} else {
				cur.newPath = ""
			}
		case git && strings.HasPrefix(l, "index "):
			if f := strings.Fields(l); len(f) == 3 {
				if err := checkMode(cmp.Or(cur.newPath, cur.oldPath), f[2]); err != nil {
					return nil, err
				}
			}
		case git && (strings.HasPrefix(l, "rename ") || strings.HasPrefix(l, "copy ") ||
			strings.HasPrefix(l, "similarity index") || strings.HasPrefix(l, "dissimilarity index")):
			return nil, refusef("%s: renames and copies are refused", cur.newPath)
		case strings.HasPrefix(l, "Binary files ") || l == "GIT binary patch":
			return nil, refusef("line %d: binary files are refused", i+1)
		case strings.HasPrefix(l, "--- ") && i+1 < len(lines) && strings.HasPrefix(lines[i+1], "+++ "):
			oldP, err := diffPath(l[4:], "a/")
			if err != nil {
				return nil, err
			}
			newP, err := diffPath(lines[i+1][4:], "b/")
			if err != nil {
				return nil, err
			}
			if oldP != "" && newP != "" && oldP != newP {
				return nil, refusef("%s is renamed to %s: renames are refused", oldP, newP)
			}
			if oldP == "" && newP == "" {
				return nil, refusef("line %d: both sides are /dev/null", i+1)
			}
			if git {
				if (cur.oldPath == "") != (oldP == "") || (cur.newPath == "") != (newP == "") ||
					cmp.Or(cur.newPath, cur.oldPath) != cmp.Or(newP, oldP) {
					return nil, refusef("line %d: the ---/+++ lines disagree with the diff --git header", i+1)
				}
			} else {
				flush()
				cur = &filePatch{}
			}
			cur.oldPath, cur.newPath, git = oldP, newP, false
			i++
		case strings.HasPrefix(l, "@@ "):
			if cur == nil || git {
				return nil, refusef("line %d: a hunk before its file's ---/+++ lines", i+1)
			}
			h, next, err := parseHunk(lines, i)
			if err != nil {
				return nil, err
			}
			cur.hunks = append(cur.hunks, h)
			i = next - 1
		default:
			// A preamble, a trailer, or a header line that changes nothing (a git extended header we accept).
		}
	}
	flush()
	if len(out) == 0 {
		return nil, refusef("no file in the patch")
	}
	return out, nil
}

// checkMode refuses any mode but a regular file's: a symbolic link (120000), an executable or anything else.
func checkMode(p, mode string) error {
	switch mode {
	case regularMode:
		return nil
	case "120000":
		return refusef("%s: symbolic links are refused", p)
	}
	return refusef("%s: file mode %s is refused: only %s", p, mode, regularMode)
}

// gitPath reads the path of a diff --git line: "a/<p> b/<p>", the same path on both sides.
func gitPath(s string) (string, bool) {
	n := (len(s) - 5) / 2
	if n <= 0 || len(s) != 5+2*n || !strings.HasPrefix(s, "a/") || s[2+n:5+n] != " b/" || s[2:2+n] != s[5+n:] {
		return "", false
	}
	p := s[2 : 2+n]
	return p, checkPath(p) == nil
}

// diffPath reads the path of a ---/+++ line, without its prefix and any timestamp: "" for /dev/null.
func diffPath(s, prefix string) (string, error) {
	if i := strings.IndexByte(s, '\t'); i >= 0 {
		s = s[:i]
	}
	if s == "/dev/null" {
		return "", nil
	}
	if strings.HasPrefix(s, `"`) {
		return "", refusef("%s: quoted paths are refused", s)
	}
	s = strings.TrimPrefix(s, prefix)
	return s, checkPath(s)
}

// checkPath refuses a path that is not a clean, relative path inside the package.
func checkPath(p string) error {
	if p == "" || path.Clean(p) != p || !filepath.IsLocal(filepath.FromSlash(p)) || strings.ContainsRune(p, '\\') ||
		strings.HasPrefix(p, "/") {
		return refusef("%q is not a clean path inside the package", p)
	}
	return nil
}

// parseHunk parses the hunk whose header is lines[i] and returns it with the index of the line after it.
func parseHunk(lines []string, i int) (hunk, int, error) {
	var h hunk
	header := lines[i]
	end := strings.Index(header[3:], " @@")
	if end < 0 {
		return h, 0, refusef("line %d: %q is not a hunk header", i+1, header)
	}
	f := strings.Fields(header[3 : 3+end])
	if len(f) != 2 || !strings.HasPrefix(f[0], "-") || !strings.HasPrefix(f[1], "+") {
		return h, 0, refusef("line %d: %q is not a hunk header", i+1, header)
	}
	var err1, err2 error
	h.oldStart, h.oldLines, err1 = hunkRange(f[0][1:])
	h.newStart, h.newLines, err2 = hunkRange(f[1][1:])
	if err1 != nil || err2 != nil {
		return h, 0, refusef("line %d: %q is not a hunk header", i+1, header)
	}
	oldLeft, newLeft := h.oldLines, h.newLines
	j := i + 1
	last := byte(0)
	for ; j < len(lines) && (oldLeft > 0 || newLeft > 0 || strings.HasPrefix(lines[j], `\`)); j++ {
		l := lines[j]
		if l == "" {
			l = " " // a blank context line some tools write without its space
		}
		switch l[0] {
		case ' ':
			oldLeft, newLeft = oldLeft-1, newLeft-1
		case '-':
			oldLeft--
		case '+':
			newLeft--
		case '\\':
			switch last {
			case ' ':
				h.oldNoEOL, h.newNoEOL = true, true
			case '-':
				h.oldNoEOL = true
			case '+':
				h.newNoEOL = true
			default:
				return h, 0, refusef("line %d: a no-newline marker after no line", j+1)
			}
			continue
		default:
			return h, 0, refusef("line %d: %q in a hunk", j+1, l)
		}
		if oldLeft < 0 || newLeft < 0 {
			return h, 0, refusef("line %d: the hunk has more lines than its header says", j+1)
		}
		h.lines, last = append(h.lines, l), l[0]
	}
	if oldLeft > 0 || newLeft > 0 {
		return h, 0, refusef("line %d: the hunk ends before its header's line counts", j)
	}
	return h, j, nil
}

// hunkRange reads "start,count" or "start" (count 1).
func hunkRange(s string) (int, int, error) {
	a, b, found := strings.Cut(s, ",")
	start, err := strconv.Atoi(a)
	if err != nil || start < 0 {
		return 0, 0, errors.New("bad start")
	}
	if !found {
		return start, 1, nil
	}
	n, err := strconv.Atoi(b)
	if err != nil || n < 0 {
		return 0, 0, errors.New("bad count")
	}
	return start, n, nil
}

// side returns a hunk's lines on one side: the old one (context and removed) or the new one (context and added).
func (h hunk) side(added bool) []string {
	var out []string
	for _, l := range h.lines {
		if l[0] == ' ' || (added && l[0] == '+') || (!added && l[0] == '-') {
			out = append(out, l[1:])
		}
	}
	return out
}

// apply applies the file's hunks to content, recording where each one applied for render.
func (fp *filePatch) apply(content []byte) ([]byte, error) {
	src, eol := splitLines(string(content))
	var out []string
	cursor := 0
	for k := range fp.hunks {
		h := &fp.hunks[k]
		old, repl := h.side(false), h.side(true)
		want := h.oldStart - 1
		if len(old) == 0 {
			want = h.oldStart
		}
		pos := find(src, old, cursor, want)
		if pos < 0 {
			return nil, refusef("hunk %d (@@ -%d,%d) does not match the file", k+1, h.oldStart, h.oldLines)
		}
		atEnd := pos+len(old) == len(src)
		if h.oldNoEOL && (!atEnd || eol) {
			return nil, refusef("hunk %d: its last line has no newline, unlike the file's", k+1)
		}
		if h.newNoEOL && !atEnd {
			return nil, refusef("hunk %d: a line without a newline before the end of the file", k+1)
		}
		out = append(out, src[cursor:pos]...)
		h.oldStart, h.newStart = pos+1, len(out)+1
		if len(old) == 0 {
			h.oldStart = pos
		}
		if len(repl) == 0 {
			h.newStart = len(out)
		}
		out = append(out, repl...)
		cursor = pos + len(old)
		if atEnd {
			eol = !h.newNoEOL
		}
	}
	out = append(out, src[cursor:]...)
	if len(out) == 0 {
		return []byte{}, nil
	}
	s := strings.Join(out, "\n")
	if eol {
		s += "\n"
	}
	return []byte(s), nil
}

// splitLines splits text into lines without their newlines, and says whether it ends with one (an empty text
// does).
func splitLines(s string) ([]string, bool) {
	if s == "" {
		return nil, true
	}
	eol := strings.HasSuffix(s, "\n")
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n"), eol
}

// find returns where block appears in src at or after from, the nearest to want, or -1.
func find(src, block []string, from, want int) int {
	last := len(src) - len(block)
	if last < from {
		return -1
	}
	match := func(at int) bool { return at >= from && at <= last && slices.Equal(src[at:at+len(block)], block) }
	for d := 0; want-d >= from || want+d <= last; d++ {
		if match(want - d) {
			return want - d
		}
		if match(want + d) {
			return want + d
		}
	}
	return -1
}

// render writes the file's part of a patch, in git's format, its hunks at the lines where they applied.
func (fp *filePatch) render(b *strings.Builder) {
	p := cmp.Or(fp.newPath, fp.oldPath)
	fmt.Fprintf(b, "diff --git a/%s b/%s\n", p, p)
	oldName, newName := "a/"+p, "b/"+p
	switch {
	case fp.oldPath == "":
		fmt.Fprintf(b, "new file mode %s\n", regularMode)
		oldName = "/dev/null"
	case fp.newPath == "":
		fmt.Fprintf(b, "deleted file mode %s\n", regularMode)
		newName = "/dev/null"
	}
	fmt.Fprintf(b, "--- %s\n+++ %s\n", oldName, newName)
	for _, h := range fp.hunks {
		fmt.Fprintf(b, "@@ -%d,%d +%d,%d @@\n", h.oldStart, h.oldLines, h.newStart, h.newLines)
		for i, l := range h.lines {
			b.WriteString(l)
			b.WriteByte('\n')
			if h.noEOLAfter(i) {
				b.WriteString("\\ No newline at end of file\n")
			}
		}
	}
}

// noEOLAfter reports whether the no-newline marker follows line i: the last line of a side without a newline.
func (h hunk) noEOLAfter(i int) bool {
	kind := h.lines[i][0]
	lastOf := func(sideKind byte) bool {
		for _, l := range h.lines[i+1:] {
			if l[0] == ' ' || l[0] == sideKind {
				return false
			}
		}
		return true
	}
	switch kind {
	case ' ':
		return (h.oldNoEOL || h.newNoEOL) && lastOf('-') && lastOf('+')
	case '-':
		return h.oldNoEOL && lastOf('-')
	case '+':
		return h.newNoEOL && lastOf('+')
	}
	return false
}
