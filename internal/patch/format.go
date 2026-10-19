package patch

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// context is how many unchanged lines surround a change in the hunks Format writes.
const context = 3

// maxCells bounds the line comparison of one file (its lines times the other's): past it, the file is written as
// one hunk replacing all of it, which applies just as well.
const maxCells = 4 << 20

// Format writes the patch turning before into after, as git diff writes it, with base as its Millstream-Base
// trailer when not empty. It returns nil when nothing differs. Paths are checked as Parse checks them.
func Format(base string, before, after map[string][]byte) ([]byte, error) {
	var b strings.Builder
	if base != "" {
		fmt.Fprintf(&b, "%s: %s\n\n", BaseTrailer, base)
	}
	changed := false
	paths := slices.Sorted(maps.Keys(before))
	for p := range after {
		if _, ok := before[p]; !ok {
			paths = append(paths, p)
		}
	}
	slices.Sort(paths)
	for _, p := range paths {
		old, inOld := before[p]
		cur, inNew := after[p]
		if inOld && inNew && string(old) == string(cur) {
			continue
		}
		if err := CheckPath(p); err != nil {
			return nil, err
		}
		changed = true
		fmt.Fprintf(&b, "diff --git a/%s b/%s\n", p, p)
		from, to := "a/"+p, "b/"+p
		switch {
		case !inOld:
			fmt.Fprintf(&b, "new file mode %s\n", fileMode)
			from = devNull
		case !inNew:
			fmt.Fprintf(&b, "deleted file mode %s\n", fileMode)
			to = devNull
		}
		oldLines, newLines := split(old), split(cur)
		if len(oldLines) == 0 && len(newLines) == 0 {
			continue // an empty file created or deleted: git writes no --- and +++ either
		}
		fmt.Fprintf(&b, "--- %s\n+++ %s\n", from, to)
		for _, h := range hunks(oldLines, newLines) {
			writeHunk(&b, h)
		}
	}
	if !changed {
		return nil, nil
	}
	return []byte(b.String()), nil
}

// split cuts content into lines holding their terminators.
func split(content []byte) []string {
	lines := strings.SplitAfter(string(content), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// edit is one line of the comparison: kind ' ', '-' or '+', and its line numbers on each side (0-based).
type edit struct {
	kind     byte
	old, new int
}

// script compares a and b by their longest common subsequence of lines.
func script(a, b []string) []edit {
	n, m := len(a), len(b)
	if n*m > maxCells {
		var out []edit
		for i := range a {
			out = append(out, edit{kind: '-', old: i})
		}
		for j := range b {
			out = append(out, edit{kind: '+', new: j})
		}
		return out
	}
	// lcs[i][j] is the length of the longest common subsequence of a[i:] and b[j:].
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	var out []edit
	i, j := 0, 0
	for i < n || j < m {
		switch {
		case i < n && j < m && a[i] == b[j]:
			out = append(out, edit{kind: ' ', old: i, new: j})
			i++
			j++
		case i < n && (j == m || lcs[i+1][j] >= lcs[i][j+1]): // removals first, as git writes them
			out = append(out, edit{kind: '-', old: i, new: j})
			i++
		default:
			out = append(out, edit{kind: '+', old: i, new: j})
			j++
		}
	}
	return out
}

// hunks groups the comparison's changes with their context into hunks.
func hunks(a, b []string) []Hunk {
	es := script(a, b)
	var out []Hunk
	for i := 0; i < len(es); {
		if es[i].kind == ' ' {
			i++
			continue
		}
		// A hunk starts context lines before this change and runs until context*2 unchanged lines in a row.
		start := max(i-context, 0)
		for start > 0 && es[start-1].kind != ' ' {
			start--
		}
		end := i
		for end < len(es) {
			if es[end].kind != ' ' {
				end++
				continue
			}
			run := end
			for run < len(es) && es[run].kind == ' ' {
				run++
			}
			if run == len(es) || run-end > 2*context {
				end = min(end+context, run)
				break
			}
			end = run
		}
		h := Hunk{OldStart: -1, NewStart: -1}
		for _, e := range es[start:end] {
			switch e.kind {
			case ' ':
				h.Lines = append(h.Lines, Line{Kind: ' ', Text: a[e.old]})
				h.OldLines++
				h.NewLines++
			case '-':
				h.Lines = append(h.Lines, Line{Kind: '-', Text: a[e.old]})
				h.OldLines++
			case '+':
				h.Lines = append(h.Lines, Line{Kind: '+', Text: b[e.new]})
				h.NewLines++
			}
		}
		first := es[start]
		h.OldStart, h.NewStart = first.old+1, first.new+1
		if h.OldLines == 0 {
			h.OldStart-- // an insertion names the line it follows
		}
		if h.NewLines == 0 {
			h.NewStart--
		}
		out = append(out, h)
		i = end
	}
	return out
}

// writeHunk writes a hunk with its header, marking a last line without a terminator.
func writeHunk(b *strings.Builder, h Hunk) {
	fmt.Fprintf(b, "@@ -%s +%s @@\n", span(h.OldStart, h.OldLines), span(h.NewStart, h.NewLines))
	for _, l := range h.Lines {
		b.WriteByte(l.Kind)
		b.WriteString(l.Text)
		if !strings.HasSuffix(l.Text, "\n") {
			b.WriteString("\n\\ No newline at end of file\n")
		}
	}
}

// span is a hunk header's side: start, and the count unless it is 1.
func span(start, n int) string {
	if n == 1 {
		return fmt.Sprint(start)
	}
	return fmt.Sprintf("%d,%d", start, n)
}
