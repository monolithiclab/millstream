package proposals

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// newFile is the patch adding a file of these lines.
func newFile(p string, lines ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "diff --git a/%s b/%s\nnew file mode 100644\n--- /dev/null\n+++ b/%s\n@@ -0,0 +1,%d @@\n", p, p, p,
		len(lines))
	for _, l := range lines {
		b.WriteString("+" + l + "\n")
	}
	return b.String()
}

// edit is the patch replacing line at (1-based) of p, between a line of context on each side, with repl.
func edit(p string, at int, before, old, repl, after string) string {
	return fmt.Sprintf("diff --git a/%s b/%s\nindex 1111111..2222222 100644\n--- a/%s\n+++ b/%s\n@@ -%d,3 +%d,3 @@\n"+
		" %s\n-%s\n+%s\n %s\n", p, p, p, p, at-1, at-1, before, old, repl, after)
}

func TestApplyPatch(t *testing.T) {
	files := map[string][]byte{
		"a.txt":     []byte("one\ntwo\nthree\nfour\n"),
		"gone.txt":  []byte("x\ny\n"),
		"noeol.txt": []byte("first\nlast"),
	}
	tests := []struct {
		name, patch string
		want        map[string]string // path to content, "" for deleted
		rebased     string            // a line the rendered patch holds
	}{
		{"modify", edit("a.txt", 2, "one", "two", "TWO", "three"), map[string]string{"a.txt": "one\nTWO\nthree\nfour\n"},
			"@@ -1,3 +1,3 @@"},
		{"offset", edit("a.txt", 3, "two", "three", "THREE", "four"), map[string]string{"a.txt": "one\ntwo\nTHREE\nfour\n"},
			"@@ -2,3 +2,3 @@"},
		{"moved hunk", edit("a.txt", 9, "two", "three", "THREE", "four"),
			map[string]string{"a.txt": "one\ntwo\nTHREE\nfour\n"}, "@@ -2,3 +2,3 @@"},
		{"add", newFile("dir/new.txt", "hello", "world"), map[string]string{"dir/new.txt": "hello\nworld\n"},
			"new file mode 100644"},
		{"delete", "--- a/gone.txt\n+++ /dev/null\n@@ -1,2 +0,0 @@\n-x\n-y\n", map[string]string{"gone.txt": ""},
			"deleted file mode 100644"},
		{"no newline", "--- a/noeol.txt\n+++ b/noeol.txt\n@@ -1,2 +1,2 @@\n first\n-last\n\\ No newline at end of file\n" +
			"+last\n", map[string]string{"noeol.txt": "first\nlast\n"}, "\\ No newline at end of file"},
		{"blank context", "--- a/a.txt\n+++ b/a.txt\n@@ -1,2 +1,3 @@\n one\n+\n two\n",
			map[string]string{"a.txt": "one\n\ntwo\nthree\nfour\n"}, "@@ -1,2 +1,3 @@"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := ApplyPatch(files, tt.patch)
			if err != nil {
				t.Fatal(err)
			}
			for p, want := range tt.want {
				got, ok := a.Files[p]
				if want == "" && ok || want != "" && string(got) != want {
					t.Errorf("%s = %q (present %v), want %q", p, got, ok, want)
				}
			}
			if !strings.Contains(a.Rebased, tt.rebased) {
				t.Errorf("rendered patch lacks %q:\n%s", tt.rebased, a.Rebased)
			}
			again, err := ApplyPatch(files, a.Rebased) // the rendered patch applies at its lines, to the same result
			if err != nil || len(again.Files) != len(a.Files) || again.Rebased != a.Rebased {
				t.Fatalf("the rendered patch again: %v\n%s", err, again.Rebased)
			}
		})
	}
	if string(files["a.txt"]) != "one\ntwo\nthree\nfour\n" || len(files) != 3 {
		t.Fatal("ApplyPatch changed its input")
	}
}

// Every refusal wraps ErrPatch: what spec section 11.7 refuses whatever the kind, and what does not apply.
func TestApplyPatchRefusals(t *testing.T) {
	files := map[string][]byte{"a.txt": []byte("one\ntwo\nthree\n"), "b.txt": []byte("b\n")}
	tests := []struct{ name, patch, why string }{
		{"symlink", "diff --git a/l b/l\nnew file mode 120000\n--- /dev/null\n+++ b/l\n@@ -0,0 +1 @@\n+/etc/passwd\n",
			"symbolic link"},
		{"symlink index", "diff --git a/a.txt b/a.txt\nindex 1..2 120000\n--- a/a.txt\n+++ b/a.txt\n", "symbolic link"},
		{"mode change", "diff --git a/a.txt b/a.txt\nold mode 100644\nnew mode 100755\n", "mode change"},
		{"executable", "diff --git a/x b/x\nnew file mode 100755\n--- /dev/null\n+++ b/x\n@@ -0,0 +1 @@\n+x\n",
			"mode 100755"},
		{"rename", "diff --git a/a.txt b/a.txt\nsimilarity index 90%\nrename from a.txt\n", "renames"},
		{"rename by lines", "--- a/a.txt\n+++ b/c.txt\n@@ -1 +1 @@\n-one\n+uno\n", "renamed"},
		{"binary", "diff --git a/a.txt b/a.txt\nBinary files a/a.txt and b/a.txt differ\n", "binary"},
		{"climbs out", "--- a/../etc/passwd\n+++ b/../etc/passwd\n@@ -1 +1 @@\n-x\n+y\n", "inside the package"},
		{"absolute", "--- /etc/passwd\n+++ /etc/passwd\n@@ -1 +1 @@\n-x\n+y\n", "inside the package"},
		{"unclean", newFile("dir/../a.txt", "x"), "diff --git"},
		{"quoted", "--- \"a/x y\"\n+++ \"b/x y\"\n@@ -1 +1 @@\n-x\n+y\n", "quoted"},
		{"no file", "just words\n", "no file"},
		{"context", edit("a.txt", 2, "one", "TWO", "2", "three"), "does not match"},
		{"added exists", newFile("a.txt", "x"), "exists"},
		{"missing", edit("nope.txt", 2, "a", "b", "c", "d"), "does not exist"},
		{"twice", newFile("n.txt", "x") + newFile("n.txt", "y"), "twice"},
		{"short hunk", "--- a/a.txt\n+++ b/a.txt\n@@ -1,3 +1,3 @@\n one\n", "line counts"},
		{"long hunk", "--- a/b.txt\n+++ b/b.txt\n@@ -1 +1 @@\n-b\n-c\n+d\n", "more lines"},
		{"bad header", "--- a/a.txt\n+++ b/a.txt\n@@ -x +1 @@\n", "hunk header"},
		{"partial delete", "--- a/a.txt\n+++ /dev/null\n@@ -1 +0,0 @@\n-one\n", "leaves lines"},
		{"hunk first", "@@ -1 +1 @@\n-one\n+uno\n", "before its file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ApplyPatch(files, tt.patch)
			if !errors.Is(err, ErrPatch) || !strings.Contains(err.Error(), tt.why) {
				t.Fatalf("ApplyPatch = %v, want a refusal saying %q", err, tt.why)
			}
		})
	}
}
