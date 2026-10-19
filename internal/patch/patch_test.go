package patch

import (
	"errors"
	"maps"
	"strings"
	"testing"
)

const base = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

var files = map[string][]byte{
	"pipeline.yaml":           []byte("pipeline: p\nsteps:\n  a: { action: x }\n  b: { action: y }\n"),
	"actions/score/prompt.md": []byte("Score it.\nBe strict."),
	"lessons/score.yaml":      []byte("lessons: []\n"),
}

// TestRoundTrip: Format writes what Parse reads and Apply turns into the new files: a modification, a file without a
// final newline, a creation and a deletion, with the base trailer.
func TestRoundTrip(t *testing.T) {
	after := maps.Clone(files)
	after["pipeline.yaml"] = []byte("pipeline: p\nsteps:\n  a: { action: x }\n  b: { action: z }\n")
	after["actions/score/prompt.md"] = []byte("Score it.\nBe lenient.")
	after["actions/score/lessons.md"] = []byte("one\ntwo\n")
	delete(after, "lessons/score.yaml")
	data, err := Format(base, files, after)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse:\n%s\n%v", data, err)
	}
	if p.Base != base {
		t.Errorf("Base = %q", p.Base)
	}
	want := []string{"actions/score/lessons.md", "actions/score/prompt.md", "lessons/score.yaml", "pipeline.yaml"}
	if got := p.Paths(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("Paths = %v", got)
	}
	got, err := p.Apply(files)
	if err != nil {
		t.Fatal(err)
	}
	if !maps.EqualFunc(got, after, func(a, b []byte) bool { return string(a) == string(b) }) {
		t.Errorf("Apply = %q, want %q", got, after)
	}
	if again, _ := Format(base, after, after); again != nil {
		t.Errorf("Format of equal files = %q", again)
	}
	if Hash(data) == Hash(append(data, ' ')) || len(Hash(data)) != 64 {
		t.Error("Hash does not identify the patch")
	}
}

// TestHunksWithContext: a long file changed in two distant places gets two hunks, each with three lines of context,
// and applies; a change near the start names the right lines.
func TestHunksWithContext(t *testing.T) {
	var a, b strings.Builder
	for i := range 30 {
		line := strings.Repeat("x", i) + "\n"
		a.WriteString(line)
		switch i {
		case 1:
			b.WriteString("inserted\n" + line)
		case 20:
			b.WriteString("changed\n")
		default:
			b.WriteString(line)
		}
	}
	before := map[string][]byte{"f.txt": []byte(a.String())}
	after := map[string][]byte{"f.txt": []byte(b.String())}
	data, err := Format("", before, after)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(data), "\n@@ "); n != 2 || !strings.Contains(string(data), "@@ -1,4 +1,5 @@") {
		t.Fatalf("hunks:\n%s", data)
	}
	p, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Apply(before)
	if err != nil || string(got["f.txt"]) != b.String() {
		t.Fatalf("Apply = %q, %v", got["f.txt"], err)
	}
}

// TestParseRefusals: every rule Parse enforces refuses with ErrPatch.
func TestParseRefusals(t *testing.T) {
	hunk := "@@ -1 +1 @@\n-a\n+b\n"
	for name, text := range map[string]string{
		"empty":         "just a message\n",
		"absolute":      "--- a//etc/passwd\n+++ b//etc/passwd\n" + hunk,
		"climbs":        "--- a/../x\n+++ b/../x\n" + hunk,
		"unclean":       "--- a/x/./y\n+++ b/x/./y\n" + hunk,
		"git dir":       "--- a/.git/config\n+++ b/.git/config\n" + hunk,
		"state dir":     "--- a/.millstream/db\n+++ b/.millstream/db\n" + hunk,
		"env file":      "--- a/millstream.yaml\n+++ b/millstream.yaml\n" + hunk,
		"backslash":     "--- a/x\\y\n+++ b/x\\y\n" + hunk,
		"rename":        "--- a/x\n+++ b/y\n" + hunk,
		"git rename":    "diff --git a/x b/y\nsimilarity index 90%\nrename from x\nrename to y\n",
		"mode change":   "diff --git a/x b/x\nold mode 100644\nnew mode 100755\n",
		"symlink":       "diff --git a/x b/x\nnew file mode 120000\n--- /dev/null\n+++ b/x\n@@ -0,0 +1 @@\n+target\n",
		"executable":    "diff --git a/x b/x\nnew file mode 100755\n--- /dev/null\n+++ b/x\n@@ -0,0 +1 @@\n+#!/bin/sh\n",
		"binary":        "diff --git a/x b/x\nBinary files a/x and b/x differ\n",
		"no prefix":     "--- x\n+++ x\n" + hunk,
		"both null":     "--- /dev/null\n+++ /dev/null\n" + hunk,
		"twice":         "--- a/x\n+++ b/x\n" + hunk + "--- a/x\n+++ b/x\n" + hunk,
		"no hunk":       "--- a/x\n+++ b/x\n",
		"short hunk":    "--- a/x\n+++ b/x\n@@ -1,2 +1,2 @@\n-a\n+b\n",
		"bad line":      "--- a/x\n+++ b/x\n@@ -1 +1 @@\n*a\n+b\n",
		"bad header":    "--- a/x\n+++ b/x\n@@ -x +1 @@\n",
		"bad base":      "Millstream-Base: nope\n--- a/x\n+++ b/x\n" + hunk,
		"two bases":     "Millstream-Base: " + base + "\nMillstream-Base: " + strings.Repeat("f", 64) + "\n",
		"mode mismatch": "diff --git a/x b/x\n--- /dev/null\n+++ b/x\n@@ -0,0 +1 @@\n+a\n",
		"not utf8":      "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-\xff\n+b\n",
	} {
		if _, err := Parse([]byte(text)); !errors.Is(err, ErrPatch) {
			t.Errorf("%s: Parse = %v, want ErrPatch", name, err)
		}
	}
}

// TestApplyRefusals: a patch applies only where its lines match exactly.
func TestApplyRefusals(t *testing.T) {
	for name, text := range map[string]string{
		"context differs": "--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -1 +1 @@\n-pipeline: q\n+pipeline: r\n",
		"past the end":    "--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -9 +9 @@\n-x\n+y\n",
		"missing file":    "--- a/nope\n+++ b/nope\n@@ -1 +1 @@\n-x\n+y\n",
		"create exists": "diff --git a/lessons/score.yaml b/lessons/score.yaml\nnew file mode 100644\n" +
			"--- /dev/null\n+++ b/lessons/score.yaml\n@@ -0,0 +1 @@\n+x\n",
		"partial delete": "diff --git a/pipeline.yaml b/pipeline.yaml\ndeleted file mode 100644\n" +
			"--- a/pipeline.yaml\n+++ /dev/null\n@@ -1 +0,0 @@\n-pipeline: p\n",
		"no newline differs": "--- a/actions/score/prompt.md\n+++ b/actions/score/prompt.md\n" +
			"@@ -2 +2 @@\n-Be strict.\n+Be kind.\n",
	} {
		p, err := Parse([]byte(text))
		if err != nil {
			t.Fatalf("%s: Parse: %v", name, err)
		}
		if _, err := p.Apply(files); !errors.Is(err, ErrPatch) {
			t.Errorf("%s: Apply = %v, want ErrPatch", name, err)
		}
	}
}

// TestParseDiffU: a patch as diff -u writes it (timestamps after a tab, no diff --git line) applies too.
func TestParseDiffU(t *testing.T) {
	text := "--- a/lessons/score.yaml\t2026-10-19 10:00:00\n+++ b/lessons/score.yaml\t2026-10-19 10:01:00\n" +
		"@@ -1 +1,2 @@\n-lessons: []\n+lessons:\n+  - be strict\n"
	p, err := Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Apply(files)
	if err != nil || string(got["lessons/score.yaml"]) != "lessons:\n  - be strict\n" {
		t.Fatalf("Apply = %q, %v", got["lessons/score.yaml"], err)
	}
	if p.Base != "" {
		t.Errorf("Base = %q", p.Base)
	}
}
