package storetest

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestTestsListed checks that Tests lists every exported Test function of the build's files, once.
func TestTestsListed(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") || strings.HasSuffix(f, "_e2e.go") && !e2eBuild {
			continue
		}
		src, err := os.ReadFile(f) // #nosec G304 -- the package's own source
		if err != nil {
			t.Fatal(err)
		}
		for rest := string(src); ; {
			i := strings.Index(rest, "\nfunc Test")
			if i < 0 {
				break
			}
			rest = rest[i+len("\nfunc "):]
			if name, sig, ok := strings.Cut(rest, "("); ok && strings.HasPrefix(sig, "t *testing.T, open Opener) {") {
				want = append(want, name)
			}
		}
	}
	var got []string
	for _, tc := range Tests {
		got = append(got, tc.Name)
	}
	slices.Sort(got)
	slices.Sort(want)
	for _, name := range want {
		if !slices.Contains(got, name) {
			t.Errorf("Tests does not list %s", name)
		}
	}
	for _, name := range got {
		if !slices.Contains(want, name) {
			t.Errorf("Tests lists %s, which the build's sources do not define", name)
		}
	}
	if len(got) != len(want) {
		t.Errorf("Tests lists %d tests, the sources define %d: one is listed twice", len(got), len(want))
	}
}
