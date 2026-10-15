// Package containertest writes a fake container runtime CLI: a shell script that answers image inspect and image
// ls from canned data and records every other invocation, so tests of image resolution and the lock file run
// without Docker. The script uses shell builtins only, so it works whatever PATH the test gives the runtime.
package containertest

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Image is a canned local image.
type Image struct {
	ID          string
	RepoDigests []string
	Labels      map[string]string
}

// Write writes the fake CLI as dir/name, with images by reference (repository:tag) and the tags of each
// repository, and returns its path. Invocations other than image inspect and image ls are appended to dir/calls.
func Write(dir, name string, images map[string]Image, tags map[string][]string) (string, error) {
	var b strings.Builder
	b.WriteString("#!/bin/sh\ncase \"$1 $2\" in\n\"image inspect\")\n\tcase \"$3\" in\n")
	for _, ref := range slices.Sorted(maps.Keys(images)) {
		img := images[ref]
		body, err := json.Marshal([]map[string]any{{
			"Id": img.ID, "RepoDigests": img.RepoDigests, "Config": map[string]any{"Labels": img.Labels},
		}})
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "\t%s) printf '%%s\\n' %s; exit 0 ;;\n", quote(ref), quote(string(body)))
	}
	b.WriteString("\tesac\n\techo \"Error: No such image: $3\" >&2\n\texit 1 ;;\n\"image ls\")\n\tcase \"$5\" in\n")
	for _, repo := range slices.Sorted(maps.Keys(tags)) {
		fmt.Fprintf(&b, "\t%s) printf '%%s\\n' %s ;;\n", quote(repo), quote(strings.Join(tags[repo], "\n")))
	}
	fmt.Fprintf(&b, "\tesac\n\texit 0 ;;\nesac\necho \"$*\" >> %s\n", quote(filepath.Join(dir, "calls")))
	bin := filepath.Join(dir, name)
	if err := os.WriteFile(bin, []byte(b.String()), 0o700); err != nil { // #nosec G306 -- an executable test script
		return "", fmt.Errorf("write fake %s: %w", name, err)
	}
	return bin, nil
}

// Calls returns the recorded invocations of the fake CLI in dir, one per line.
func Calls(dir string) []string {
	b, _ := os.ReadFile(filepath.Join(dir, "calls")) // #nosec G304 -- the test's own directory
	if len(b) == 0 {
		return nil
	}
	return strings.Split(strings.TrimRight(string(b), "\n"), "\n")
}

// quote quotes s for the shell, in single quotes.
func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
