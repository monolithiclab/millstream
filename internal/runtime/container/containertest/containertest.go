// Package containertest writes a fake container runtime CLI: a shell script that answers image inspect and image
// ls from canned data and records every other invocation, so tests of image resolution and the lock file run
// without Docker. The script uses shell builtins only, so it works whatever PATH the test gives the runtime. Fake
// is the same CLI in-process, for the tests that start no process.
package containertest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	osexec "os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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
		body, err := images[ref].inspect()
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

// inspect is image inspect's output for the image.
func (img Image) inspect() ([]byte, error) {
	return json.Marshal([]map[string]any{{
		"Id": img.ID, "RepoDigests": img.RepoDigests, "Config": map[string]any{"Labels": img.Labels},
	}})
}

// Fake is the fake CLI in-process: Run answers an invocation as the script Write writes would, on the command's
// output writers, without starting it.
type Fake struct {
	// Images are the canned images by reference (repository:tag), Tags the tags of each repository.
	Images map[string]Image
	Tags   map[string][]string

	mu    sync.Mutex
	calls []string
}

// Run answers the invocation cmd describes (its arguments after the CLI's name): image inspect and image ls from the
// canned data, an absent image as an error with the CLI's message on standard error; every other invocation is
// recorded.
func (f *Fake) Run(cmd *osexec.Cmd) error {
	stdout, stderr := orDiscard(cmd.Stdout), orDiscard(cmd.Stderr)
	arg := func(i int) string { // $i of the script: empty when missing
		if i < len(cmd.Args) {
			return cmd.Args[i]
		}
		return ""
	}
	switch arg(1) + " " + arg(2) {
	case "image inspect":
		img, ok := f.Images[arg(3)]
		if !ok {
			_, _ = fmt.Fprintf(stderr, "Error: No such image: %s\n", arg(3))
			return errors.New("exit status 1")
		}
		body, err := img.inspect()
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s\n", body)
		return err
	case "image ls":
		if tags, ok := f.Tags[arg(5)]; ok {
			_, err := fmt.Fprintf(stdout, "%s\n", strings.Join(tags, "\n"))
			return err
		}
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, strings.Join(cmd.Args[1:], " "))
	return nil
}

// Calls returns the recorded invocations of the fake.
func (f *Fake) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func orDiscard(w io.Writer) io.Writer {
	if w == nil {
		return io.Discard
	}
	return w
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
