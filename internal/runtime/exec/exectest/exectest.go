// Package exectest builds the fake action process (exec/testdata/fakeaction) that the tests of the exec runtime
// and of the node run as a real child process. Its modes are listed in that command's doc comment.
package exectest

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
)

// Build compiles the fake action into dir and returns the binary's path. Tests call it once, from TestMain.
func Build(dir string) (string, error) { return build(filepath.Join(dir, "fakeaction"), nil) }

// BuildFor compiles the fake action for another platform, statically (CGO_ENABLED=0), into dir, so it can run in
// an image FROM scratch, and returns the binary's path.
func BuildFor(dir, goos, goarch string) (string, error) {
	return build(filepath.Join(dir, "fakeaction-"+goos+"-"+goarch),
		[]string{"CGO_ENABLED=0", "GOOS=" + goos, "GOARCH=" + goarch})
}

func build(bin string, env []string) (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("locate exectest source")
	}
	src := filepath.Join(filepath.Dir(file), "..", "testdata", "fakeaction")
	cmd := osexec.Command("go", "build", "-o", bin, ".") // #nosec G204 -- fixed arguments; bin is the caller's temp dir
	cmd.Dir = src
	cmd.Env = append(os.Environ(), env...)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build fake action: %w", err)
	}
	return bin, nil
}

// Place puts the binary bin at dst, creating dst's directory and replacing a file already there: a hard link when
// the file system allows it, a copy otherwise. Tests use it to ship the fake action inside a pipeline package.
func Place(bin, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return fmt.Errorf("place fake action: %w", err)
	}
	if err := os.Remove(dst); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("place fake action: %w", err)
	}
	if err := os.Link(bin, dst); err == nil {
		return nil
	}
	data, err := os.ReadFile(bin) // #nosec G304 -- the binary Build produced
	if err != nil {
		return fmt.Errorf("place fake action: %w", err)
	}
	if err := os.WriteFile(dst, data, 0o700); err != nil { // #nosec G306 G703 -- an executable test binary at the test's own path
		return fmt.Errorf("place fake action: %w", err)
	}
	return nil
}
