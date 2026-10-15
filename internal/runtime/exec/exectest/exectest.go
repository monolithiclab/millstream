// Package exectest builds the fake action process (exec/testdata/fakeaction) that the tests of the exec runtime
// and of the node run as a real child process. Its modes are listed in that command's doc comment.
package exectest

import (
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
)

// Build compiles the fake action into dir and returns the binary's path. Tests call it once, from TestMain.
func Build(dir string) (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("locate exectest source")
	}
	src := filepath.Join(filepath.Dir(file), "..", "testdata", "fakeaction")
	bin := filepath.Join(dir, "fakeaction")
	cmd := osexec.Command("go", "build", "-o", bin, ".") // #nosec G204 -- fixed arguments; bin is the caller's temp dir
	cmd.Dir = src
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build fake action: %w", err)
	}
	return bin, nil
}
