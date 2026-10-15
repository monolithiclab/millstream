//go:build e2e

package containertest

import (
	"os/exec"
	"testing"
)

// TestFakeCLI: the script Write writes answers as a process.
func TestFakeCLI(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bin, err := Write(dir, "docker", testImages, testTags)
	if err != nil {
		t.Fatal(err)
	}
	checkCLI(t, func(args ...string) (string, error) {
		cmd := exec.Command(bin, args...) // #nosec G204 -- the script just written
		cmd.Env = []string{}
		out, err := cmd.Output()
		return string(out), err
	}, func() []string { return Calls(dir) })
}
