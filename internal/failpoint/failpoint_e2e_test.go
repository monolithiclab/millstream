//go:build e2e

package failpoint

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

// TestEnvExitsProcess runs the test binary as a child with the variable set and checks the real exit status.
func TestEnvExitsProcess(t *testing.T) {
	if os.Getenv(EnvVar) != "" {
		Hit("child.point")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestEnvExitsProcess$") // #nosec G204 -- the test binary itself
	cmd.Env = append(os.Environ(), EnvVar+"=child.point")
	err := cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != ExitCode {
		t.Fatalf("child: %v, want exit status %d", err, ExitCode)
	}
}
