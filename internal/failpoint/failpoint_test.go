package failpoint

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

// The tests below change process-wide state, so they do not run in parallel.

func TestHitUnarmedDoesNothing(t *testing.T) {
	Hit("done.beforeCommit")
}

func TestSetPanicsWithCrash(t *testing.T) {
	reset := Set("done.beforeCommit")
	defer reset()
	Hit("done.afterCommit") // another name: no effect
	defer func() {
		r := recover()
		c, ok := r.(Crash)
		if !ok || c.Name != "done.beforeCommit" {
			t.Fatalf("recovered %v, want Crash{done.beforeCommit}", r)
		}
		var err error = c
		if !errors.As(err, new(Crash)) || err.Error() != "failpoint done.beforeCommit" {
			t.Errorf("error = %q", err)
		}
	}()
	Hit("done.beforeCommit")
	t.Fatal("Hit did not panic")
}

func TestResetDisarms(t *testing.T) {
	Set("fire.afterCommit")()
	Hit("fire.afterCommit")
}

func TestEnvExits(t *testing.T) {
	var code int
	exit = func(c int) { code = c }
	fromEnv = "lease.beforeCommit"
	defer func() { exit, fromEnv = os.Exit, "" }()
	Hit("lease.afterCommit")
	if code != 0 {
		t.Fatalf("exited %d for another name", code)
	}
	Hit("lease.beforeCommit")
	if code != ExitCode {
		t.Fatalf("exit code %d, want %d", code, ExitCode)
	}
}

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
