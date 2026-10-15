package failpoint

import (
	"errors"
	"os"
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
