// Package fakeexec gives the tests of the packages above the exec runtime (node, engine, eval, cli) the fake action
// of exectest: in-process in the default build, so those tests start no process, and as the built binary in the
// e2e build (go test -tags e2e), so the same tests also cover real processes.
package fakeexec

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// NewPool returns a process pool running the fake action with opts, closed when the test ends.
func NewPool(t testing.TB, opts exec.Options) *exec.Pool {
	t.Helper()
	opts.Launchers = Launchers()
	pool := exec.NewPool(exec.PoolOptions{Options: opts})
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}
