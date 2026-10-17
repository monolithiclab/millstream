// Package testdir gives tests temporary directories under one root per test binary, removed once by Remove from
// TestMain: t.TempDir removes each test's directory as the test ends, which on some file systems costs more than the
// tests that use one.
package testdir

import (
	"os"
	"sync"
	"testing"
)

var root struct {
	once sync.Once
	dir  string
	err  error
}

// New returns a new, empty directory under the binary's root.
func New(t testing.TB) string {
	t.Helper()
	root.once.Do(func() { root.dir, root.err = os.MkdirTemp("", "millstream-test-") })
	if root.err != nil {
		t.Fatal(root.err)
	}
	dir, err := os.MkdirTemp(root.dir, "")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// Remove removes the root and every directory New made; TestMain calls it once the tests ended.
func Remove() {
	if root.dir != "" {
		_ = os.RemoveAll(root.dir)
	}
}
