//go:build e2e

package fakeexec

import (
	"os"
	"sync"

	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
)

var (
	dir   string
	build = sync.OnceValues(func() (string, error) {
		var err error
		if dir, err = os.MkdirTemp("", "fakeexec-"); err != nil {
			return "", err
		}
		return exectest.Build(dir)
	})
)

// Launchers returns nil: the exec runtime's own launchers, which start the built binary as a process.
func Launchers() map[string]exec.Launcher { return nil }

// Place puts the fake action binary, built once, at dst.
func Place(dst string) error {
	bin, err := build()
	if err != nil {
		return err
	}
	return exectest.Place(bin, dst)
}

// Command returns the path of the fake action binary, built once.
func Command() (string, error) { return build() }

// Cleanup removes the built binary; TestMain calls it after m.Run.
func Cleanup() {
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
}
