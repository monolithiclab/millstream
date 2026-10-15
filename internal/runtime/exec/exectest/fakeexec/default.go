//go:build !e2e

package fakeexec

import (
	"os"
	"path/filepath"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
)

// Launchers returns the launchers that run the fake action: exectest.Serve in-process for exec actions.
func Launchers() map[string]exec.Launcher {
	return map[string]exec.Launcher{pipeline.KindExec: exec.FuncLauncher{F: exectest.Serve}}
}

// Place puts the fake action at dst: an inert stub, since the in-process launcher ignores the command.
func Place(dst string) error { return writeStub(dst) }

// Command returns the fake action's command: a name, since the in-process launcher ignores it.
func Command() (string, error) { return "fakeaction", nil }

// Cleanup removes what the package built; TestMain calls it after m.Run. Nothing in the default build.
func Cleanup() {}

// stub is what Place writes in the default build: an executable the in-process launcher never runs.
const stub = "#!/bin/sh\necho 'fakeexec: the default build runs the fake action in-process' >&2\nexit 1\n"

func writeStub(dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return err
	}
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(dst, []byte(stub), 0o700) // #nosec G306 -- an executable test stub
}
