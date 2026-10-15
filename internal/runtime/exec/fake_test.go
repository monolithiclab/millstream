//go:build !e2e

package exec

import (
	"time"

	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
)

// The default build runs the fake action in-process (exectest.Serve); the e2e build runs the built binary.
const (
	// answerTimeout is the timeout of a test that wants it to expire: at once in-process, where nothing needs time to
	// start or answer.
	answerTimeout = time.Nanosecond
	// shutdownTimeout only needs to outlast an in-process exit: no test waits for it.
	shutdownTimeout = 20 * millisecond
	// pipeFill is a payload size that blocks a write to an action that does not read: any size on an in-memory pipe.
	pipeFill = 1 << 10
)

func fakeLaunchers() map[string]Launcher {
	return map[string]Launcher{"exec": FuncLauncher{F: exectest.Serve}}
}

func fakeCommand() (string, error) { return "fakeaction", nil }

func cleanupFake() {}

// Start makes fakeLauncher a Starter in the default build, so the container kind runs the fake in-process too.
func (f *fakeLauncher) Start(spec Spec, workdir string) (*Proc, error) {
	f.record("command " + spec.Image)
	return FuncLauncher{F: exectest.Serve}.Start(spec, workdir)
}
