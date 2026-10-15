//go:build !e2e

package exec

import "github.com/monolithiclab/millstream/internal/runtime/exec/exectest"

// The default build runs the fake action in-process (exectest.Serve); the e2e build runs the built binary. The
// durations below only need to outlast an in-process answer.
const (
	// answerTimeout bounds deliveries that hang while the others answer.
	answerTimeout   = 20 * millisecond
	shutdownTimeout = 20 * millisecond
	slowMs          = 30
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
