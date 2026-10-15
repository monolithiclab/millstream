//go:build e2e

package exec

import (
	"os"

	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
)

// The e2e build runs the fake action as the built binary, a real process: the durations leave it time to start and
// exit under load.
const (
	// answerTimeout is the timeout of a test that wants it to expire.
	answerTimeout   = 300 * millisecond
	shutdownTimeout = 300 * millisecond
	slowMs          = 200
	// pipeFill is a payload size larger than an OS pipe buffer, so writing it blocks on a process that does not read.
	pipeFill = 4 << 20
)

var fakeDir string

func fakeLaunchers() map[string]Launcher { return nil }

func fakeCommand() (string, error) {
	var err error
	if fakeDir, err = os.MkdirTemp("", "exec-test-"); err != nil {
		return "", err
	}
	return exectest.Build(fakeDir)
}

func cleanupFake() { _ = os.RemoveAll(fakeDir) }
