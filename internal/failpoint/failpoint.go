// Package failpoint injects crashes at named points so the crash suite can kill the engine at every boundary of
// every atomic unit (spec section 16). A process started with MILLSTREAM_FAILPOINT=<name> exits with status 137 the
// first time it reaches Hit(name), as abruptly as kill -9: no deferred call runs and nothing is flushed. Tests in
// the same process use Set instead, which makes Hit panic with Crash so the caller can recover and check what was
// left behind.
package failpoint

import (
	"os"
	"sync"
)

// EnvVar names the environment variable holding the failpoint that ends the process.
const EnvVar = "MILLSTREAM_FAILPOINT"

// ExitCode is the status a failpoint exits with, the one a shell reports for SIGKILL.
const ExitCode = 137

// Crash is the value Hit panics with when the failpoint was armed by Set.
type Crash struct{ Name string }

// Error makes a recovered Crash usable as an error.
func (c Crash) Error() string { return "failpoint " + c.Name }

var (
	fromEnv = os.Getenv(EnvVar) // read once: the crash suite sets it per process

	mu    sync.Mutex
	armed string
)

// exit is os.Exit, replaced in tests.
var exit = os.Exit

// Hit marks a failpoint. It exits the process when MILLSTREAM_FAILPOINT names it, panics with Crash when Set armed
// it, and otherwise does nothing.
func Hit(name string) {
	if fromEnv != "" && fromEnv == name {
		exit(ExitCode)
	}
	mu.Lock()
	hit := armed != "" && armed == name
	mu.Unlock()
	if hit {
		panic(Crash{Name: name})
	}
}

// Set arms name in this process until the returned function disarms it. Only one failpoint is armed at a time. The
// state is process-wide, so a test using Set must not run in parallel with tests that reach the same failpoint.
func Set(name string) (reset func()) {
	mu.Lock()
	armed = name
	mu.Unlock()
	return func() {
		mu.Lock()
		armed = ""
		mu.Unlock()
	}
}
