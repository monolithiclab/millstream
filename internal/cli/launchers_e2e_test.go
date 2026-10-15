//go:build e2e

package cli

import "github.com/monolithiclab/millstream/internal/runtime/exec"

// testLaunchers returns nil: the CLI's own launchers, which start the fake action binary fakeexec.Place put in the
// package and run agent steps as this test binary re-executed (TestMain serves its agent subcommand).
func testLaunchers() map[string]exec.Launcher { return nil }
