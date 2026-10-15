//go:build !unix

package exec

import (
	"os"
	osexec "os/exec"
)

// SetProcessGroup is a no-op where process groups are not available.
func SetProcessGroup(*osexec.Cmd) {}

// KillProcessGroup kills the process alone where process groups are not available.
func KillProcessGroup(p *os.Process) { _ = p.Kill() }
