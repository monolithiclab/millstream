//go:build !unix

package exec

import (
	"os"
	osexec "os/exec"
)

// setProcessGroup is a no-op where process groups are not available.
func setProcessGroup(*osexec.Cmd) {}

// killProcessGroup kills the process alone where process groups are not available.
func killProcessGroup(p *os.Process) { _ = p.Kill() }
