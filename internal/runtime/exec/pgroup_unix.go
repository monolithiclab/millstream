//go:build unix

package exec

import (
	"os"
	osexec "os/exec"
	"syscall"
)

// setProcessGroup starts the command as the leader of its own process group, so killProcessGroup reaches the
// children it spawns as well.
func setProcessGroup(cmd *osexec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killProcessGroup sends SIGKILL to the process's whole group, falling back to the process alone.
func killProcessGroup(p *os.Process) {
	if err := syscall.Kill(-p.Pid, syscall.SIGKILL); err != nil {
		_ = p.Kill()
	}
}
