//go:build unix

package exec

import (
	"os"
	osexec "os/exec"
	"syscall"
)

// SetProcessGroup starts the command as the leader of its own process group, so KillProcessGroup reaches the
// children it spawns as well.
func SetProcessGroup(cmd *osexec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// KillProcessGroup sends SIGKILL to the process's whole group, falling back to the process alone.
func KillProcessGroup(p *os.Process) {
	if err := syscall.Kill(-p.Pid, syscall.SIGKILL); err != nil {
		_ = p.Kill()
	}
}
