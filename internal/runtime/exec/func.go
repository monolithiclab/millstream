package exec

import (
	"context"
	"errors"
	"fmt"
	"io"
	osexec "os/exec"
	"slices"
)

// FuncLauncher runs every action it launches as the function F, in-process, whatever the spec's command: tests use
// it to run actions without starting a process. F gets what a process would (its command line, its whole
// environment with MILLSTREAM_WORKDIR, its pipes) and a context the runtime cancels when it kills the action, and
// returns the exit status. F must return once ctx ends or its pipes fail.
type FuncLauncher struct {
	F func(ctx context.Context, args, env []string, stdin io.Reader, stdout, stderr io.Writer) int
}

var (
	_ Launcher = FuncLauncher{}
	_ Starter  = FuncLauncher{}
)

// errKilled is the exit error of a function action the runtime killed.
var errKilled = errors.New("killed")

// Command implements Launcher: a FuncLauncher has none, the runtime uses Start.
func (FuncLauncher) Command(spec Spec, _ string) (*osexec.Cmd, error) {
	return nil, fmt.Errorf("action %s runs in-process: it has no command", spec.Action)
}

// Kill implements Launcher: nothing outlives the function.
func (FuncLauncher) Kill(Spec, string) {}

// Start implements Starter: F runs on its own goroutine, on in-memory pipes.
func (l FuncLauncher) Start(spec Spec, workdir string) (*Proc, error) {
	env := append(slices.Clone(spec.Env), "MILLSTREAM_WORKDIR="+workdir)
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var status int
	var killed bool
	go func() {
		defer close(done)
		status = l.F(ctx, spec.Command, env, inR, outW, errW)
		killed = ctx.Err() != nil
		// Like a process exiting: its ends of the pipes close.
		_ = inR.Close()
		_ = outW.Close()
		_ = errW.Close()
	}()
	wait := func() error {
		<-done
		switch {
		case killed:
			return errKilled
		case status != 0:
			return fmt.Errorf("exit status %d", status)
		}
		return nil
	}
	kill := func() {
		// Like SIGKILL: the action's ends of the pipes fail at once, wherever it is blocked.
		cancel()
		_ = inR.CloseWithError(errKilled)
		_ = outW.Close() // the runtime reads EOF, as from a dead process
		_ = errW.Close()
	}
	return &Proc{Stdin: inW, Stdout: outR, Stderr: errR, Wait: wait, Kill: kill}, nil
}
