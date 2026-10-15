//go:build !e2e

package container

import (
	"errors"
	"fmt"
	osexec "os/exec"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container/containertest"
)

// The runtimes of the default build run their CLI in-process (Runtime.cli), so the tests start no process; the e2e
// build's run a script as the CLI.

// fakeRuntime returns a runtime whose CLI is the fake CLI of containertest, and the invocations it records.
func fakeRuntime(_ *testing.T, images map[string]containertest.Image, tags map[string][]string) (Runtime,
	func() []string,
) {
	f := &containertest.Fake{Images: images, Tags: tags}
	return Runtime{Name: pipeline.RuntimeDocker, Bin: "/fake/docker", Env: []string{"PATH=/usr/bin:/bin"}, cli: f}, f.Calls
}

// rawRuntime returns a runtime named name whose CLI prints out, whatever it is asked.
func rawRuntime(_ *testing.T, name, out string) Runtime {
	return Runtime{Name: name, Bin: "/fake/" + name, cli: rawCLI(out)}
}

// missingRuntime returns a runtime whose CLI cannot be started.
func missingRuntime(_ *testing.T) Runtime {
	return Runtime{Name: pipeline.RuntimeDocker, Bin: "/absent/docker", cli: missingCLI{}}
}

// rawCLI prints itself.
type rawCLI string

func (o rawCLI) Run(cmd *osexec.Cmd) error {
	_, err := fmt.Fprintln(cmd.Stdout, string(o))
	return err
}

// missingCLI fails to start, as a CLI that is not there.
type missingCLI struct{}

func (missingCLI) Run(cmd *osexec.Cmd) error {
	return &osexec.Error{Name: cmd.Path, Err: errors.New("no such file or directory")}
}
