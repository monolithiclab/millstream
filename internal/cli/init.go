package cli

import (
	"errors"
	"fmt"

	"github.com/monolithiclab/millstream/internal/scaffold"
)

// InitCmd is "millstream init".
type InitCmd struct {
	Pipeline InitPipelineCmd `cmd:"" help:"Scaffold a pipeline package: a manual trigger, one Go action step, a fixture, an eval and a package test."`
	Action   InitActionCmd   `cmd:"" help:"Scaffold an action: Go with the SDK, or raw protocol in Python 3 (a POSIX sh JSON implementation is not realistic)."`
}

// InitPipelineCmd is "millstream init pipeline".
type InitPipelineCmd struct {
	Name string `arg:"" help:"The pipeline's name: lowercase letters, digits and single dashes."`
	Dir  string `default:"." type:"path" help:"The directory to create the package in."`
}

// Run writes the package and says what to do next.
func (c InitPipelineCmd) Run(rt *Runtime) error {
	dir, err := scaffold.Pipeline(c.Dir, c.Name)
	if err != nil {
		return initErr(err)
	}
	fmt.Fprintf(rt.IO.Out, "created %s\nnext: millstream check %s && millstream test pipeline %s\n", dir, dir, dir)
	return nil
}

// InitActionCmd is "millstream init action".
type InitActionCmd struct {
	Name string `arg:"" help:"The action's name: lowercase letters, digits and single dashes."`
	Dir  string `default:"." type:"path" help:"The directory to create the action in."`
	Lang string `enum:"go,raw" default:"go" help:"go: the Go SDK; raw: the protocol spoken directly, in Python 3."`
}

// Run writes the action and says what to do next.
func (c InitActionCmd) Run(rt *Runtime) error {
	dir, err := scaffold.Action(c.Dir, c.Name, c.Lang)
	if err != nil {
		return initErr(err)
	}
	fmt.Fprintf(rt.IO.Out, "created %s\n", dir)
	if c.Lang == scaffold.LangGo {
		fmt.Fprintf(rt.IO.Out, "next: point go.mod at the SDK, build as main.go says, then millstream test action %s\n",
			dir)
		return nil
	}
	fmt.Fprintf(rt.IO.Out, "next: millstream test action %s\n", dir)
	return nil
}

// initErr makes a refused name or directory a usage error.
func initErr(err error) error {
	if errors.Is(err, scaffold.ErrName) || errors.Is(err, scaffold.ErrExists) {
		return usageError(err.Error())
	}
	return err
}
