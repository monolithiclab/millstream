package cli

import (
	"errors"
	"fmt"

	"github.com/monolithiclab/millstream/internal/improve"
	"github.com/monolithiclab/millstream/internal/scaffold"
)

// InitCmd is "millstream init".
type InitCmd struct {
	Pipeline InitPipelineCmd `cmd:"" help:"Scaffold a pipeline package: a manual trigger, one Go action step, a fixture, an eval and a package test."`
	Action   InitActionCmd   `cmd:"" help:"Scaffold an action: Go with the SDK, or raw protocol in Python 3 (a POSIX sh JSON implementation is not realistic)."`
	Env      InitEnvCmd      `cmd:"" name:"environment" help:"Write millstream.yaml, the environment file, with every key commented out at its default."`
}

// InitPipelineCmd is "millstream init pipeline".
type InitPipelineCmd struct {
	Name string `arg:"" help:"The pipeline's name: lowercase letters, digits and single dashes."`
	Dir  string `type:"path" help:"The directory to create the package in (default: the current one)."`
	From string `enum:",improve,listings" default:"" help:"Copy a package embedded in the binary: listings, the example that runs offline; improve, to fork it."`
}

// Run writes the package and says what to do next.
func (c InitPipelineCmd) Run(rt *Runtime) error {
	if c.From == improve.Name {
		files, err := improve.Fork(c.Name)
		if err != nil {
			return err
		}
		dir, err := scaffold.Files(dirOrCurrent(c.Dir), c.Name, files)
		if err != nil {
			return initErr(err)
		}
		fmt.Fprintf(rt.IO.Out, "created %s, a copy of the improve package\nnext: map its %s and %s secrets "+
			"(the API's URL and a token from millstream proposals token), then millstream check %s\n", dir,
			improve.SecretAPI, improve.SecretToken, dir)
		return nil
	}
	if c.From == scaffold.ListingsName {
		dir, err := scaffold.Listings(dirOrCurrent(c.Dir), c.Name)
		if err != nil {
			return initErr(err)
		}
		fmt.Fprintf(rt.IO.Out, "created %s, the listings example\nnext: cd %s && "+
			"MILLSTREAM_HARNESS_MODE=replay MILLSTREAM_HARNESS_DIR=recordings millstream run --once\n", dir, dir)
		return nil
	}
	dir, err := scaffold.Pipeline(dirOrCurrent(c.Dir), c.Name)
	if err != nil {
		return initErr(err)
	}
	fmt.Fprintf(rt.IO.Out, "created %s\nnext: millstream check %s && millstream test pipeline %s\n", dir, dir, dir)
	return nil
}

// InitActionCmd is "millstream init action".
type InitActionCmd struct {
	Name string `arg:"" help:"The action's name: lowercase letters, digits and single dashes."`
	Dir  string `type:"path" help:"The directory to create the action in (default: the current one)."`
	Lang string `enum:"go,raw" default:"go" help:"go: the Go SDK; raw: the protocol spoken directly, in Python 3."`
}

// Run writes the action and says what to do next.
func (c InitActionCmd) Run(rt *Runtime) error {
	dir, err := scaffold.Action(dirOrCurrent(c.Dir), c.Name, c.Lang)
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

// InitEnvCmd is "millstream init environment".
type InitEnvCmd struct {
	Dir string `type:"path" help:"The directory to write millstream.yaml in (default: the current one)."`
}

// Help is the command's detail, printed by its --help.
func (InitEnvCmd) Help() string {
	return `Every key is commented out at its default, or at an example when it has none, and each line names the
MILLSTREAM_* variable that sets it. Uncomment and edit the keys to change.

Examples:
  millstream init environment
  millstream init environment --dir /etc/millstream`
}

// Run writes the file and says what to do next.
func (c InitEnvCmd) Run(rt *Runtime) error {
	path, err := scaffold.Environment(dirOrCurrent(c.Dir))
	if err != nil {
		return initErr(err)
	}
	fmt.Fprintf(rt.IO.Out, "created %s\nnext: uncomment the keys to change, then millstream serve --env-file %s\n", path, path)
	return nil
}

// initErr makes a refused name or directory a usage error.
func initErr(err error) error {
	if errors.Is(err, scaffold.ErrName) || errors.Is(err, scaffold.ErrExists) {
		return usageError(err.Error())
	}
	return err
}
