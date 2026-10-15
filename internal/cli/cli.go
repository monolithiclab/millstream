// Package cli is the millstream command line: a Kong parser over a command tree, run through an injectable seam
// so tests drive every command through the parser without touching the process.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/alecthomas/kong"
)

// Exit codes: a closed set, so deferred work always runs before the process exits.
const (
	exitOK    = 0
	exitFail  = 1
	exitUsage = 2
)

// IO is the process's standard streams.
type IO struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

// StdIO returns the real standard streams.
func StdIO() IO { return IO{In: os.Stdin, Out: os.Stdout, Err: os.Stderr} }

// Runtime is what commands run against. Tests adjust it through the configure argument of Run.
type Runtime struct {
	Ctx     context.Context
	IO      IO
	Version string
}

// errNotImplemented is returned by the commands whose task has not landed yet.
var errNotImplemented = usageError("not implemented yet")

// usageError marks an error that exits with the usage status.
type usageError string

func (e usageError) Error() string { return string(e) }

// exitSignal carries a status out of Kong's Exit hook (--help) without exiting mid-defer.
type exitSignal int

// CLI is the command tree.
type CLI struct {
	Run     RunCmd     `cmd:"" help:"Apply packages and run the engine."`
	Check   CheckCmd   `cmd:"" help:"Validate a pipeline package."`
	Graph   GraphCmd   `cmd:"" help:"Print a package's graph as Mermaid or DOT."`
	Vars    VarsCmd    `cmd:"" help:"Print a package's variables, values and sources."`
	Status  StatusCmd  `cmd:"" help:"Show runs and steps."`
	Tail    TailCmd    `cmd:"" help:"Stream events."`
	Trigger TriggerCmd `cmd:"" help:"Fire a trigger."`
	Cancel  CancelCmd  `cmd:"" help:"Cancel a run."`
	Replay  ReplayCmd  `cmd:"" help:"Replay dead letters."`
	DLQ     DLQCmd     `cmd:"" name:"dlq" help:"Inspect and discard dead letters."`
	Pause   PauseCmd   `cmd:"" help:"Pause a pipeline or step."`
	Resume  ResumeCmd  `cmd:"" help:"Resume a pipeline or step."`
	Test    TestCmd    `cmd:"" help:"Test a package against stubbed actions."`
	Version VersionCmd `cmd:"" help:"Print the version."`
}

// DBFlag selects the state database.
type DBFlag struct {
	DB string `name:"db" default:".millstream/millstream.db" help:"State database path."`
}

// RunCmd is "millstream run".
type RunCmd struct {
	DBFlag
	Dirs []string          `arg:"" optional:"" type:"path" help:"Package directories."`
	Var  map[string]string `help:"Set a variable (name=value)."`
	Once bool              `help:"Fire each manual trigger once and exit when every run settles."`
}

// Run is a stub until the engine lands.
func (RunCmd) Run() error { return errNotImplemented }

// CheckCmd is "millstream check".
type CheckCmd struct {
	Dir string `arg:"" optional:"" default:"." type:"path" help:"Package directory."`
}

// Run is a stub until the checks land.
func (CheckCmd) Run() error { return errNotImplemented }

// GraphCmd is "millstream graph".
type GraphCmd struct {
	Dir    string `arg:"" optional:"" default:"." type:"path" help:"Package directory."`
	Format string `enum:"mermaid,dot" default:"mermaid" help:"Output format."`
}

// Run is a stub until the graph renderer lands.
func (GraphCmd) Run() error { return errNotImplemented }

// VarsCmd is "millstream vars".
type VarsCmd struct {
	Dir string            `arg:"" optional:"" default:"." type:"path" help:"Package directory."`
	Var map[string]string `help:"Set a variable (name=value)."`
}

// Run is a stub until variable resolution lands.
func (VarsCmd) Run() error { return errNotImplemented }

// StatusCmd is "millstream status".
type StatusCmd struct{ DBFlag }

// Run is a stub until the store lands.
func (StatusCmd) Run() error { return errNotImplemented }

// TailCmd is "millstream tail".
type TailCmd struct{ DBFlag }

// Run is a stub until the store lands.
func (TailCmd) Run() error { return errNotImplemented }

// TriggerCmd is "millstream trigger".
type TriggerCmd struct {
	DBFlag
	Pipeline string `arg:"" help:"Pipeline name."`
	Trigger  string `arg:"" help:"Trigger name."`
}

// Run is a stub until the store lands.
func (TriggerCmd) Run() error { return errNotImplemented }

// CancelCmd is "millstream cancel".
type CancelCmd struct {
	DBFlag
	RunID string `arg:"" name:"run" help:"Run id."`
}

// Run is a stub until the store lands.
func (CancelCmd) Run() error { return errNotImplemented }

// ReplayCmd is "millstream replay".
type ReplayCmd struct {
	DBFlag
	From    string `enum:"dlq" required:"" help:"What to replay from."`
	RunID   string `name:"run" required:"" help:"Run id."`
	Migrate bool   `help:"Replay against the current revision."`
}

// Run is a stub until the store lands.
func (ReplayCmd) Run() error { return errNotImplemented }

// DLQCmd is "millstream dlq".
type DLQCmd struct {
	List    DLQListCmd    `cmd:"" help:"List dead letters."`
	Discard DLQDiscardCmd `cmd:"" help:"Discard a dead letter."`
}

// DLQListCmd is "millstream dlq list".
type DLQListCmd struct{ DBFlag }

// Run is a stub until the store lands.
func (DLQListCmd) Run() error { return errNotImplemented }

// DLQDiscardCmd is "millstream dlq discard".
type DLQDiscardCmd struct {
	DBFlag
	ID string `arg:"" help:"Dead letter id."`
}

// Run is a stub until the store lands.
func (DLQDiscardCmd) Run() error { return errNotImplemented }

// PauseCmd is "millstream pause".
type PauseCmd struct {
	DBFlag
	Pipeline string `arg:"" help:"Pipeline name."`
	Step     string `arg:"" optional:"" help:"Step name."`
}

// Run is a stub until the store lands.
func (PauseCmd) Run() error { return errNotImplemented }

// ResumeCmd is "millstream resume".
type ResumeCmd struct {
	DBFlag
	Pipeline string `arg:"" help:"Pipeline name."`
	Step     string `arg:"" optional:"" help:"Step name."`
}

// Run is a stub until the store lands.
func (ResumeCmd) Run() error { return errNotImplemented }

// TestCmd is "millstream test".
type TestCmd struct {
	Pipeline TestPipelineCmd `cmd:"" help:"Run a package against stubbed actions."`
}

// TestPipelineCmd is "millstream test pipeline".
type TestPipelineCmd struct {
	Dir string `arg:"" optional:"" default:"." type:"path" help:"Package directory."`
}

// Run is a stub until the test runner lands.
func (TestPipelineCmd) Run() error { return errNotImplemented }

// Main runs the CLI and returns the process exit code.
func Main(ctx context.Context, argv []string, stdio IO, version string) int {
	return Run(ctx, argv, stdio, version, nil)
}

// Run parses argv and runs the selected command, returning its exit code. configure, when not nil, adjusts the
// Runtime before the command runs: the seam tests use to inject fakes.
func Run(ctx context.Context, argv []string, stdio IO, version string, configure func(*Runtime)) (code int) {
	rt := &Runtime{Ctx: ctx, IO: stdio, Version: version}
	if configure != nil {
		configure(rt)
	}
	defer func() {
		if r := recover(); r != nil {
			sig, ok := r.(exitSignal)
			if !ok {
				panic(r)
			}
			code = int(sig)
		}
	}()

	var tree CLI
	parser, err := kong.New(&tree,
		kong.Name("millstream"),
		kong.Description("Pipelines of actions connected by persisted streams."),
		kong.Writers(rt.IO.Out, rt.IO.Err),
		kong.Exit(func(c int) { panic(exitSignal(c)) }),
		kong.BindTo(rt.Ctx, (*context.Context)(nil)),
		kong.Bind(rt),
	)
	if err != nil {
		fmt.Fprintf(rt.IO.Err, "millstream: %v\n", err)
		return exitFail
	}
	kctx, err := parser.Parse(argv)
	if err != nil {
		fmt.Fprintf(rt.IO.Err, "millstream: %v\n", err)
		if trace, terr := kong.Trace(parser, argv); terr == nil {
			parser.Stdout = rt.IO.Err // usage after an error belongs on stderr
			_ = trace.PrintUsage(true)
		}
		return exitUsage
	}
	if err := kctx.Run(); err != nil {
		fmt.Fprintf(rt.IO.Err, "millstream: %v\n", err)
		if errors.As(err, new(usageError)) {
			return exitUsage
		}
		return exitFail
	}
	return exitOK
}
