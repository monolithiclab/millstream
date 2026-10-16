// Package cli is the millstream command line: a Kong parser over a command tree, run through an injectable seam
// so tests drive every command through the parser without touching the process.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/alecthomas/kong"

	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
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
	// Env looks up an environment variable: variables and secrets resolve through it.
	Env func(string) (string, bool)
	// DB is the state database path, from the global --db flag; DBSet says the flag was given, for the commands
	// that write to the database only when asked to (eval).
	DB    string
	DBSet bool
	// EnvFile is the environment file named by the global --env-file flag; empty reads ./millstream.yaml when it
	// exists.
	EnvFile string
	// Launchers, when set, replace the launchers the environment selects: tests run actions in-process with them.
	Launchers map[string]exec.Launcher
	// HTTPClient, when set, posts review notifications instead of a default client: tests point it at httptest.
	HTTPClient *http.Client
	// Store, when set, is the state database the commands use instead of opening --db, and leave open: tests run a
	// series of commands against one store with it.
	Store *sqlite.Store
}

// dirOrCurrent returns dir, or the current directory's absolute path when dir is empty. The directory arguments
// default to it here rather than through a default:"." tag, which Kong expands (a getwd and its stats) for every
// such argument of the tree on every run, whichever command runs.
func dirOrCurrent(dir string) string {
	if dir != "" {
		return dir
	}
	if abs, err := filepath.Abs("."); err == nil {
		return abs
	}
	return "."
}

// usageError marks an error that exits with the usage status.
type usageError string

func (e usageError) Error() string { return string(e) }

// errFailed exits with the failure status without a message: the command already reported why on its streams.
var errFailed = errors.New("failed")

// exitSignal carries a status out of Kong's Exit hook (--help) without exiting mid-defer.
type exitSignal int

// CLI is the command tree.
type CLI struct {
	DB      string `name:"db" default:".millstream/millstream.db" help:"State database path (created with its directory)."`
	EnvFile string `name:"env-file" type:"path" help:"Environment file (default: ./millstream.yaml when it exists)."`

	Run      RunCmd      `cmd:"" help:"Apply packages and run the engine with an embedded node; run report prints a run's report."`
	Check    CheckCmd    `cmd:"" help:"Validate a pipeline package."`
	Lock     LockCmd     `cmd:"" help:"Pin a package's container and registry actions by image digest in millstream.lock."`
	Graph    GraphCmd    `cmd:"" help:"Print a package's graph as Mermaid or DOT."`
	Vars     VarsCmd     `cmd:"" help:"Print a package's variables, values and sources."`
	Status   StatusCmd   `cmd:"" help:"Show pipelines and recent runs."`
	Tail     TailCmd     `cmd:"" help:"Print events as JSON lines."`
	Trigger  TriggerCmd  `cmd:"" help:"Fire a trigger."`
	Cancel   CancelCmd   `cmd:"" help:"Cancel a run."`
	Replay   ReplayCmd   `cmd:"" help:"Replay dead letters into their run, or a step over past items in a replay run."`
	DLQ      DLQCmd      `cmd:"" name:"dlq" help:"List and discard dead letters."`
	Inbox    InboxCmd    `cmd:"" help:"List and decide reviews and review samples."`
	Pause    PauseCmd    `cmd:"" help:"Pause a pipeline or one of its steps."`
	Resume   ResumeCmd   `cmd:"" help:"Resume a pipeline or one of its steps."`
	Init     InitCmd     `cmd:"" help:"Scaffold a pipeline package or an action."`
	Test     TestCmd     `cmd:"" help:"Test a package against stubbed actions, or an action against golden outputs."`
	Eval     EvalCmd     `cmd:"" help:"Run an action's evals and score them."`
	Metrics  MetricsCmd  `cmd:"" help:"Print metric rollups, or their Prometheus exposition."`
	Insights InsightsCmd `cmd:"" help:"List, acknowledge and mute insights."`
	Version  VersionCmd  `cmd:"" help:"Print the version."`
	Agent    AgentCmd    `cmd:"" hidden:"" help:"Run the agent adapter: the action process of agent steps."`
}

// Main runs the CLI and returns the process exit code.
func Main(ctx context.Context, argv []string, stdio IO, version string) int {
	return Run(ctx, argv, stdio, version, nil)
}

// Run parses argv and runs the selected command, returning its exit code. configure, when not nil, adjusts the
// Runtime before the command runs: the seam tests use to inject fakes.
func Run(ctx context.Context, argv []string, stdio IO, version string, configure func(*Runtime)) (code int) {
	rt := &Runtime{Ctx: ctx, IO: stdio, Version: version, Env: os.LookupEnv}
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
	rt.DB, rt.EnvFile = tree.DB, tree.EnvFile
	if kctx != nil {
		for _, p := range kctx.Path {
			rt.DBSet = rt.DBSet || p.Flag != nil && p.Flag.Name == "db"
		}
	}
	if err != nil {
		fmt.Fprintf(rt.IO.Err, "millstream: %v\n", err)
		if trace, terr := kong.Trace(parser, argv); terr == nil {
			parser.Stdout = rt.IO.Err // usage after an error belongs on stderr
			_ = trace.PrintUsage(true)
		}
		return exitUsage
	}
	if err := kctx.Run(); err != nil {
		if errors.Is(err, errFailed) {
			return exitFail
		}
		fmt.Fprintf(rt.IO.Err, "millstream: %v\n", err)
		if errors.As(err, new(usageError)) {
			return exitUsage
		}
		return exitFail
	}
	return exitOK
}
