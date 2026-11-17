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
	"os/user"
	"path/filepath"
	"sync"

	"github.com/alecthomas/kong"

	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
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
	// Environ lists the variables of the process (NAME=value), for the environment's strictness: a MILLSTREAM_*
	// variable inside a section's prefix that names no setting is an error. nil checks none.
	Environ func() []string
	// DB is the global --db flag: the store's DSN, overriding store.dsn when not empty; DBSet says the flag was
	// given, for the commands that write to the database only when asked to (eval).
	DB    string
	DBSet bool
	// EnvFile is the environment file named by the global --env-file flag; empty reads ./millstream.yaml when it
	// exists.
	EnvFile string
	// Server is the served engine's API URL, from the global --server flag (else $MILLSTREAM_SERVER): commands that
	// change state go through it.
	Server string
	// Direct is the global --direct flag: commands that change state write to the store from this process.
	Direct bool
	// Launchers, when set, replace the launchers the environment selects: tests run actions in-process with them.
	Launchers map[string]exec.Launcher
	// HTTPClient, when set, posts review notifications instead of a default client: tests point it at httptest.
	HTTPClient *http.Client
	// Store, when set, is the state database the commands use instead of opening --db, and leave open: tests run a
	// series of commands against one store with it.
	Store store.Store
	// CurrentUser names the account running the command, for when $USER is unset; nil asks os/user.
	CurrentUser func() (string, error)
	// IsTerminal says whether help written to w reaches a terminal, to color it; nil checks for a character device.
	IsTerminal func(w io.Writer) bool
	// color is the global --color flag: auto, always or never.
	color string
}

// identity is who runs the command, the reviewer of its decisions (but of those a token server records under
// auth.user): $USER, else the account the system names; "" when neither does.
func identity(rt *Runtime) string {
	if u, ok := rt.Env("USER"); ok && u != "" {
		return u
	}
	lookup := rt.CurrentUser
	if lookup == nil {
		lookup = func() (string, error) {
			u, err := user.Current()
			if err != nil {
				return "", err
			}
			return u.Username, nil
		}
	}
	if u, err := lookup(); err == nil {
		return u
	}
	return ""
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
	DB      string `name:"db" placeholder:"DSN" help:"Store DSN, overriding store.dsn: sqlite://<path> (created with its directory) or postgres://host/db (default: sqlite://.millstream/millstream.db)."`
	EnvFile string `name:"env-file" type:"path" env:"MILLSTREAM_ENV_FILE" help:"Environment file (default: $MILLSTREAM_ENV_FILE, else ./millstream.yaml when it exists); millstream init environment writes one."`
	Server  string `name:"server" placeholder:"URL" help:"Change state through the API of the engine serving at this URL (default: $MILLSTREAM_SERVER; token: $MILLSTREAM_TOKEN)."`
	Direct  bool   `name:"direct" help:"Change state from this process, not through --server; on Postgres this takes the leadership lock, waiting while an engine runs."`
	Color   string `name:"color" enum:"auto,always,never" default:"auto" placeholder:"WHEN" help:"Color the help: auto (on a terminal, unless NO_COLOR or TERM=dumb), always or never (default: $MILLSTREAM_COLOR, else auto)."`

	Init          InitCmd          `cmd:"" group:"Develop" help:"Scaffold a pipeline package or an action."`
	Check         CheckCmd         `cmd:"" group:"Develop" help:"Validate pipeline packages, alone and against each other."`
	Vars          VarsCmd          `cmd:"" group:"Develop" help:"Print a package's variables, their values and sources."`
	Graph         GraphCmd         `cmd:"" group:"Develop" help:"Print a package's graph as Mermaid or DOT."`
	Test          TestCmd          `cmd:"" group:"Develop" help:"Test a package against stubbed actions, or an action against golden outputs."`
	Eval          EvalCmd          `cmd:"" group:"Develop" help:"Run an action's evals and score them."`
	Lock          LockCmd          `cmd:"" group:"Develop" help:"Pin a package's container and registry actions by image digest."`
	Run           RunCmd           `cmd:"" group:"Run" help:"Run packages locally with an embedded engine and node."`
	Trigger       TriggerCmd       `cmd:"" group:"Run" help:"Fire a trigger by hand, or replace a webhook trigger's token."`
	Cancel        CancelCmd        `cmd:"" group:"Run" help:"Cancel a run."`
	Replay        ReplayCmd        `cmd:"" group:"Run" help:"Replay a step over past items, or back-fill an imported stream."`
	DeadLetters   DeadLettersCmd   `cmd:"" name:"dead-letters" aliases:"dlq" group:"Run" help:"List, replay and discard dead letters."`
	Inbox         InboxCmd         `cmd:"" group:"Run" help:"List and decide reviews and review samples."`
	Status        StatusCmd        `cmd:"" group:"Observe" help:"Show pipelines and recent runs."`
	Tail          TailCmd          `cmd:"" group:"Observe" help:"Print events as JSON lines."`
	Metrics       MetricsCmd       `cmd:"" group:"Observe" help:"Print metric rollups, or their Prometheus exposition."`
	Insights      InsightsCmd      `cmd:"" group:"Observe" help:"List, acknowledge and mute insights."`
	Report        ReportCmd        `cmd:"" group:"Observe" help:"Print a run's report: critical path, slowest steps, failures, cost, comparison."`
	Serve         ServeCmd         `cmd:"" group:"Operate" help:"Run an engine from the environment file, or its standby."`
	Apply         ApplyCmd         `cmd:"" group:"Operate" help:"Check packages against the deployment and upload them as new revisions."`
	Rollback      RollbackCmd      `cmd:"" group:"Operate" help:"Reactivate an earlier revision of a pipeline."`
	Pause         PauseCmd         `cmd:"" group:"Operate" help:"Pause a pipeline or one of its steps, or a pipeline's triggers too."`
	Resume        ResumeCmd        `cmd:"" group:"Operate" help:"Resume a pipeline or one of its steps."`
	Purge         PurgeCmd         `cmd:"" group:"Operate" help:"Erase items by key or id, with their descendants and copies."`
	Node          NodeCmd          `cmd:"" group:"Operate" help:"Run a remote node that joins an engine."`
	Nodes         NodesCmd         `cmd:"" group:"Operate" help:"List the remote nodes, drain the one running here, or revoke one."`
	Evidence      EvidenceCmd      `cmd:"" group:"Improve" help:"Export an evidence bundle in production; import it in development."`
	Improve       ImproveCmd       `cmd:"" group:"Improve" help:"Run the improve pipeline on one insight or every open one; development only."`
	Proposals     ProposalsCmd     `cmd:"" group:"Improve" help:"List, create and decide proposals."`
	Backtest      BacktestCmd      `cmd:"" group:"Improve" help:"Validate a patch against past items, before and after."`
	Help          HelpCmd          `cmd:"" group:"Help" help:"Print the guide, a topic, or a command's help."`
	Version       VersionCmd       `cmd:"" group:"Help" help:"Print the version."`
	Agent         AgentCmd         `cmd:"" hidden:"" help:"Run the agent adapter: the action process of agent steps."`
	ImproveAction ImproveActionCmd `cmd:"" name:"improve-action" hidden:"" help:"Run a step of the improve package: its action process."`
}

// Main runs the CLI and returns the process exit code.
func Main(ctx context.Context, argv []string, stdio IO, version string) int {
	return Run(ctx, argv, stdio, version, nil)
}

// Run parses argv and runs the selected command, returning its exit code. configure, when not nil, adjusts the
// Runtime before the command runs: the seam tests use to inject fakes.
func Run(ctx context.Context, argv []string, stdio IO, version string, configure func(*Runtime)) (code int) {
	rt := &Runtime{Ctx: ctx, IO: stdio, Version: version, Env: os.LookupEnv, Environ: os.Environ}
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

	p, err := takeParser(rt)
	if err != nil {
		fmt.Fprintf(rt.IO.Err, "millstream: %v\n", err)
		return exitFail
	}
	defer p.release()
	parser := p.k
	if len(argv) == 0 {
		argv = []string{"--help"} // the bare command prints the top-level help
	}
	kctx, err := p.parse(argv)
	rt.DB, rt.EnvFile, rt.Server, rt.Direct = p.tree.DB, p.tree.EnvFile, p.tree.Server, p.tree.Direct
	if kctx != nil {
		for _, e := range kctx.Path {
			rt.DBSet = rt.DBSet || e.Flag != nil && e.Flag.Name == "db"
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

// parser is a Kong parser over a command tree of its own, and the Runtime of the run using it. Building one walks
// the tree by reflection and interpolates every tag, which costs more than running most commands (0.5 ms, 8,000
// allocations): Run takes one from parsers and gives it back, so a process running command after command (the tests)
// builds one per concurrent run instead of one per run.
type parser struct {
	k    *kong.Kong
	tree CLI
	rt   *Runtime
}

// parsers holds the idle parsers.
var parsers sync.Pool

// takeParser returns an idle parser, or a new one, bound to rt and ready to parse.
func takeParser(rt *Runtime) (*parser, error) {
	p, _ := parsers.Get().(*parser)
	if p == nil {
		p = &parser{}
		k, err := kong.New(&p.tree,
			kong.Name("millstream"),
			kong.Description("Pipelines of actions connected by persisted streams."),
			kong.Exit(func(c int) { panic(exitSignal(c)) }),
			kong.Help(func(opts kong.HelpOptions, ctx *kong.Context) error { return printHelp(p.rt, opts, ctx) }),
			kong.BindToProvider(func() (context.Context, error) { return p.rt.Ctx, nil }),
			kong.BindToProvider(func() (*Runtime, error) { return p.rt, nil }),
		)
		if err != nil {
			return nil, err
		}
		p.k = k
	}
	p.rt = rt
	p.k.Stdout, p.k.Stderr = rt.IO.Out, rt.IO.Err
	// Parse resets every value to its default, but not the marks a parse leaves on the model: which values were set
	// and which nodes were on the parsed path. New leaves them all unset.
	_ = kong.Visit(p.k.Model, func(n kong.Visitable, next kong.Next) error {
		switch n := n.(type) {
		case *kong.Application:
			n.Active = false
		case *kong.Node:
			n.Active = false
		case *kong.Value:
			n.Active, n.Set = false, false
		}
		return next(nil)
	})
	return p, nil
}

// release gives p back to parsers, without the run's Runtime and streams.
func (p *parser) release() {
	p.rt, p.k.Stdout, p.k.Stderr = nil, nil, nil
	parsers.Put(p)
}

// parse is kong's Parse without its hooks. Parse runs four, each walking the whole command tree and cloning its
// bindings at every node, for hook methods none of our types has (TestNoKongHooks keeps it so): 0.09 ms of the 0.1
// ms a parse takes. The one hook a parse can meet, kong's --help flag, is handled here as kong would.
func (p *parser) parse(argv []string) (*kong.Context, error) {
	kctx, err := kong.Trace(p.k, argv)
	if err != nil {
		return nil, err
	}
	p.rt.color = colorFlag(kctx) // help prints before the flags apply: on --help, after a usage error, and by help
	if kctx.Error != nil {
		return nil, kctx.Error
	}
	for _, e := range kctx.Path {
		if e.Flag != nil && e.Flag.Name == "help" {
			_ = kctx.PrintUsage(false)
			p.k.Exit(exitOK)
		}
	}
	if err := kctx.Reset(); err != nil {
		return nil, err
	}
	if err := kctx.Resolve(); err != nil {
		return nil, err
	}
	if _, err := kctx.Apply(); err != nil {
		return nil, err
	}
	if err := kctx.Validate(); err != nil {
		return nil, err
	}
	return kctx, nil
}
