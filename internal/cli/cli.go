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
	// DB is the state database path, from the global --db flag; DBSet says the flag was given, for the commands
	// that write to the database only when asked to (eval).
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
	DB      string `name:"db" default:".millstream/millstream.db" help:"State database path (created with its directory)."`
	EnvFile string `name:"env-file" type:"path" help:"Environment file (default: ./millstream.yaml when it exists)."`
	Server  string `name:"server" placeholder:"URL" help:"Change state through the API of the engine serving at this URL (default: $MILLSTREAM_SERVER; token: $MILLSTREAM_TOKEN)."`
	Direct  bool   `name:"direct" help:"Change state from this process, not through --server; on Postgres this takes the leadership lock, waiting while an engine runs."`

	Run      RunCmd      `cmd:"" help:"Apply packages and run the engine with an embedded node; run report prints a run's report."`
	Serve    ServeCmd    `cmd:"" help:"Run an engine from the environment file; a second one on the same Postgres store waits as the standby."`
	Node     NodeCmd     `cmd:"" help:"Run a remote node that joins the engine with the node token; node drain drains it."`
	Nodes    NodesCmd    `cmd:"" help:"List the remote nodes, or revoke one."`
	Apply    ApplyCmd    `cmd:"" help:"Check packages against the whole deployment and upload them as new revisions."`
	Rollback RollbackCmd `cmd:"" help:"Reactivate an earlier revision of a pipeline."`
	Disable  DisableCmd  `cmd:"" help:"Disable a pipeline: its triggers stop firing and its deliveries wait until resume."`
	Check    CheckCmd    `cmd:"" help:"Validate a pipeline package."`
	Lock     LockCmd     `cmd:"" help:"Pin a package's container and registry actions by image digest in millstream.lock."`
	Graph    GraphCmd    `cmd:"" help:"Print a package's graph as Mermaid or DOT."`
	Vars     VarsCmd     `cmd:"" help:"Print a package's variables, values and sources."`
	Status   StatusCmd   `cmd:"" help:"Show pipelines and recent runs."`
	Tail     TailCmd     `cmd:"" help:"Print events as JSON lines."`
	Trigger  TriggerCmd  `cmd:"" help:"Fire a trigger; trigger token --rotate replaces a webhook trigger's token."`
	Cancel   CancelCmd   `cmd:"" help:"Cancel a run."`
	Replay   ReplayCmd   `cmd:"" help:"Replay dead letters into their run, or a step over past items in a replay run."`
	DLQ      DLQCmd      `cmd:"" name:"dlq" help:"List and discard dead letters."`
	Purge    PurgeCmd    `cmd:"" help:"Erase the items with a key, or an item, with their descendants and their copies in other pipelines."`
	Inbox    InboxCmd    `cmd:"" help:"List and decide reviews and review samples."`
	Pause    PauseCmd    `cmd:"" help:"Pause a pipeline or one of its steps."`
	Resume   ResumeCmd   `cmd:"" help:"Resume a pipeline or one of its steps."`
	Init     InitCmd     `cmd:"" help:"Scaffold a pipeline package or an action."`
	Test     TestCmd     `cmd:"" help:"Test a package against stubbed actions, or an action against golden outputs."`
	Eval     EvalCmd     `cmd:"" help:"Run an action's evals and score them."`
	Metrics  MetricsCmd  `cmd:"" help:"Print metric rollups, or their Prometheus exposition."`
	Insights InsightsCmd `cmd:"" help:"List, acknowledge and mute insights."`
	Evidence EvidenceCmd `cmd:"" help:"Export an evidence bundle in production; import it in development."`
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

	p, err := takeParser(rt)
	if err != nil {
		fmt.Fprintf(rt.IO.Err, "millstream: %v\n", err)
		return exitFail
	}
	defer p.release()
	parser := p.k
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
