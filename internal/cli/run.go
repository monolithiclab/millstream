package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// RunCmd is "millstream run": "run [dir…]" runs packages, "run report" prints a run's report.
type RunCmd struct {
	Packages RunPackagesCmd `cmd:"" default:"withargs" hidden:"" help:"Apply packages and run the engine with an embedded node."`
	Report   RunReportCmd   `cmd:"" help:"Print a run's report: critical path, slowest steps, failures, cost, comparison."`
}

// RunPackagesCmd is "millstream run [dir…]".
type RunPackagesCmd struct {
	Dirs      []string          `arg:"" optional:"" type:"path" help:"Package directories (default: the current one)."`
	Var       map[string]string `help:"Set a variable (name=value)."`
	Once      bool              `help:"Fire each pipeline's manual triggers (or its first cron trigger) once, wait for every run to settle, print a summary and exit."`
	Slots     int               `default:"4" help:"Deliveries the embedded node runs at once."`
	RequestID string            `name:"request-id" help:"With --once, the request id of the manual firings, so a rerun resumes the same runs."`
	LeaseTTL  time.Duration     `name:"lease-ttl" default:"60s" help:"Lease duration; a crashed run's leases are retried after it."`
}

// signalContext returns ctx cancelled on SIGINT or SIGTERM as well.
func signalContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
}

// Run checks and applies each package, then runs the engine and an embedded node until interrupted, draining
// in-flight deliveries. With --once it fires one run per pipeline, waits until those runs and every other unsettled
// run of the pipelines settle (runs left by a crash, runs reopened by a replay), prints a summary and fails unless
// they all succeeded. Logs are JSON lines on stderr.
func (c RunPackagesCmd) Run(rt *Runtime) error {
	ctx, stop := signalContext(rt.Ctx)
	defer stop()
	log := slog.New(slog.NewJSONHandler(rt.IO.Err, nil))
	env, err := environment(rt)
	if err != nil {
		return err
	}
	actions := resolver(rt, env, false)
	pkgs, err := c.load(rt, log, actions, checkOptions(env))
	if err != nil {
		return err
	}
	return withStore(rt, func(st *sqlite.Store) error {
		e, err := engine.New(engine.Options{
			Store: st, Logger: log, LeaseTTL: c.LeaseTTL, Env: rt.Env, Flags: c.Var, NoCron: c.Once,
			Artifacts: artifactStore(rt), Resolver: actions, Agents: agents(rt, env),
		})
		if err != nil {
			return err
		}
		for _, pkg := range pkgs {
			if _, err := e.Apply(ctx, pkg); err != nil {
				return err
			}
		}
		var runs []engine.RunRef
		if c.Once {
			if runs, err = c.fire(ctx, e, pkgs); err != nil {
				return err
			}
		}
		pool := exec.NewPool(exec.PoolOptions{Options: exec.Options{Logger: log, Launchers: launchers(rt, env)}})
		defer func() { _ = pool.Close() }()
		n, err := node.New(e, node.Options{
			Owner: "embedded-" + strconv.Itoa(os.Getpid()), Slots: c.Slots, Pool: pool, Logger: log,
			HeartbeatInterval: min(node.DefaultHeartbeatInterval, c.LeaseTTL/3),
		})
		if err != nil {
			return err
		}
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		engineDone, nodeDone := make(chan error, 1), make(chan error, 1)
		go func() { engineDone <- e.Run(runCtx) }()
		// The node pulls only once the engine has rebuilt its leases in flight from the store.
		select {
		case <-e.Ready():
		case err := <-engineDone:
			return err
		}
		go func() { nodeDone <- n.Run(runCtx) }()
		var waitErr error
		if c.Once {
			if runs, waitErr = waitAll(runCtx, e, st, pkgs, runs); waitErr == nil {
				log.Info("every run settled", "runs", len(runs))
				// The runs' metrics and insights, as the engine's projectors would find them at their next pass.
				if err := e.Project(runCtx); err != nil {
					log.Warn("projections", "error", err.Error())
				}
			}
		} else {
			<-runCtx.Done()
		}
		cancel()
		if err := errors.Join(waitErr, <-nodeDone, <-engineDone); err != nil {
			return err
		}
		if !c.Once {
			return nil
		}
		return summarize(rt, st, runs)
	})
}

// load loads and checks each package, logging each diagnostic, and fails on any check error and on a --var no
// package declares.
func (c RunPackagesCmd) load(rt *Runtime, log *slog.Logger, actions pipeline.ActionResolver, opts pipeline.CheckOptions,
) ([]*pipeline.Package, error) {
	dirs := c.Dirs
	if len(dirs) == 0 {
		dirs = []string{"."}
	}
	if c.Slots <= 0 || c.LeaseTTL <= 0 {
		return nil, usageError("--slots and --lease-ttl must be positive")
	}
	var pkgs []*pipeline.Package
	declared := map[string]bool{}
	failed := false
	for _, dir := range dirs {
		pkg, err := pipeline.Load(dir)
		if err != nil {
			return nil, err
		}
		diags := pipeline.Check(pkg, actions, rt.Env, opts)
		for _, d := range diags {
			level := slog.LevelWarn
			if d.Severity == pipeline.SeverityError {
				level = slog.LevelError
			}
			log.Log(rt.Ctx, level, "check", "pipeline", pkg.Pipeline.Name, "path", d.Path, "message", d.Message)
		}
		if pipeline.HasErrors(diags) {
			failed = true
		}
		for name := range pkg.Pipeline.Vars {
			declared[name] = true
		}
		pkgs = append(pkgs, pkg)
	}
	if failed {
		return nil, errFailed
	}
	for _, name := range slices.Sorted(maps.Keys(c.Var)) {
		if !declared[name] {
			return nil, usageError(fmt.Sprintf("--var %s: no pipeline declares this variable", name))
		}
	}
	return pkgs, nil
}

// fire fires each pipeline's manual triggers under the request id, or its first cron trigger for the current
// minute when it has no manual trigger.
func (c RunPackagesCmd) fire(ctx context.Context, e *engine.Engine, pkgs []*pipeline.Package) ([]engine.RunRef, error) {
	reqID := c.RequestID
	if reqID == "" {
		reqID = engine.NewRequestID(time.Now())
	}
	var runs []engine.RunRef
	for _, pkg := range pkgs {
		p := pkg.Pipeline
		var manual, crons []string
		for _, name := range slices.Sorted(maps.Keys(p.Triggers)) {
			switch t := p.Triggers[name]; {
			case t.Manual != nil:
				manual = append(manual, name)
			case t.Cron != nil:
				crons = append(crons, name)
			}
		}
		for _, name := range manual {
			id, _, err := e.Fire(ctx, p.Name, name, reqID, nil)
			if err != nil {
				return nil, err
			}
			runs = append(runs, engine.RunRef{Pipeline: p.Name, ID: id})
		}
		if len(manual) == 0 && len(crons) > 0 {
			id, _, err := e.FireCron(ctx, p.Name, crons[0], time.Now().Truncate(time.Minute))
			if err != nil {
				return nil, err
			}
			runs = append(runs, engine.RunRef{Pipeline: p.Name, ID: id})
		}
	}
	return runs, nil
}

// waitAll waits until the runs fired and every run of the packages' pipelines still unsettled have settled, and
// returns them all.
func waitAll(ctx context.Context, e *engine.Engine, st store.Store, pkgs []*pipeline.Package,
	fired []engine.RunRef,
) ([]engine.RunRef, error) {
	runs := slices.Clone(fired)
	for _, pkg := range pkgs {
		open, err := st.Runs(ctx, store.RunFilter{Pipeline: pkg.Pipeline.Name, State: store.RunRunning})
		if err != nil {
			return nil, err
		}
		for _, r := range open {
			ref := engine.RunRef{Pipeline: r.Pipeline, ID: r.ID}
			if !slices.Contains(runs, ref) {
				runs = append(runs, ref)
			}
		}
	}
	return runs, e.WaitSettled(ctx, runs)
}

// summarize prints each run with its state and item counts per stream, and fails unless every run succeeded.
func summarize(rt *Runtime, st store.Store, runs []engine.RunRef) error {
	ok := true
	for _, ref := range runs {
		r, err := st.Run(rt.Ctx, ref.Pipeline, ref.ID)
		if err != nil {
			return err
		}
		ok = ok && r.State == store.RunSucceeded
		fmt.Fprintf(rt.IO.Out, "%s %s %s\n", r.Pipeline, r.ID, r.State)
		counts, err := streamCounts(rt.Ctx, st, r)
		if err != nil {
			return err
		}
		for _, stream := range sortedKeys(counts) {
			fmt.Fprintf(rt.IO.Out, "  %s %d\n", stream, counts[stream])
		}
	}
	if !ok {
		return errFailed
	}
	return nil
}
