package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	osexec "os/exec"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/nodeapi"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
)

// RunCmd is "millstream run": "run [dir…]" runs packages, "run report" prints a run's report.
type RunCmd struct {
	Packages RunPackagesCmd `cmd:"" default:"withargs" implicit:"" help:"Apply packages and run the engine with an embedded node."`
	Report   RunReportCmd   `cmd:"" help:"Print a run's report: critical path, slowest steps, failures, cost, comparison."`
}

// Help is the command's detail, printed by its --help.
func (RunPackagesCmd) Help() string {
	return `Examples:
  # Apply the package in the current directory, fire its manual triggers once, print a summary
  millstream run --once
  # Serve the engine, the API and the UI on 127.0.0.1:7070 until interrupted, logs on stderr
  millstream run 2>engine.log
  # Run two packages, one importing the other's stream, with a variable set
  millstream run ./crawl ./score --var region=lyon`
}

// RunPackagesCmd is "millstream run [dir…]".
type RunPackagesCmd struct {
	Dirs      []string          `arg:"" optional:"" type:"path" help:"Package directories (default: the current one)."`
	Var       map[string]string `help:"Set a variable (name=value)."`
	Once      bool              `help:"Fire each pipeline's manual triggers (or its first cron trigger) once, wait for every run to settle or wait only on reviews, print a summary and exit."`
	Slots     int               `default:"4" help:"Deliveries the embedded node runs at once."`
	RequestID string            `name:"request-id" help:"With --once, the request id of the manual firings, so a rerun resumes the same runs."`
	LeaseTTL  time.Duration     `name:"lease-ttl" default:"60s" help:"Lease duration; a crashed run's leases are retried after it."`
	Listen    string            `placeholder:"HOST:PORT" help:"Serve the API and the web UI on this loopback address (default: ui.listen, else listen, else 127.0.0.1:7070); not with --once."`
}

// defaultListen is where run serves the API and the UI without --listen or ui.listen.
const defaultListen = "127.0.0.1:7070"

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
	listen := ""
	var sec apiSecurity
	if !c.Once {
		listen = cmp.Or(c.Listen, env.UI.Listen, env.Listen, defaultListen)
		if err := api.CheckLoopback(listen); err != nil {
			return usageError(err.Error())
		}
		if sec, err = newAPISecurity(rt, env, listen); err != nil {
			return err
		}
		// The API records the decisions of requests that name nobody under the server's user (spec section 12.1),
		// those of a token session under auth.user.
		if identity(rt) == "" && env.Auth.Token == nil {
			return usageError("run: no user to record review decisions under: set $USER, or auth.token and auth.user")
		}
	} else if c.Listen != "" {
		return usageError("--listen: run --once does not serve the API")
	}
	// The improve pipeline runs while run serves the API its actions call (spec section 11.3).
	var im *improveHost
	if !c.Once {
		if im, err = newImproveHost(rt, env, pipeline.ModeDevelopment); err != nil {
			return err
		}
	}
	actions := resolver(rt, env, false)
	pkgs, err := c.load(rt, log, actions, im.check(checkOptions(rt, env)), im.packages())
	if err != nil {
		return err
	}
	arts, err := artifactStore(rt, env)
	if err != nil {
		return err
	}
	return withStoreMode(rt, engineStore, postgres.Options{}, func(st store.Store) error {
		notes := notifier(rt, env, log)
		id := newEngineID()
		mode := env.EffectiveMode(pipeline.ModeDevelopment)
		bt := newBacktester(rt, env, st, arts, mode, log)
		e, err := engine.New(engine.Options{
			Store: st, Logger: log, LeaseTTL: c.LeaseTTL, Env: rt.Env, Secrets: im.secrets(secrets(rt, env)), Flags: c.Var,
			NoCron: c.Once, Artifacts: arts, Resolver: actions, Agents: agents(rt, env),
			Notifier: notes, PackageDir: packageDir(env), Check: im.check(checkOptions(rt, env)), Retention: env.Retention,
			Leadership: engine.Leadership{ID: id}, Improve: env.Improve, Proposals: proposalOptions(rt, env, bt),
			Mode: mode,
		})
		if err != nil {
			return err
		}
		for _, pkg := range pkgs {
			a, err := e.Apply(ctx, pkg)
			if err != nil {
				return err
			}
			printTokens(rt, a.Pipeline, api.AppliedOf(a).Tokens)
		}
		if err := im.start(ctx, e); err != nil {
			return err
		}
		var runs []engine.RunRef
		if c.Once {
			if runs, err = c.fire(ctx, e, pkgs); err != nil {
				return err
			}
		}
		var wait func(context.Context) error
		if c.Once {
			wait = func(ctx context.Context) error {
				var err error
				if runs, err = waitAll(ctx, e, st, pkgs, runs); err != nil {
					return err
				}
				log.Info("every run settled", "runs", len(runs))
				// The runs' metrics and insights, as the engine's projectors would find them at their next pass.
				if err := e.Project(ctx); err != nil {
					log.Warn("projections", "error", err.Error())
				}
				// And the retention job's pass, which a one-shot run would otherwise never reach.
				if err := e.Retain(ctx); err != nil {
					log.Warn("retention", "error", err.Error())
				}
				return nil
			}
		}
		h := engineHost{listen: listen, sec: sec, embedded: true, slots: c.Slots, leaseTTL: c.LeaseTTL,
			lead: leadership{identity: api.Identity{ID: id, Since: time.Now()}}, arts: arts, laptop: true,
			mode: mode, backtests: bt, served: im.served}
		if err := h.run(ctx, rt, env, st, e, notes, log, wait); err != nil {
			return err
		}
		if !c.Once {
			return nil
		}
		return summarize(rt, st, runs)
	})
}

// engineHost is how run and serve host an engine: its embedded node, when it has one, and the API and the UI on
// site, or on listen unless it is empty.
type engineHost struct {
	listen   string
	site     *site       // serve's, across its terms; nil makes run listen on listen for the engine's lifetime
	sec      apiSecurity // the API server's token and TLS
	embedded bool
	slots    int
	leaseTTL time.Duration
	lead     leadership // who the engine is on a deployment with a standby; for run, its id alone
	arts     artifacts.Store
	// nodeToken serves the node protocol to remote nodes (serve with node.token); empty serves none.
	nodeToken string
	// laptop makes the embedded node take every delivery whatever its placement: run's, a laptop's only node.
	laptop bool
	// mode is the environment's effective mode (spec section 11.2): run defaults to development, serve to production.
	mode string
	// backtests runs the engine's proposal validation and serves the API's distill training sets, sharing one improve budget.
	backtests *backtest.Backtester
	// served, when set, learns the site's URL once it serves: the improve pipeline's API secret.
	served func(url string)
}

// run runs the engine, its embedded node and the API server until ctx ends or wait, called once the engine is
// ready, returns; nil waits for ctx. It stops them all then, draining the node's deliveries in flight, or
// abandoning them when wait's error wraps node.ErrAbandon: the engine stepped down.
func (h engineHost) run(ctx context.Context, rt *Runtime, env *pipeline.Environment, st store.Store, e *engine.Engine,
	notes *review.Notifier, log *slog.Logger, wait func(context.Context) error,
) error {
	run := launchers(rt, env)
	pool := exec.NewPool(exec.PoolOptions{Options: exec.Options{Logger: log, Launchers: run}})
	defer func() { _ = pool.Close() }()
	// The embedded node is named after the engine's id, a host name and a random suffix: a process id would name
	// the embedded nodes of two engines in containers alike, embedded-1 both.
	owner := "embedded-" + h.lead.identity.ID
	// The node's lines about its deliveries, the actions' stderr among them, feed the live log tail too.
	logs := api.NewLogRing(0, time.Now())
	var n *node.Node
	var nodes []api.Node
	if h.embedded {
		var err error
		n, err = node.New(e, node.Options{
			Owner: owner, Slots: h.slots, Pool: pool, Logger: slog.New(logs.Handler(log.Handler())),
			HeartbeatInterval: node.HeartbeatFor(h.leaseTTL),
		})
		if err != nil {
			return err
		}
		runtimes := node.Runtimes(sortedKeys(run), agents(rt, env).Agents, func(p string) bool {
			_, err := osexec.LookPath(p)
			return err == nil
		})
		e.AddNode(engine.NodeInfo{ID: owner, Version: rt.Version, Runtimes: runtimes, Slots: h.slots,
			AnyPlacement: h.laptop})
		nodes = []api.Node{{ID: owner, Embedded: true, Version: rt.Version, Slots: h.slots, Runtimes: runtimes}}
	}
	var nodeAPI http.Handler
	if h.nodeToken != "" {
		var err error
		if nodeAPI, err = nodeapi.New(nodeapi.Options{Engine: e, Token: h.nodeToken, Artifacts: h.arts,
			Version: rt.Version, Relay: slog.New(logs.Handler(log.Handler())), Logger: log}); err != nil {
			return err
		}
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	engineDone, nodeDone, serveDone, siteDone := make(chan error, 1), make(chan error, 1), make(chan error, 1),
		make(chan error, 1)
	site := h.site
	if site == nil && h.listen != "" {
		var err error
		if site, err = listenSite(runCtx, h.listen, h.sec.tls, func() { announce(rt, site) }); err != nil {
			return err
		}
		go func() { siteDone <- site.serve(runCtx) }()
	} else {
		siteDone <- nil
	}
	if site != nil && h.served != nil {
		h.served(site.url)
	}
	if site == nil {
		serveDone <- nil
	} else {
		pub, err := newPublisher(env, e)
		var srv *apiServer
		if err == nil {
			srv, err = newAPIServer(st, e, notes, logs, apiExtras{rt: rt, arts: h.arts, nodes: nodes, nodeAPI: nodeAPI,
				leaseTTL: h.leaseTTL, mode: h.mode, improve: env.Improve.Enabled,
				evidence: api.EvidenceSettings{Mode: h.mode,
					Sample: env.Improve.Evidence.Sample, TTL: time.Duration(env.Improve.Evidence.TTL)},
				training: h.backtests, publisher: pub}, h.lead, h.sec, log)
		}
		if err != nil {
			cancel(nil)
			return errors.Join(err, <-siteDone)
		}
		if err := srv.start(runCtx); err != nil {
			cancel(nil)
			return errors.Join(err, <-siteDone)
		}
		site.set(runCtx, srv.handler)
		go func() { serveDone <- srv.run(runCtx) }()
	}
	go func() { engineDone <- e.Run(runCtx) }()
	// The node pulls only once the engine has rebuilt its leases in flight from the store.
	select {
	case <-e.Ready():
	case err := <-engineDone:
		cancel(nil)
		return errors.Join(err, <-serveDone, <-siteDone)
	}
	if n != nil {
		go func() { nodeDone <- n.Run(runCtx) }()
	} else {
		nodeDone <- nil
	}
	var waitErr error
	if wait != nil {
		waitErr = wait(runCtx)
	} else {
		<-runCtx.Done()
	}
	cancel(waitErr)
	return errors.Join(waitErr, <-nodeDone, <-engineDone, <-serveDone, <-siteDone)
}

// announce says where the site serves, once it does.
func announce(rt *Runtime, s *site) {
	fmt.Fprintf(rt.IO.Out, "Millstream is serving the UI and the API on %s\n", s.url)
}

// load loads and checks each package, logging each diagnostic, and fails on any check error and on a --var no
// package declares.
func (c RunPackagesCmd) load(rt *Runtime, log *slog.Logger, actions pipeline.ActionResolver, opts pipeline.CheckOptions,
	extra []*pipeline.Package,
) ([]*pipeline.Package, error) {
	dirs := c.Dirs
	if len(dirs) == 0 {
		dirs = []string{dirOrCurrent("")}
	}
	if c.Slots <= 0 || c.LeaseTTL <= 0 {
		return nil, usageError("--slots and --lease-ttl must be positive")
	}
	var pkgs []*pipeline.Package
	declared := map[string]bool{}
	for _, dir := range dirs {
		pkg, err := pipeline.Load(dir)
		if err != nil {
			return nil, err
		}
		for name := range pkg.Pipeline.Vars {
			declared[name] = true
		}
		pkgs = append(pkgs, pkg)
	}
	pkgs = append(pkgs, extra...)
	failed := false
	diags := checkPackages(rt, pkgs, actions, opts)
	for _, pkg := range pkgs {
		for _, d := range diags[pkg.Pipeline.Name] {
			level := slog.LevelWarn
			if d.Severity == pipeline.SeverityError {
				level, failed = slog.LevelError, true
			}
			log.Log(rt.Ctx, level, "check", "pipeline", pkg.Pipeline.Name, "path", d.Path, "message", d.Message)
		}
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

// waitAll waits until the runs fired and every run of the packages' pipelines still unsettled have settled or wait
// only on reviews, and returns them all: a run decided since the last run --once resumes. The runs that imported
// streams open for them (spec section 4.12) are waited on too, and the runs those open in turn: a producer run
// settles only once each of them is open, so asking again after it settled finds them all.
func waitAll(ctx context.Context, e *engine.Engine, st store.Store, pkgs []*pipeline.Package,
	fired []engine.RunRef,
) ([]engine.RunRef, error) {
	runs := slices.Clone(fired)
	add := func(found []store.Run) {
		for _, r := range found {
			ref := engine.RunRef{Pipeline: r.Pipeline, ID: r.ID}
			if !slices.Contains(runs, ref) {
				runs = append(runs, ref)
			}
		}
	}
	for _, pkg := range pkgs {
		open, err := st.Runs(ctx, store.RunFilter{Pipeline: pkg.Pipeline.Name, Unsettled: true})
		if err != nil {
			return nil, err
		}
		add(open)
	}
	for waited := 0; waited < len(runs); {
		if err := e.WaitSettled(ctx, runs); err != nil {
			return nil, err
		}
		n := len(runs)
		for _, ref := range runs[waited:n] {
			opened, err := st.Runs(ctx, store.RunFilter{Producer: ref.Pipeline, ProducerRun: ref.ID})
			if err != nil {
				return nil, err
			}
			add(opened)
		}
		waited = n
	}
	return runs, nil
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
