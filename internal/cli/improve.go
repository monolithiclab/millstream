package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/improve"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
)

// The improve pipeline (spec section 11.3) as run and serve host it: the package the environment file configures,
// applied with the others, and the secrets the engine gives it, the API's URL once it serves and the proposals-only
// token it mints at start.

// improveHost is the improve pipeline of one engine.
type improveHost struct {
	pkg *pipeline.Package

	mu         sync.Mutex
	url, token string
}

// newImproveHost returns the improve pipeline the environment file asks for, nil when improve.enabled is not set,
// and refuses one it may not run (improve.Check). def is the command's default mode.
func newImproveHost(rt *Runtime, env *pipeline.Environment, def string) (*improveHost, error) {
	if !env.Improve.Enabled {
		return nil, nil
	}
	if err := improve.Check(env, def); err != nil {
		return nil, err
	}
	files, err := improve.Files(env)
	if err != nil {
		return nil, err
	}
	pkg, err := pipeline.Materialize(packageDir(env), files)
	if err != nil {
		return nil, fmt.Errorf("the improve package: %w", err)
	}
	return &improveHost{pkg: pkg}, nil
}

// secrets wraps how pipelines resolve secrets: the improve pipeline's improveApi and improveToken are the host's.
// A nil host changes nothing.
func (h *improveHost) secrets(base func(string) pipeline.SecretLookup) func(string) pipeline.SecretLookup {
	if h == nil {
		return base
	}
	return func(name string) pipeline.SecretLookup {
		lookup := base(name)
		if name != improve.Name {
			return lookup
		}
		return func(secret string) (string, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			switch secret {
			case improve.SecretAPI:
				return h.url, nil
			case improve.SecretToken:
				return h.token, nil
			}
			return lookup(secret)
		}
	}
}

// packages is the host's package, none for a nil host: the packages run checks and applies with its own.
func (h *improveHost) packages() []*pipeline.Package {
	if h == nil {
		return nil
	}
	return []*pipeline.Package{h.pkg}
}

// check is the check options with the host's secrets.
func (h *improveHost) check(opts pipeline.CheckOptions) pipeline.CheckOptions {
	opts.Secrets = h.secrets(opts.Secrets)
	return opts
}

// start mints the proposals-only token the package calls the API with, replacing any earlier one.
func (h *improveHost) start(ctx context.Context, e *engine.Engine) error {
	if h == nil {
		return nil
	}
	token, err := e.MintProposalToken(ctx)
	if err != nil {
		return fmt.Errorf("the improve pipeline's token: %w", err)
	}
	h.mu.Lock()
	h.token = token
	h.mu.Unlock()
	return nil
}

// served records where the API serves, the URL the package calls.
func (h *improveHost) served(url string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.url = url
	h.mu.Unlock()
}

// apply checks the package and applies it, for an engine that applies no package of its own (serve, improve).
func (h *improveHost) apply(ctx context.Context, rt *Runtime, env *pipeline.Environment, e *engine.Engine,
	log *slog.Logger,
) error {
	failed := false
	for _, d := range pipeline.Check(h.pkg, resolver(rt, env, false), rt.Env, h.check(checkOptions(rt, env))) {
		level := slog.LevelWarn
		if d.Severity == pipeline.SeverityError {
			level, failed = slog.LevelError, true
		}
		log.Log(ctx, level, "check", "pipeline", improve.Name, "path", d.Path, "message", d.Message)
	}
	if failed {
		return fmt.Errorf("the improve package fails its check: %w", errFailed)
	}
	_, err := e.Apply(ctx, h.pkg)
	return err
}

// ImproveCmd is "millstream improve": the improve pipeline's manual trigger, "Propose a fix".
type ImproveCmd struct {
	Insight  string        `help:"Propose a fix for this insight (default: every open insight at or above improve.minSeverity)."`
	Pipeline string        `help:"Only this pipeline's insights."`
	Slots    int           `default:"4" help:"Deliveries the embedded node runs at once."`
	LeaseTTL time.Duration `name:"lease-ttl" default:"60s" help:"Lease duration."`
}

// Run fires the improve pipeline's propose trigger: through the served engine's API with --server, printing the
// run; else from this process, development only, which runs an engine with the improve package alone, serving
// the API on a loopback port for the package's actions, until the run settles, and prints what it proposed.
func (c ImproveCmd) Run(rt *Runtime) error {
	in := map[string]string{}
	if c.Insight != "" {
		in["insight"] = c.Insight
	}
	if c.Pipeline != "" {
		in["pipeline"] = c.Pipeline
	}
	payload, _ := json.Marshal(in)
	cl, err := remote(rt)
	if err != nil {
		return err
	}
	if cl != nil {
		var run api.Run
		if _, err := cl.post(rt.Ctx, pipelinePath(improve.Name, "runs"), api.TriggerRequest{Trigger: "propose",
			Payload: payload}, &run); err != nil {
			return err
		}
		_, err = fmt.Fprintf(rt.IO.Out, "%s\n", run.ID)
		return err
	}
	return c.direct(rt, payload)
}

// direct runs the improve pipeline from this process.
func (c ImproveCmd) direct(rt *Runtime, payload json.RawMessage) error {
	if c.Slots <= 0 || c.LeaseTTL <= 0 {
		return usageError("--slots and --lease-ttl must be positive")
	}
	ctx, stop := signalContext(rt.Ctx)
	defer stop()
	log := slog.New(slog.NewJSONHandler(rt.IO.Err, nil))
	env, err := environment(rt)
	if err != nil {
		return err
	}
	if err := improve.Check(env, pipeline.ModeDevelopment); err != nil {
		return err // newImproveHost would answer nil without improve.enabled
	}
	im, err := newImproveHost(rt, env, pipeline.ModeDevelopment)
	if err != nil {
		return err
	}
	const listen = "127.0.0.1:0"
	sec, err := newAPISecurity(rt, env, listen)
	if err != nil {
		return err
	}
	arts, err := artifactStore(rt, env)
	if err != nil {
		return err
	}
	var run engine.RunRef
	err = withStoreMode(rt, engineStore, postgres.Options{}, func(st store.Store) error {
		id, mode := newEngineID(), pipeline.ModeDevelopment
		bt := newBacktester(rt, env, st, arts, mode, log)
		e, err := engine.New(engine.Options{
			Store: st, Logger: log, LeaseTTL: c.LeaseTTL, Env: rt.Env, Secrets: im.secrets(secrets(rt, env)),
			NoCron: true, Artifacts: arts, Resolver: resolver(rt, env, false), Agents: agents(rt, env),
			Notifier: notifier(rt, env, log), PackageDir: packageDir(env), Check: im.check(checkOptions(rt, env)),
			Leadership: engine.Leadership{ID: id}, Improve: env.Improve, Proposals: proposalOptions(rt, env, bt),
		})
		if err != nil {
			return err
		}
		if err := im.apply(ctx, rt, env, e, log); err != nil {
			return err
		}
		if err := im.start(ctx, e); err != nil {
			return err
		}
		wait := func(ctx context.Context) error {
			// The insights as the engine's projector finds them now, so the trigger reads the latest.
			if err := e.Project(ctx); err != nil {
				log.Warn("projections", "error", err.Error())
			}
			runID, _, err := e.Fire(ctx, improve.Name, "propose", engine.NewRequestID(time.Now()), payload)
			if err != nil {
				return err
			}
			run = engine.RunRef{Pipeline: improve.Name, ID: runID}
			return e.WaitSettled(ctx, []engine.RunRef{run})
		}
		h := engineHost{listen: listen, sec: sec, embedded: true, slots: c.Slots, leaseTTL: c.LeaseTTL,
			lead: leadership{identity: api.Identity{ID: id, Since: time.Now()}}, arts: arts, laptop: true,
			mode: mode, backtests: bt, served: im.served}
		if err := h.run(ctx, rt, env, st, e, notifier(rt, env, log), log, wait); err != nil {
			return err
		}
		return printImprove(rt, st, run)
	})
	return err
}

// printImprove prints the run and what became of each idea it drafted, and fails unless the run succeeded.
func printImprove(rt *Runtime, st store.Store, ref engine.RunRef) error {
	r, err := st.Run(rt.Ctx, ref.Pipeline, ref.ID)
	if err != nil {
		return err
	}
	fmt.Fprintf(rt.IO.Out, "%s %s %s\n", r.Pipeline, r.ID, r.State)
	items, err := st.Items(rt.Ctx, store.ItemFilter{Pipeline: ref.Pipeline, RunID: ref.ID,
		Stream: "improve.published"})
	if err != nil {
		return err
	}
	for _, it := range items {
		var o improve.Outcome
		if json.Unmarshal(it.Payload, &o) != nil {
			continue
		}
		line := []string{" ", o.Outcome, o.Kind, "on", o.Pipeline}
		if o.Proposal != "" {
			line = append(line, o.Proposal, o.State)
		}
		if o.Message != "" {
			line = append(line, "("+o.Message+")")
		}
		fmt.Fprintln(rt.IO.Out, strings.Join(line, " "))
	}
	if r.State != store.RunSucceeded {
		return errFailed
	}
	return nil
}

// ImproveActionCmd is "millstream improve-action <step>", hidden: the action process of the improve package's exec
// steps (spec section 11.3). It speaks the action protocol on stdin and stdout; its logs go to stderr.
type ImproveActionCmd struct {
	Step string `arg:"" help:"The step: gather-evidence, validate, propose or publish."`
}

// Run serves the protocol until shutdown.
func (c ImproveActionCmd) Run(rt *Runtime) error {
	return improve.Serve(rt.Ctx, c.Step, rt.IO.In, rt.IO.Out,
		improve.Options{Logger: slog.New(slog.NewTextHandler(rt.IO.Err, nil))})
}
