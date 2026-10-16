package cli

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// ServeCmd is "millstream serve" (spec sections 5.9 and 14).
type ServeCmd struct {
	Listen   string        `placeholder:"HOST:PORT" help:"Serve the API and the web UI on this address (default: listen, else ui.listen, else 127.0.0.1:7070)."`
	Slots    int           `default:"4" help:"Deliveries the embedded node runs at once (node.embedded)."`
	LeaseTTL time.Duration `name:"lease-ttl" default:"60s" help:"Lease duration; a lost node's leases are retried after it."`
}

// Run runs an engine from the environment file until interrupted: the store its store key names, the packages of
// the store's revisions (loaded from their bundles as they are needed, never from directories), an embedded node
// unless node.embedded is false, and the API and the UI on listen. On Postgres it takes the deployment's leadership
// lock first: while another engine holds it, this one waits as the standby, saying so. Logs are JSON lines on
// stderr.
func (c ServeCmd) Run(rt *Runtime) error {
	ctx, stop := signalContext(rt.Ctx)
	defer stop()
	log := slog.New(slog.NewJSONHandler(rt.IO.Err, nil))
	env, err := environment(rt)
	if err != nil {
		return err
	}
	if err := env.CheckMode(pipeline.ModeProduction); err != nil {
		return err
	}
	if c.Slots <= 0 || c.LeaseTTL <= 0 {
		return usageError("--slots and --lease-ttl must be positive")
	}
	listen := cmp.Or(c.Listen, env.Listen, env.UIListen, defaultListen)
	if err := checkListen(listen, env); err != nil {
		return err
	}
	sec, err := newAPISecurity(rt, env, listen)
	if err != nil {
		return err
	}
	if identity(rt) == "" && env.AuthToken == nil {
		return usageError("serve: no user to record review decisions under: set $USER, or auth.token and auth.user")
	}
	waiting := func() { log.Info("another engine holds the leadership lock: waiting as the standby") }
	return withStoreMode(rt, engineStore, waiting, func(st store.Store) error {
		return c.serve(ctx, rt, env, st, log, listen, sec)
	})
}

func (c ServeCmd) serve(ctx context.Context, rt *Runtime, env *pipeline.Environment, st store.Store, log *slog.Logger,
	listen string, sec apiSecurity,
) error {
	log.Info("leading the deployment", "listen", listen, "embeddedNode", env.NodeEmbedded,
		"mode", env.EffectiveMode(pipeline.ModeProduction))
	notes := notifier(rt, env, log)
	e, err := engine.New(engine.Options{
		Store: st, Logger: log, LeaseTTL: c.LeaseTTL, Env: rt.Env, Secrets: secrets(rt, env),
		Artifacts: artifactStore(rt, env), Resolver: resolver(rt, env, false), Agents: agents(rt, env),
		Notifier: notes, PackageDir: packageDir(rt, env), Check: checkOptions(rt, env),
	})
	if err != nil {
		return err
	}
	// The engine steps down when the store loses its write connection, and the leadership lock with it.
	var wait func(context.Context) error
	if l, ok := st.(interface{ Lost() <-chan struct{} }); ok {
		wait = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return nil
			case <-l.Lost():
				return fmt.Errorf("serve: stepping down: %w", store.ErrWriteLost)
			}
		}
	}
	h := engineHost{listen: listen, sec: sec, embedded: env.NodeEmbedded, slots: c.Slots, leaseTTL: c.LeaseTTL}
	return h.run(ctx, rt, env, st, e, notes, log, wait)
}

// checkListen refuses a listen address that is not a loopback one without auth.token: exposed, the API requires the
// token (spec section 15.2).
func checkListen(listen string, env *pipeline.Environment) error {
	if _, _, err := net.SplitHostPort(listen); err != nil {
		return usageError(fmt.Sprintf("listen %q: want host:port, such as 127.0.0.1:7070", listen))
	}
	if api.CheckLoopback(listen) != nil && env.AuthToken == nil {
		return usageError(fmt.Sprintf("listen %s is not a loopback address: a non-loopback listener requires "+
			"auth.token (spec section 15.2)", listen))
	}
	return nil
}
