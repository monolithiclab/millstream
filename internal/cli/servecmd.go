package cli

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/nodeapi"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
	"github.com/monolithiclab/millstream/internal/ui"
)

// ServeCmd is "millstream serve" (spec sections 5.9 and 14).
type ServeCmd struct {
	Listen   string        `placeholder:"HOST:PORT" help:"Serve the API and the web UI on this address (default: listen, else ui.listen, else 127.0.0.1:7070)."`
	Slots    int           `default:"4" help:"Deliveries the embedded node runs at once (node.embedded)."`
	LeaseTTL time.Duration `name:"lease-ttl" default:"60s" help:"Lease duration; a lost node's leases are retried after it."`
}

// Run runs an engine from the environment file until interrupted: the store its store key names, the packages of
// the store's revisions (loaded from their bundles as they are needed, never from directories), an embedded node
// unless node.embedded is false, and the API and the UI on listen. On Postgres it leads the deployment or stands by
// (lead). Logs are JSON lines on stderr.
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
	arts, err := artifactStore(rt, env)
	if err != nil {
		return err
	}
	nodeToken, err := readNodeToken(rt, env)
	if err != nil {
		return err
	}
	if identity(rt) == "" && env.AuthToken == nil {
		return usageError("serve: no user to record review decisions under: set $USER, or auth.token and auth.user")
	}
	var site *site
	site, err = listenSite(ctx, listen, sec.tls, func() { announce(rt, site) })
	if err != nil {
		return err
	}
	siteCtx, stopSite := context.WithCancel(ctx)
	siteDone := make(chan error, 1)
	go func() { siteDone <- site.serve(siteCtx) }()
	err = c.serveOn(ctx, rt, env, log, site, sec, arts, nodeToken)
	stopSite()
	return errors.Join(err, <-siteDone)
}

// serveOn runs the engine on site until ctx ends: on SQLite (or a store a test hands in) one engine, without a
// standby (spec section 5.9); on Postgres, lead's terms and standby.
func (c ServeCmd) serveOn(ctx context.Context, rt *Runtime, env *pipeline.Environment, log *slog.Logger, site *site,
	sec apiSecurity, arts artifacts.Store, nodeToken string,
) error {
	s := &server{cmd: c, rt: rt, env: env, log: log, site: site, sec: sec, id: newEngineID(), arts: arts,
		nodeToken: nodeToken}
	s.open, s.run = s.acquire, s.term
	if rt.Store == nil && env.Store.PostgresDSN != nil {
		return s.lead(ctx)
	}
	return withStoreMode(rt, engineStore, postgres.Options{}, func(st store.Store) error {
		return s.term(ctx, st, leadership{identity: api.Identity{ID: s.id, Since: time.Now()}})
	})
}

// server is a serve process: the engine of each term it leads, and its standby API between them, on one site.
type server struct {
	cmd  ServeCmd
	rt   *Runtime
	env  *pipeline.Environment
	log  *slog.Logger
	site *site
	sec  apiSecurity // the API's token and TLS, the active engine's and the standby's
	id   string      // the engine's id, the same across the process's terms
	arts artifacts.Store
	// nodeToken is node.token's value, empty without it: the node protocol is served only with it.
	nodeToken string
	// open and run are acquire and term: tests replace them.
	open func(ctx context.Context) (store.Store, error)
	run  func(ctx context.Context, st store.Store, lead leadership) error
}

// leadership is who an engine is on a deployment with a standby, as its API reports it: its identity, and the
// deployment's engines.
type leadership struct {
	identity   api.Identity
	deployment api.Deployment
}

// errSteppedDown ends a term whose store lost its write connection, and the leadership lock with it: the embedded
// node abandons its deliveries, and the process stands by again.
var errSteppedDown = fmt.Errorf("serve: stepping down: %w", node.ErrAbandon)

// lead runs serve on Postgres (spec section 5.9): it takes the deployment's leadership lock, serving the standby's
// read-only API while another engine holds it; leads until the store loses its write connection, then steps down
// at once and stands by again, rather than exiting; and returns when ctx ends. A first store that cannot be opened
// fails serve; a later one is retried every engine.lockCheck, as the database may be failing over.
func (s *server) lead(ctx context.Context) error {
	for first := true; ; first = false {
		st, err := s.open(ctx)
		switch {
		case ctx.Err() != nil:
			if st != nil {
				return st.Close()
			}
			return nil
		case err != nil && first:
			return err
		case err != nil:
			s.log.Error("open the store again", "engine", s.id, "error", err.Error())
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(cmp.Or(s.env.Engine.LockCheck, postgres.DefaultCheckEvery)):
			}
			continue
		}
		var lead leadership
		if pg, ok := st.(*postgres.Store); ok {
			l := pg.Leadership()
			lead = leadership{identity: api.Identity{ID: s.id, Since: l.Since, Epoch: l.Epoch},
				deployment: pgDeployment{pg}}
		}
		err = s.run(ctx, st, lead)
		cerr := st.Close()
		if ctx.Err() != nil {
			return cerr // stopped, perhaps while the term was starting: not the term's failure
		}
		err = errors.Join(err, cerr)
		if !errors.Is(err, errSteppedDown) {
			return err
		}
		s.log.Info("stepped down: standing by again", "engine", s.id, "epoch", lead.identity.Epoch)
	}
}

// acquire opens the store, taking the leadership lock, and serves the standby's read-only API meanwhile when
// another engine holds it: the database is migrated then, so a read-only store can open.
func (s *server) acquire(ctx context.Context) (store.Store, error) {
	var wg sync.WaitGroup
	defer wg.Wait()
	standbyCtx, stopStandby := context.WithCancel(ctx)
	defer stopStandby()
	since := time.Now()
	opts := postgres.Options{
		Engine: postgres.Engine{ID: s.id, Version: s.rt.Version, Address: s.site.addr},
		Waiting: func() {
			s.log.Info("another engine holds the leadership lock: waiting as the standby", "engine", s.id)
			wg.Go(func() { s.standby(standbyCtx, since) })
		},
	}
	return openStore(ctx, s.rt, s.env, engineStore, opts)
}

// standby serves the standby's API until ctx ends: the reads and the UI from a read-only store, every action
// refused with the active engine's id and address. Without a read-only store it serves nothing, saying why.
func (s *server) standby(ctx context.Context, since time.Time) {
	st, err := openStore(ctx, s.rt, s.env, readStore, postgres.Options{})
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn("the standby serves no API: open the store read-only", "error", err.Error())
		}
		return
	}
	defer func() { _ = st.Close() }()
	var dep api.Deployment
	if e, ok := st.(engines); ok {
		dep = pgDeployment{e}
	}
	var nodes http.Handler
	if s.nodeToken != "" {
		nodes = nodeapi.NewStandby(s.nodeToken, func(ctx context.Context) (string, string) {
			if dep == nil {
				return "", ""
			}
			es, err := dep.Engines(ctx)
			if err != nil || len(es) == 0 || es[0].State != "active" {
				return "", ""
			}
			return es[0].ID, es[0].Address
		})
	}
	a, err := api.New(api.Options{Store: st, Artifacts: s.arts, Version: s.rt.Version,
		User: identity(s.rt), Auth: s.sec.auth, Logger: s.log, Identity: api.Identity{ID: s.id, Since: since},
		Deployment: dep, Standby: true, NodeAPI: nodes, LeaseTTL: s.cmd.LeaseTTL})
	if err != nil {
		s.log.Error("the standby serves no API", "error", err.Error())
		return
	}
	s.site.set(ctx, api.Handler(a, ui.Handler()))
	<-ctx.Done()
}

// term runs one term of the engine on st, until ctx ends or the store loses its write connection
// (errSteppedDown).
func (s *server) term(ctx context.Context, st store.Store, lead leadership) error {
	s.log.Info("leading the deployment", "engine", s.id, "epoch", lead.identity.Epoch, "listen", s.site.url,
		"embeddedNode", s.env.NodeEmbedded, "mode", s.env.EffectiveMode(pipeline.ModeProduction))
	rt, env := s.rt, s.env
	notes := notifier(rt, env, s.log)
	e, err := engine.New(engine.Options{
		Store: st, Logger: s.log, LeaseTTL: s.cmd.LeaseTTL, Env: rt.Env, Secrets: secrets(rt, env),
		Artifacts: s.arts, Resolver: resolver(rt, env, false), Agents: agents(rt, env),
		Notifier: notes, PackageDir: packageDir(rt, env), Check: checkOptions(rt, env),
		Leadership: engine.Leadership{ID: s.id, Epoch: lead.identity.Epoch}, Takeover: true, Retention: env.Retention,
	})
	if err != nil {
		return err
	}
	// The engine steps down at once when the store loses its write connection, and the leadership lock with it.
	var wait func(context.Context) error
	if l, ok := st.(interface {
		Lost() <-chan struct{}
		LostErr() error
	}); ok {
		wait = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return nil
			case <-l.Lost():
				s.log.Warn("the write connection is lost, and the leadership lock with it: stepping down", "engine",
					s.id, "epoch", lead.identity.Epoch, "error", l.LostErr().Error())
				return fmt.Errorf("%w: %w", errSteppedDown, l.LostErr())
			}
		}
	}
	h := engineHost{site: s.site, sec: s.sec, embedded: env.NodeEmbedded, slots: s.cmd.Slots, leaseTTL: s.cmd.LeaseTTL,
		lead: lead, arts: s.arts, nodeToken: s.nodeToken}
	return h.run(ctx, rt, env, st, e, notes, s.log, wait)
}

// engines is a Postgres store's view of the deployment's engines.
type engines interface {
	Engines(ctx context.Context) ([]postgres.EngineStatus, error)
}

// pgDeployment is the deployment's engines as the database sees them, for the API.
type pgDeployment struct{ st engines }

func (d pgDeployment) Engines(ctx context.Context) ([]api.DeploymentEngine, error) {
	es, err := d.st.Engines(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]api.DeploymentEngine, 0, len(es))
	for _, e := range es {
		de := api.DeploymentEngine{ID: e.ID, State: "standby", Version: e.Version, Address: e.Address, Epoch: e.Epoch}
		if e.Active {
			de.State = "active"
		}
		if !e.Since.IsZero() {
			since := e.Since
			de.Since = &since
		}
		out = append(out, de)
	}
	return out, nil
}

// newEngineID names this process's engine: its host name and a random suffix, so two engines on one host, or one
// restarted, never share an id.
func newEngineID() string {
	host, _ := os.Hostname()
	host = strings.Map(func(r rune) rune {
		if r < 0x80 && (unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '-' || r == '_') {
			return r
		}
		return '-'
	}, host)
	if len(host) > 24 {
		host = host[:24]
	}
	return cmp.Or(host, "engine") + "-" + strings.ToLower(rand.Text()[:6])
}

// readNodeToken reads node.token, empty without it: an engine serves remote nodes only with it (spec section 15.7).
func readNodeToken(rt *Runtime, env *pipeline.Environment) (string, error) {
	if env.NodeToken == nil {
		return "", nil
	}
	return env.NodeToken.Token("node.token", rt.Env)
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
