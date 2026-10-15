// Package engine is the active engine (spec sections 4.6 and 5.9): it applies packages as revisions, fires runs from
// cron and manual triggers with their variables resolved and stored at run start, starts replay runs (spec section
// 5.7), runs the filter and dedupe builtins itself, and serves nodes as their Transport, leasing deliveries through
// the store and resolving each into the action process to run.
//
// Coordination is in memory, as the spec asks: step concurrency and pipeline limits are counted from the leases the
// engine handed out, and step rates are token buckets. The leases in flight are rebuilt from the store's leased
// deliveries once, when Run starts (Ready is closed after that), so those of a previous engine count until they
// expire; afterwards an entry leaves only when its own lease ends, matched by fencing token: a report the store
// accepted or refused as stale, a lease the heartbeat lost, or one ExpireLeases ended. Pause state and the round-robin across pipelines belong to the store's Lease unit, which
// receives the engine's counts as slots and exclusions. All of the engine's mutable state is guarded by one mutex.
//
// Packages are the engine's to hold: a delivery runs the action of its run's revision, so it needs that revision's
// package loaded in this process (Apply). A run pinned to a revision whose package is not loaded fails its
// deliveries as retryable until it is, rather than running another revision's code.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

// Defaults for zero Options.
const (
	DefaultLeaseTTL     = 60 * time.Second
	DefaultTickInterval = time.Second
)

// Options configure an Engine. Store is required.
type Options struct {
	Store store.Store
	// Now is the engine's clock (cron schedules, rate buckets); nil means time.Now. Lease expiry uses the store's
	// clock, never this one.
	Now func() time.Time
	// Logger receives the engine's logs; nil discards them.
	Logger *slog.Logger
	// LeaseTTL is the lease duration; zero means DefaultLeaseTTL.
	LeaseTTL time.Duration
	// TickInterval paces lease expiry and the cron scheduler; zero means DefaultTickInterval.
	TickInterval time.Duration
	// Env looks up environment variables: variables (MILLSTREAM_VAR_*), secrets (MILLSTREAM_SECRET_*) and the
	// PATH, HOME and TMPDIR action processes receive. Nil means an empty environment.
	Env func(string) (string, bool)
	// Flags are --var values. A flag naming a variable a pipeline does not declare is ignored for that pipeline:
	// with several packages, callers check each flag against all of them.
	Flags map[string]string
	// NoCron disables the cron scheduler: the engine fires only what Fire is asked to (run --once).
	NoCron bool
	// Stubs complete the deliveries of the steps they name with fixed outcomes instead of running the action
	// (millstream test pipeline).
	Stubs map[store.StepRef]Stub
	// Resolver resolves action references; nil means pipeline.Resolver{}.
	Resolver pipeline.ActionResolver
}

// Stub is the fixed outcome of a stubbed step: its outputs, or an action error when Err is set.
type Stub struct {
	Outputs []store.Output
	Err     *exec.ActionError
}

// RunRef names a run.
type RunRef struct {
	Pipeline string
	ID       string
}

// Engine is the active engine. It implements node.Transport.
type Engine struct {
	st   store.Store
	opts Options
	log  *slog.Logger

	parser cron.Parser // standard five fields and descriptors, with a CRON_TZ= prefix for the zone

	mu       sync.Mutex
	packages map[string]*pipeline.Package   // by package hash
	revs     map[store.RevisionID]*revision // decoded revisions, immutable once built
	actions  map[actionKey]*resolvedAction  // by package hash and reference
	inflight map[string]flight              // leased deliveries by delivery id
	buckets  map[store.StepRef]*bucket      // step rate limits
	builtins map[builtinKey]*builtin        // compiled builtin steps by package hash and step

	crons map[[2]string]*cronState // cron triggers by pipeline and name; the scheduler's goroutine only

	ready     chan struct{} // closed once Run has rebuilt the leases in flight
	readyOnce sync.Once
}

// revision is a decoded revision.
type revision struct {
	id        store.RevisionID
	hash      string
	appliedAt time.Time
	p         *pipeline.Pipeline
}

// flight is a delivery leased through the engine and not yet reported, under its fencing token.
type flight struct {
	step    store.StepRef
	runID   string
	itemID  string
	attempt int
	token   int64
	redact  *redact.Redactor // the secrets resolved for its dispatch; nil until then
}

// New returns an engine over opts.Store.
func New(opts Options) (*Engine, error) {
	if opts.Store == nil {
		return nil, errors.New("engine: no store")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = DefaultLeaseTTL
	}
	if opts.TickInterval <= 0 {
		opts.TickInterval = DefaultTickInterval
	}
	if opts.Env == nil {
		opts.Env = func(string) (string, bool) { return "", false }
	}
	if opts.Resolver == nil {
		opts.Resolver = pipeline.Resolver{}
	}
	return &Engine{
		st: opts.Store, opts: opts, log: opts.Logger,
		packages: map[string]*pipeline.Package{}, revs: map[store.RevisionID]*revision{},
		actions: map[actionKey]*resolvedAction{}, inflight: map[string]flight{}, buckets: map[store.StepRef]*bucket{},
		builtins: map[builtinKey]*builtin{},
		crons:    map[[2]string]*cronState{}, ready: make(chan struct{}),
		parser: cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
	}, nil
}

// LeaseTTL is the lease duration the engine leases with; nodes heartbeat well within it.
func (e *Engine) LeaseTTL() time.Duration { return e.opts.LeaseTTL }

// Apply stores the package as a revision of its pipeline, made active, and keeps the package so its deliveries can
// run. The bundle stored with the revision is the package's files as a JSON object of base64 contents.
func (e *Engine) Apply(ctx context.Context, pkg *pipeline.Package) (store.RevisionID, error) {
	bundle, err := json.Marshal(pkg.Files)
	if err != nil {
		return 0, fmt.Errorf("apply %s: encode bundle: %w", pkg.Pipeline.Name, err)
	}
	id, err := e.st.ApplyRevision(ctx, store.PackageRecord{Hash: pkg.Hash, Bundle: bundle, Pipeline: pkg.Pipeline})
	if err != nil {
		return 0, fmt.Errorf("apply %s: %w", pkg.Pipeline.Name, err)
	}
	e.mu.Lock()
	e.packages[pkg.Hash] = pkg
	e.mu.Unlock()
	e.log.Info("revision applied", "pipeline", pkg.Pipeline.Name, "revision", int64(id), "packageHash", pkg.Hash)
	return id, nil
}

// Run rebuilds the leases in flight from the store, closes Ready, then expires leases and runs the cron scheduler
// every tick until ctx is cancelled. It returns nil then. Nodes start pulling once Ready is closed, so the rebuild
// never races their leases.
func (e *Engine) Run(ctx context.Context) error {
	if err := e.resync(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	e.readyOnce.Do(func() { close(e.ready) })
	tick := time.NewTicker(e.opts.TickInterval)
	defer tick.Stop()
	for {
		e.expire(ctx)
		if !e.opts.NoCron {
			e.schedule(ctx)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// Ready is closed once Run has rebuilt the leases in flight: a node pulling from the engine starts after it.
func (e *Engine) Ready() <-chan struct{} { return e.ready }

// expire ends expired leases in the store and forgets exactly those it ended.
func (e *Engine) expire(ctx context.Context) {
	refs, err := e.st.ExpireLeases(ctx)
	if err != nil {
		if ctx.Err() == nil {
			e.log.Error("expire leases", "error", err.Error())
		}
		return
	}
	for _, ref := range refs {
		f, _ := e.release(ref)
		e.flightLog(ref, f).Warn("lease expired")
	}
}

// resync rebuilds the deliveries in flight from the store's leased deliveries, at start: those of a previous engine
// that died count until their leases expire. It holds e.mu from the read to the swap, so a lease taken or released
// meanwhile is ordered entirely before or after it.
func (e *Engine) resync(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	ds, err := e.st.Deliveries(ctx, store.DeliveryFilter{State: store.DeliveryLeased})
	if err != nil {
		return fmt.Errorf("engine: read leased deliveries: %w", err)
	}
	inflight := make(map[string]flight, len(ds))
	for _, d := range ds {
		inflight[d.ID] = flight{
			step: store.StepRef{Pipeline: d.Pipeline, Step: d.Step}, runID: d.RunID, itemID: d.ItemID,
			attempt: d.Attempt, token: d.FencingToken,
		}
	}
	e.inflight = inflight
	return nil
}

// Settled reports whether every run named is settled.
func (e *Engine) Settled(ctx context.Context, runs []RunRef) (bool, error) {
	for _, r := range runs {
		run, err := e.st.Run(ctx, r.Pipeline, r.ID)
		if err != nil {
			return false, fmt.Errorf("settled: %w", err)
		}
		if run.SettledAt.IsZero() {
			return false, nil
		}
	}
	return true, nil
}

// WaitSettled polls until every run named is settled or ctx ends.
func (e *Engine) WaitSettled(ctx context.Context, runs []RunRef) error {
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		ok, err := e.Settled(ctx, runs)
		if err != nil || ok {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for runs to settle: %w", ctx.Err())
		case <-poll.C:
		}
	}
}

// revision returns the decoded revision id, from the cache or the store.
func (e *Engine) revision(ctx context.Context, id store.RevisionID) (*revision, error) {
	e.mu.Lock()
	r := e.revs[id]
	e.mu.Unlock()
	if r != nil {
		return r, nil
	}
	rec, err := e.st.Revision(ctx, id)
	if err != nil {
		return nil, err
	}
	p, err := rec.Decode()
	if err != nil {
		return nil, err
	}
	r = &revision{id: id, hash: rec.PackageHash, appliedAt: rec.AppliedAt, p: p}
	e.mu.Lock()
	e.revs[id] = r
	e.mu.Unlock()
	return r, nil
}
