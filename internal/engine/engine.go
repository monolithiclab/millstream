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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

// Defaults for zero Options.
const (
	DefaultLeaseTTL        = 60 * time.Second
	DefaultTickInterval    = time.Second
	DefaultProjectInterval = 15 * time.Second
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
	// ProjectInterval paces the projectors (Project); zero means DefaultProjectInterval.
	ProjectInterval time.Duration
	// Env looks up environment variables: variables (MILLSTREAM_VAR_*), secrets (MILLSTREAM_SECRET_*) and the
	// PATH, HOME and TMPDIR action processes receive. Nil means an empty environment.
	Env func(string) (string, bool)
	// Secrets resolves each pipeline's secrets: the environment file's grant (pipeline.Environment.SecretLookup);
	// nil reads MILLSTREAM_SECRET_<NAME> through Env.
	Secrets func(pipelineName string) pipeline.SecretLookup
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
	// Artifacts is the artifact store nodes move artifacts through; nil fails every delivery that has any.
	Artifacts *artifacts.Store
	// Agents says how agent steps run.
	Agents Agents
	// Notifier posts the notifications of review steps with a notify webhook (spec section 8); nil sends none.
	Notifier *review.Notifier
	// PackageDir is where the engine writes the packages it loads from the store's bundles, one directory per
	// content hash: an engine run from the store (serve) loads each revision's package so. Empty loads none: a
	// revision runs only if Apply gave the engine its package.
	PackageDir string
	// Check is what ApplyPackages checks packages with; its Secrets default to Secrets.
	Check pipeline.CheckOptions
}

// Agents says how the engine runs the agent adapter of agent steps (spec section 7.3): in a container sandbox when
// one is configured, else as a child process only when the environment file allows it.
type Agents struct {
	// Sandbox runs the adapter in a container; nil when no container runtime, agent image or Linux build of
	// millstream is available.
	Sandbox *AgentSandbox
	// AllowUnsandboxed lets the adapter run without a sandbox, as a child process of the node running Executable
	// (agents.allowUnsandboxed); every such start is logged.
	AllowUnsandboxed bool
	// Executable is the millstream binary whose agent subcommand is the adapter, outside a sandbox.
	Executable string
	// Prices are the environment file's model prices, handed to the adapter for the model it runs.
	Prices map[string]pipeline.Price
}

// AgentSandbox is the container the adapter runs in: Image (agents.image), with Binary, a Linux build of millstream,
// mounted read-only at AgentBinaryPath, and the package mounted read-only at its own path.
type AgentSandbox struct {
	Image  string
	Binary string
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
	lessonsC map[actionKey]lessonsEntry     // agent steps' lessons by package hash and step
	inflight map[store.DeliveryKey]flight   // leased deliveries
	buckets  map[store.StepRef]*bucket      // step rate limits
	builtins map[builtinKey]*builtin        // compiled builtin steps by package hash and step
	// changed, made when WaitSettled first asks for it, is closed (and forgotten) when a lease in flight ends, the
	// moments a run may settle.
	changed chan struct{}

	crons map[[2]string]*cronState // cron triggers by pipeline and name; the scheduler's goroutine only

	ready     chan struct{} // closed once Run has rebuilt the leases in flight
	readyOnce sync.Once

	projMu       sync.Mutex           // serialises Project
	lastInsights map[string]time.Time // when each pipeline's insights last ran; under projMu

	// Test seams, set before Run: maxDue bounds the missed cron times kept (the maxDue constant), and ticked and
	// projected, when set, are called after each pass of Run's expiry tick and of its projectors.
	maxDue    int
	ticked    func()
	projected func()
}

// revision is a decoded revision.
type revision struct {
	id        store.RevisionID
	hash      string
	appliedAt time.Time
	p         *pipeline.Pipeline
	loaded    atomic.Bool // its package is in the engine's packages: loadPackage has nothing to do
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
	if opts.ProjectInterval <= 0 {
		opts.ProjectInterval = DefaultProjectInterval
	}
	if opts.Env == nil {
		opts.Env = func(string) (string, bool) { return "", false }
	}
	if opts.Resolver == nil {
		opts.Resolver = pipeline.Resolver{}
	}
	e := &Engine{}
	if opts.Check.Secrets == nil {
		opts.Check.Secrets = e.secrets // resolved when called: tests replace Env after New
	}

	*e = Engine{
		st: opts.Store, opts: opts, log: opts.Logger,
		packages: map[string]*pipeline.Package{}, revs: map[store.RevisionID]*revision{},
		actions:  map[actionKey]*resolvedAction{},
		lessonsC: map[actionKey]lessonsEntry{}, inflight: map[store.DeliveryKey]flight{}, buckets: map[store.StepRef]*bucket{},
		builtins: map[builtinKey]*builtin{}, maxDue: maxDue,
		crons: map[[2]string]*cronState{}, ready: make(chan struct{}), lastInsights: map[string]time.Time{},
		parser: cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
	}
	return e, nil
}

// secrets returns how a pipeline's secrets resolve: Options.Secrets, else MILLSTREAM_SECRET_<NAME> through Env.
func (e *Engine) secrets(pipelineName string) pipeline.SecretLookup {
	if e.opts.Secrets != nil {
		return e.opts.Secrets(pipelineName)
	}
	return pipeline.EnvSecrets(e.opts.Env)(pipelineName)
}

// LeaseTTL is the lease duration the engine leases with; nodes heartbeat well within it.
func (e *Engine) LeaseTTL() time.Duration { return e.opts.LeaseTTL }

// Apply stores the package as a revision of its pipeline, made active, and keeps the package so its deliveries can
// run. The bundle stored with the revision is the package's files as a JSON object of base64 contents
// (EncodeBundle). It does not check the package: ApplyPackages does.
func (e *Engine) Apply(ctx context.Context, pkg *pipeline.Package) (store.RevisionID, error) {
	return e.apply(ctx, pkg, "")
}

func (e *Engine) apply(ctx context.Context, pkg *pipeline.Package, message string) (store.RevisionID, error) {
	bundle, err := EncodeBundle(pkg.Files)
	if err != nil {
		return 0, fmt.Errorf("apply %s: encode bundle: %w", pkg.Pipeline.Name, err)
	}
	id, err := e.st.ApplyRevision(ctx, store.PackageRecord{Hash: pkg.Hash, Bundle: bundle, Pipeline: pkg.Pipeline,
		Message: message})
	if err != nil {
		return 0, fmt.Errorf("apply %s: %w", pkg.Pipeline.Name, err)
	}
	e.mu.Lock()
	e.packages[pkg.Hash] = pkg
	e.mu.Unlock()
	e.log.Info("revision applied", "pipeline", pkg.Pipeline.Name, "revision", int64(id), "packageHash", pkg.Hash)
	return id, nil
}

// Run rebuilds the leases in flight from the store and times out the overdue reviews, closes Ready, then expires
// leases, times out reviews and runs the cron scheduler every tick, and the projectors every ProjectInterval, until
// ctx is cancelled. It returns nil then, once the
// projectors stopped. Nodes start pulling once Ready is closed, so the rebuild never races their leases.
func (e *Engine) Run(ctx context.Context) error {
	// The review notifications outlive ctx: queued and in-flight ones get a grace period once the engine stops.
	if e.opts.Notifier != nil {
		defer e.opts.Notifier.Shutdown(review.ShutdownGrace)
	}
	if err := e.resync(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	// Reviews overdue while no engine ran time out before anything waits on their runs.
	e.reviewTimeouts(ctx)
	e.readyOnce.Do(func() { close(e.ready) })
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() { e.projectLoop(ctx) })
	tick := time.NewTicker(e.opts.TickInterval)
	defer tick.Stop()
	for first := true; ; first = false {
		e.expire(ctx)
		if !first {
			e.reviewTimeouts(ctx)
		}
		if !e.opts.NoCron {
			e.schedule(ctx)
		}
		if e.ticked != nil {
			e.ticked()
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
	inflight := make(map[store.DeliveryKey]flight, len(ds))
	for _, d := range ds {
		inflight[store.DeliveryKey{Pipeline: d.Pipeline, ID: d.ID}] = flight{
			step: store.StepRef{Pipeline: d.Pipeline, Step: d.Step}, runID: d.RunID, itemID: d.ItemID,
			attempt: d.Attempt, token: d.FencingToken,
		}
	}
	e.inflight = inflight
	return nil
}

// Settled reports whether every run named is settled or waiting only on reviews (waitingReview): nothing moves
// them on without a person, or a review timeout.
func (e *Engine) Settled(ctx context.Context, runs []RunRef) (bool, error) {
	for _, r := range runs {
		run, err := e.st.Run(ctx, r.Pipeline, r.ID)
		if err != nil {
			return false, fmt.Errorf("settled: %w", err)
		}
		if run.SettledAt.IsZero() && run.State != store.RunWaitingReview {
			return false, nil
		}
	}
	return true, nil
}

// WaitSettled waits until every run named is settled, or waiting only on reviews, or ctx ends. It checks again whenever a lease in flight ends,
// and polls for what settles a run elsewhere (another engine's nodes, a cancel).
func (e *Engine) WaitSettled(ctx context.Context, runs []RunRef) error {
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		changed := e.changes()
		ok, err := e.Settled(ctx, runs)
		if err != nil || ok {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for runs to settle: %w", ctx.Err())
		case <-changed:
		case <-poll.C:
		}
	}
}

// changes returns the channel closed when a lease in flight next ends.
func (e *Engine) changes() <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.changed == nil {
		e.changed = make(chan struct{})
	}
	return e.changed
}

// revision returns the decoded revision id, from the cache or the store.
func (e *Engine) revision(ctx context.Context, id store.RevisionID) (*revision, error) {
	e.mu.Lock()
	r := e.revs[id]
	e.mu.Unlock()
	if r != nil {
		e.loadPackage(ctx, r)
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
	e.loadPackage(ctx, r)
	return r, nil
}

// loadPackage loads a revision's package from the store when the engine has a PackageDir and not the package yet.
// A failure is logged: the revision's deliveries fail as retryable until a later attempt loads it.
func (e *Engine) loadPackage(ctx context.Context, r *revision) {
	if e.opts.PackageDir == "" || r.loaded.Load() {
		return
	}
	if _, err := e.pkg(ctx, r.hash); err != nil {
		if ctx.Err() == nil {
			e.log.Error("load package", "revision", int64(r.id), "packageHash", r.hash, "error", err.Error())
		}
		return
	}
	r.loaded.Store(true)
}
