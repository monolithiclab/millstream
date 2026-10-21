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
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/schema"
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
	// Stubber decides each leased delivery before Stubs and the builtins do: ok true completes it with the stub,
	// false lets it run as it would (a backtest's replay, package backtest).
	Stubber func(ctx context.Context, l store.Lease) (s Stub, ok bool)
	// Resolver resolves action references; nil means pipeline.Resolver{}.
	Resolver pipeline.ActionResolver
	// Artifacts is the artifact store nodes move artifacts through; nil fails every delivery that has any.
	Artifacts artifacts.Store
	// ArtifactFiles is the artifact store as the artifact sweep sees it (spec section 9.3); nil means Artifacts, when
	// it implements artifacts.Files (the local directory and the S3-compatible store do).
	ArtifactFiles artifacts.Files
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
	// Leadership is who this engine is on a deployment with a standby and the epoch it leads in (spec section
	// 5.9), what nodes following the active engine across a failover learn it by; zero on SQLite.
	Leadership Leadership
	// Retention is the environment file's retention key: how long events, transcripts and rollups are kept (zero
	// for ever), and how often the retention job runs (Every; zero means DefaultRetentionInterval). Streams carry
	// their own retention in their pipelines (spec section 9.3).
	Retention pipeline.Retention
	// Improve is the environment file's improve key: the default minSeverity of insight triggers and the settings
	// the insights projector reads (spec sections 11.1, 11.5 and 11.6).
	Improve pipeline.Improve
	// Proposals are what proposals are validated with (spec section 11.3), and the revisions carrying proposals in
	// a combination nobody validated.
	Proposals ProposalOptions
	// Mode is the deployment's environment (spec section 11.2): production refuses ApplyProposal. Empty reads
	// development.
	Mode string
	// Takeover makes every step's rate start empty, a full token's interval before its first lease, so that an
	// engine taking a deployment over (serve) never causes a burst (spec section 5.9).
	Takeover bool
}

// Leadership is an engine's identity and the epoch of its leadership: one more at each acquisition of the
// deployment's lock.
type Leadership struct {
	ID    string
	Epoch int64
}

// Agents says how the engine's embedded node runs the agent adapter of agent steps (node.Agents), and the model prices
// the adapter is handed, which remote nodes receive from the engine too.
type Agents struct {
	node.Agents
	// Prices are the environment file's model prices, handed to the adapter for the model it runs.
	Prices map[string]pipeline.Price
}

// AgentSandbox is the container the adapter runs in (node.AgentSandbox).
type AgentSandbox = node.AgentSandbox

// Stub is the fixed outcome of a stubbed step: its outputs, an action error when Err is set, or the delivery
// filtered for that reason when Filter is set.
type Stub struct {
	Outputs []store.Output
	Err     *exec.ActionError
	Filter  string
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
	buckets  map[store.StepRef]*Bucket      // step rate limits
	nodes    map[string]*nodeEntry          // the nodes registered in this term, by id
	builtins map[builtinKey]*builtin        // compiled builtin steps by package hash and step
	// triggerSchemas are the triggers' compiled input schemas (nil for none), by package hash and trigger stream;
	// hookBuckets the webhook triggers' rates, by pipeline and trigger stream.
	triggerSchemas map[actionKey]*schema.Schema
	hookBuckets    map[store.StepRef]*Bucket
	// changed, made when WaitSettled first asks for it, is closed (and forgotten) when a lease in flight ends, the
	// moments a run may settle.
	changed chan struct{}

	crons map[[2]string]*cronState // cron triggers by pipeline and name; the scheduler's goroutine only

	ready     chan struct{} // closed once Run has rebuilt the leases in flight
	readyOnce sync.Once
	leased    atomic.Bool // set at the engine's first lease, which it logs: when a takeover starts dispatching

	projMu       sync.Mutex           // serialises Project
	lastInsights map[string]time.Time // when each pipeline's insights last ran; under projMu
	insightSeq   int64                // the last insight event the insight triggers fired for; under projMu
	insightTS    time.Time            // and when it was recorded
	retainMu     sync.Mutex           // serialises Retain
	bg           background           // the checks of revisions carrying unvalidated proposals, while Run runs
	regressionMu sync.Mutex           // serialises the regression insights those checks raise

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
	owner   string           // the node holding the lease
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
	if opts.Retention.Every <= 0 {
		opts.Retention.Every = pipeline.Duration(DefaultRetentionInterval)
	}
	if opts.Env == nil {
		opts.Env = func(string) (string, bool) { return "", false }
	}
	if opts.Resolver == nil {
		opts.Resolver = pipeline.Resolver{}
	}
	if opts.PackageDir != "" {
		// Action processes start from their package's directory: a relative path would resolve from theirs.
		dir, err := filepath.Abs(opts.PackageDir)
		if err != nil {
			return nil, fmt.Errorf("engine: package directory: %w", err)
		}
		opts.PackageDir = dir
	}
	e := &Engine{}
	if opts.Check.Secrets == nil {
		opts.Check.Secrets = e.secrets // resolved when called: tests replace Env after New
	}

	*e = Engine{
		st: opts.Store, opts: opts, log: opts.Logger,
		packages: map[string]*pipeline.Package{}, revs: map[store.RevisionID]*revision{},
		actions:  map[actionKey]*resolvedAction{},
		lessonsC: map[actionKey]lessonsEntry{}, inflight: map[store.DeliveryKey]flight{}, buckets: map[store.StepRef]*Bucket{},
		nodes: map[string]*nodeEntry{}, builtins: map[builtinKey]*builtin{}, triggerSchemas: map[actionKey]*schema.Schema{},
		hookBuckets: map[store.StepRef]*Bucket{}, maxDue: maxDue,
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

// Leadership returns Options.Leadership.
func (e *Engine) Leadership() Leadership { return e.opts.Leadership }

// LeaseTTL is the lease duration the engine leases with; nodes heartbeat well within it.
func (e *Engine) LeaseTTL() time.Duration { return e.opts.LeaseTTL }

// Apply stores the package as a revision of its pipeline, made active, and keeps the package so its deliveries can
// run. The bundle stored with the revision is the package's files as a JSON object of base64 contents
// (EncodeBundle). It does not check the package: ApplyPackages does. A pipeline that imports streams has its
// variables resolved now, as a firing would resolve them, for the runs its imports open inside producers' Done units
// (spec section 4.12); they must resolve, secrets included. Each webhook trigger without a token gets one, returned
// once in Applied.Tokens.
func (e *Engine) Apply(ctx context.Context, pkg *pipeline.Package) (Applied, error) {
	return e.apply(ctx, pkg, ApplyOptions{})
}

func (e *Engine) apply(ctx context.Context, pkg *pipeline.Package, opts ApplyOptions) (Applied, error) {
	name := pkg.Pipeline.Name
	fail := func(err error) (Applied, error) { return Applied{}, fmt.Errorf("apply %s: %w", name, err) }
	bundle, err := pipeline.EncodeBundle(pkg.Files)
	if err != nil {
		return fail(fmt.Errorf("encode bundle: %w", err))
	}
	rec := store.PackageRecord{Hash: pkg.Hash, Bundle: bundle, Pipeline: pkg.Pipeline, Message: opts.Message,
		Base: opts.Base}
	if len(pkg.Pipeline.Imports()) > 0 {
		res, err := pipeline.ResolveVarsWith(pkg.Pipeline, e.declaredFlags(pkg.Pipeline), e.opts.Env, e.secrets(name))
		if err != nil {
			return fail(fmt.Errorf("the runs its imported streams open: %w: %w", ErrFiringConfig, err))
		}
		rec.Vars, rec.VarSources = res.Vars, res.Sources
	}
	before, err := e.st.ActiveRevision(ctx, name)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return fail(err)
	}
	parent := ""
	if ids := opts.Proposals[name]; len(ids) > 0 && before != 0 {
		rev, err := e.st.Revision(ctx, before)
		if err != nil {
			return fail(err)
		}
		parent = rev.PackageHash
		rec.Proposals = e.carried(ctx, name, parent, pkg.Hash, ids)
	}
	id, err := e.st.ApplyRevision(ctx, rec)
	if err != nil {
		return fail(err)
	}
	e.mu.Lock()
	e.packages[pkg.Hash] = pkg
	e.mu.Unlock()
	e.log.Info("revision applied", "pipeline", name, "revision", int64(id), "packageHash", pkg.Hash)
	// Stored after the revision: an apply interrupted in between leaves the trigger without a token, answering every
	// request unauthorized, until the same apply runs again or the token is rotated.
	tokens, err := e.createTriggerTokens(ctx, pkg.Pipeline)
	if err != nil {
		return fail(err)
	}
	if id != before {
		e.checkCombination(ctx, name, id, parent, pkg.Hash, rec.Proposals)
	}
	return Applied{Pipeline: name, Revision: id, PackageHash: pkg.Hash, Created: id != before, Tokens: tokens}, nil
}

// Run rebuilds the leases in flight from the store and times out the overdue reviews, closes Ready, then expires
// leases, times out reviews and runs the cron scheduler every tick, the projectors every ProjectInterval and the
// retention job every retention.every (resuming the purges not done first), until ctx is cancelled. It returns nil
// then, once the projectors and the retention job stopped. Nodes start pulling once Ready is closed, so the rebuild never races their leases.
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
	e.resetValidations(ctx)
	e.readyOnce.Do(func() { close(e.ready) })
	e.bg.start(ctx)
	defer e.bg.stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() { e.projectLoop(ctx) })
	wg.Go(func() { e.retentionLoop(ctx) })
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
			attempt: d.Attempt, token: d.FencingToken, owner: d.LeaseOwner,
		}
	}
	// Leased audits hold their step's slots too (spec section 11.5).
	as, err := e.st.Audits(ctx, store.AuditFilter{State: store.AuditLeased})
	if err != nil {
		return fmt.Errorf("engine: read leased audits: %w", err)
	}
	for _, a := range as {
		inflight[store.DeliveryKey{Pipeline: a.Pipeline, ID: a.ID}] = flight{
			step: store.StepRef{Pipeline: a.Pipeline, Step: a.Step}, runID: a.RunID, itemID: a.ItemID, attempt: 1,
			token: a.FencingToken, owner: a.LeaseOwner,
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

// WaitSettled waits until every run named is settled, or waiting only on reviews, and the audits of their deliveries
// ended (an audit never holds its run open, spec section 11.5, but run --once leaves none behind), or ctx ends. It
// checks again whenever a lease in flight ends, and polls for what settles a run elsewhere (another engine's nodes, a
// cancel).
func (e *Engine) WaitSettled(ctx context.Context, runs []RunRef) error {
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		changed := e.changes()
		ok, err := e.Settled(ctx, runs)
		if err == nil && ok {
			ok, err = e.auditsEnded(ctx, runs)
		}
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

// auditsEnded reports whether every audit of the runs named ended: none pending or leased.
func (e *Engine) auditsEnded(ctx context.Context, runs []RunRef) (bool, error) {
	for _, r := range runs {
		as, err := e.st.Audits(ctx, store.AuditFilter{Pipeline: r.Pipeline, RunID: r.ID})
		if err != nil {
			return false, fmt.Errorf("audits: %w", err)
		}
		if slices.ContainsFunc(as, func(a store.Audit) bool {
			return a.State == store.AuditPending || a.State == store.AuditLeased
		}) {
			return false, nil
		}
	}
	return true, nil
}

// Changes returns a channel closed when a lease in flight next ends: when new work may be due, which a remote
// node's long poll waits for.
func (e *Engine) Changes() <-chan struct{} { return e.changes() }

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
