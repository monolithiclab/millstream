// Package node is the embedded node (spec section 5.10): it pulls leased deliveries from the engine through a
// Transport, runs each on the exec runtime's process pool in its own goroutine, moving its artifacts through the
// Transport's artifact store, renews every held lease with a heartbeat, and reports the outcome with the lease's
// fencing token, and the attempt's cost as its cost_usd metrics add up. A lease the engine reports lost cancels its
// delivery and drops its result; a report the store refuses as stale is dropped too, since another owner holds
// the delivery. On context cancellation the node drains: it stops pulling and lets in-flight deliveries finish, up
// to a drain timeout. Cancelled with ErrAbandon as the cause, it abandons them instead: its engine stepped down (spec
// section 5.9), so it cancels them at once and reports nothing; their leases expire, or the next active engine
// takes them back.
package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

// ClassAction is the error class of a delivery the action itself failed with an error message. Runtime failures
// carry exec's classes (timeout, crash, protocol).
const ClassAction = "action"

// Defaults for zero Options.
const (
	DefaultSlots             = 1
	DefaultPullInterval      = time.Second
	DefaultHeartbeatInterval = 5 * time.Second
	DefaultDrainTimeout      = 30 * time.Second
	// DefaultReportTimeout bounds each heartbeat, Done and Fail call.
	DefaultReportTimeout = 30 * time.Second
)

// HeartbeatFor is a node's heartbeat interval under leases of ttl: a third of the lease, at most
// DefaultHeartbeatInterval; DefaultHeartbeatInterval when ttl is not set.
func HeartbeatFor(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return DefaultHeartbeatInterval
	}
	return min(DefaultHeartbeatInterval, ttl/3)
}

// ErrAbandon, as the cause of Run's context cancellation (context.WithCancelCause), makes the node abandon its
// deliveries in flight rather than drain them.
var ErrAbandon = errors.New("abandon deliveries in flight")

// ErrDrainTimeout is returned, wrapped, by Run when in-flight deliveries were abandoned at the drain timeout. Their
// leases expire and the deliveries are retried elsewhere.
var ErrDrainTimeout = errors.New("drain timeout")

// Assignment is one leased delivery, resolved by the engine: the node only runs it.
type Assignment struct {
	Lease store.Lease
	// Launch is the action process as the engine resolved it, which a remote node receives and builds Spec from.
	Launch Launch
	// Secrets are the values of the secrets resolved for the delivery, by name.
	Secrets map[string]string
	// Spec is the action process to run, with the step's config interpolated for the run.
	Spec    exec.Spec
	ItemMsg protocol.Item
	// Timeout is the step's timeout; zero means none.
	Timeout time.Duration
	// Redact removes the values of the secrets resolved for the delivery (spec section 4.5) from its logs, the
	// action's stderr included, and from the error messages reported for it. Nil redacts nothing.
	Redact *redact.Redactor
}

// Artifacts is the artifact store as a node reaches it (spec section 5.10), for a delivery it holds: the engine's
// store for the embedded node; a remote node moves contents through signed URLs the engine issues only for the
// leases it holds (spec section 15.7).
type Artifacts interface {
	// PutArtifact stores an output artifact of the leased delivery's pipeline and returns its reference under name.
	PutArtifact(ctx context.Context, lease store.LeaseRef, name string, r io.Reader) (store.ArtifactRef, error)
	// OpenArtifact opens an input artifact of the leased delivery.
	OpenArtifact(ctx context.Context, lease store.LeaseRef, ref store.ArtifactRef) (io.ReadCloser, error)
}

// Transport connects a node to the engine: in memory for the embedded node, over HTTP/2 for remote ones.
type Transport interface {
	Artifacts
	// Pull leases up to max deliveries for owner; it returns at most max.
	Pull(ctx context.Context, owner string, max int) ([]Assignment, error)
	// Heartbeat renews owner's leases and returns those it no longer holds.
	Heartbeat(ctx context.Context, owner string, refs []store.LeaseRef) (lost []store.LeaseRef, err error)
	// Done and Fail report a delivery's outcome; they return an error wrapping store.ErrStaleLease when the
	// fencing token no longer holds the lease.
	Done(ctx context.Context, req store.DoneRequest) error
	Fail(ctx context.Context, req store.FailRequest) error
}

// Options configure a Node. Owner and Pool are required; zero durations and slots take the defaults.
type Options struct {
	// Owner is the node id the leases are taken under.
	Owner string
	// Slots is how many deliveries run at once.
	Slots             int
	PullInterval      time.Duration
	HeartbeatInterval time.Duration
	// Heartbeat, when set, gives the first interval when HeartbeatInterval is zero, and is read again after each
	// heartbeat for the next one: a remote node adopts the interval of the engine it registered with last (after a
	// failover, the new active engine's). Nil keeps HeartbeatInterval.
	Heartbeat    func() time.Duration
	DrainTimeout time.Duration
	// ReportTimeout bounds each heartbeat, Done and Fail call: a remote node's transport resends a Done or Fail
	// until the engine answers within it, across a reconnection.
	ReportTimeout time.Duration
	// IdleHeartbeat makes the node heartbeat while it holds no lease too: a remote node, whose heartbeats tell the
	// engine it is alive (spec section 5.10).
	IdleHeartbeat bool
	// Logger receives the node's logs; nil discards them.
	Logger *slog.Logger
	// Pool runs the action processes. The node does not close it.
	Pool *exec.Pool
}

// Node runs leased deliveries.
type Node struct {
	t    Transport
	opts Options
	log  *slog.Logger
	// newTimer makes the pull and heartbeat tickers (repeat) and the drain timer: real ones, which in-package tests
	// replace to drive them.
	newTimer func(d time.Duration, repeat bool) (c <-chan time.Time, stop func())
}

// realTimer is Node.newTimer outside tests.
func realTimer(d time.Duration, repeat bool) (<-chan time.Time, func()) {
	if repeat {
		t := time.NewTicker(d)
		return t.C, t.Stop
	}
	t := time.NewTimer(d)
	return t.C, func() { t.Stop() }
}

// New returns a node pulling from t.
func New(t Transport, opts Options) (*Node, error) {
	switch {
	case t == nil:
		return nil, errors.New("node: no transport")
	case opts.Owner == "":
		return nil, errors.New("node: no owner")
	case opts.Pool == nil:
		return nil, errors.New("node: no process pool")
	}
	if opts.Slots <= 0 {
		opts.Slots = DefaultSlots
	}
	if opts.PullInterval <= 0 {
		opts.PullInterval = DefaultPullInterval
	}
	if opts.HeartbeatInterval <= 0 && opts.Heartbeat != nil {
		opts.HeartbeatInterval = opts.Heartbeat()
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = DefaultDrainTimeout
	}
	if opts.ReportTimeout <= 0 {
		opts.ReportTimeout = DefaultReportTimeout
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	return &Node{t: t, opts: opts, log: opts.Logger.With("node", opts.Owner), newTimer: realTimer}, nil
}

// running is a delivery in flight.
type running struct {
	ref    store.LeaseRef
	log    *slog.Logger
	cancel context.CancelFunc
}

// loop is the state of one Run. Run's goroutine adds and removes held deliveries, which report back on finished;
// the heartbeat's goroutine reads them, under mu.
type loop struct {
	*Node
	base     context.Context // outlives Run's ctx, so in-flight work and its reports survive the drain
	mu       sync.Mutex
	held     map[store.DeliveryKey]*running
	finished chan store.DeliveryKey
}

// holding returns how many deliveries are held.
func (l *loop) holding() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.held)
}

// release forgets a delivery that finished.
func (l *loop) release(id store.DeliveryKey) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.held, id)
}

// cancelAll cancels every held delivery and returns how many there were.
func (l *loop) cancelAll() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.held {
		r.cancel()
	}
	return len(l.held)
}

// Run pulls and runs deliveries until ctx is cancelled, then drains. It returns nil after a clean drain. The
// heartbeat runs on its own goroutine, through the drain, so that a long pull never delays it.
func (n *Node) Run(ctx context.Context) error {
	l := &loop{Node: n, base: context.WithoutCancel(ctx), held: map[store.DeliveryKey]*running{},
		finished: make(chan store.DeliveryKey)}
	beatCtx, stopBeats := context.WithCancel(l.base)
	var beats sync.WaitGroup
	beats.Go(func() { l.beat(beatCtx) })
	defer func() {
		stopBeats()
		beats.Wait()
	}()
	pull, stopPull := n.newTimer(n.opts.PullInterval, true)
	defer stopPull()
	l.pull(ctx)
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-pull:
			l.pull(ctx)
		case id := <-l.finished:
			l.release(id)
			l.pull(ctx)
		}
	}
	if errors.Is(context.Cause(ctx), ErrAbandon) {
		l.abandon()
		return nil
	}
	return l.drain()
}

// beat heartbeats every interval until ctx ends, taking the interval anew after each one (Options.Heartbeat).
func (l *loop) beat(ctx context.Context) {
	every := l.opts.HeartbeatInterval
	tick, stop := l.newTimer(every, true)
	defer func() { stop() }()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			l.heartbeat()
		}
		if l.opts.Heartbeat == nil {
			continue
		}
		if next := l.opts.Heartbeat(); next > 0 && next != every {
			stop()
			every = next
			tick, stop = l.newTimer(every, true)
		}
	}
}

// abandon cancels every delivery in flight and waits for their goroutines, which drop their results.
func (l *loop) abandon() {
	n := l.cancelAll()
	if n == 0 {
		return
	}
	l.log.Warn("abandoning deliveries in flight", "inFlight", n)
	for l.holding() > 0 {
		l.release(<-l.finished)
	}
}

func (l *loop) pull(ctx context.Context) {
	free := l.opts.Slots - l.holding()
	if free <= 0 || ctx.Err() != nil {
		return
	}
	as, err := l.t.Pull(ctx, l.opts.Owner, free)
	if err != nil {
		if ctx.Err() == nil {
			l.log.Warn("pull deliveries", "error", err.Error())
		}
		return
	}
	for _, a := range as {
		l.start(a)
	}
}

func (l *loop) start(a Assignment) {
	ls := a.Lease
	log := a.Redact.Logger(l.log.With("pipeline", ls.Pipeline, "runId", ls.RunID, "step", ls.Step,
		"itemId", ls.Item.ID, "deliveryId", ls.DeliveryID, "attempt", ls.Attempt))
	ctx, cancel := context.WithCancel(l.base)
	ref := ls.Ref()
	l.mu.Lock()
	l.held[ref.Key()] = &running{ref: ref, log: log, cancel: cancel}
	l.mu.Unlock()
	log.Info("delivery started")
	go func() {
		defer cancel()
		l.execute(ctx, log, a)
		l.finished <- ref.Key()
	}()
}

// execute runs one delivery and reports it, unless its context was cancelled: the lease was lost or the drain
// timed out, and the delivery belongs to whoever leases it next.
func (l *loop) execute(ctx context.Context, log *slog.Logger, a Assignment) {
	res, err := l.opts.Pool.Deliver(ctx, a.Spec, exec.Delivery{
		Item: a.ItemMsg, Inputs: a.Lease.Item.Artifacts, Artifacts: leaseArtifacts{l.t, a.Lease.Ref()},
		Timeout: a.Timeout, Log: log,
	})
	if ctx.Err() != nil {
		log.Warn("delivery abandoned, result dropped")
		return
	}
	rctx, cancel := context.WithTimeout(l.base, l.opts.ReportTimeout)
	defer cancel()
	ref, attempt := a.Lease.Ref(), a.Lease.Attempt
	cost, metrics := protocol.CostUSD(res.Metrics), storeMetrics(log, a.Redact, res.Metrics)
	var rerr error
	switch {
	case err != nil:
		class, retryable := exec.ClassOf(err), !exec.Permanent(err)
		log.Warn("delivery failed in the runtime", "class", class, "retryable", retryable, "error", err.Error())
		rerr = l.t.Fail(rctx, store.FailRequest{
			LeaseRef: ref, Attempt: attempt, Class: class, Message: a.Redact.String(err.Error()),
			Retryable: retryable, Unreported: true,
		})
	case res.Err != nil:
		log.Info("delivery failed in the action", "retryable", res.Err.Retryable)
		rerr = l.t.Fail(rctx, store.FailRequest{
			LeaseRef: ref, Attempt: attempt, Class: ClassAction, Message: a.Redact.String(res.Err.Message),
			Retryable: res.Err.Retryable, CostUSD: cost, Artifacts: res.Artifacts, Metrics: metrics,
		})
	default:
		rerr = l.t.Done(rctx, store.DoneRequest{LeaseRef: ref, Attempt: attempt, Outputs: res.Outputs,
			CostUSD: cost, Artifacts: res.Artifacts, Metrics: metrics})
	}
	switch {
	case errors.Is(rerr, store.ErrStaleLease):
		log.Warn("stale lease, result dropped")
	case rerr != nil:
		log.Error("report delivery", "error", rerr.Error())
	}
}

// storeMetrics returns the metrics an attempt reports for the store: the action's metric messages, without the
// ones whose value is not a finite number or that have no name, and at most store.MaxAttemptMetrics, each dropped
// one logged. Names and labels are the action's text, so the delivery's secrets are redacted out of them, as out of
// its error messages.
func storeMetrics(log *slog.Logger, red *redact.Redactor, ms []protocol.Metric) []store.Metric {
	var out []store.Metric
	for _, m := range ms {
		switch {
		case m.Name == "" || math.IsNaN(m.Value) || math.IsInf(m.Value, 0):
			log.Warn("metric dropped: it needs a name and a finite value", "metric", m.Name)
		case len(out) == store.MaxAttemptMetrics:
			log.Warn("metric dropped: too many metrics for one attempt", "metric", m.Name,
				"max", store.MaxAttemptMetrics)
		default:
			labels := m.Labels
			if red != nil && labels != nil { // a copy: the action's map is left as it sent it
				labels = make(map[string]string, len(m.Labels))
				for k, v := range m.Labels {
					labels[red.String(k)] = red.String(v)
				}
			}
			out = append(out, store.Metric{Name: red.String(m.Name), Value: m.Value, Labels: labels})
		}
	}
	return out
}

// leaseArtifacts binds the transport's artifact store to one lease, as the runtime moves a delivery's files.
type leaseArtifacts struct {
	t     Artifacts
	lease store.LeaseRef
}

func (p leaseArtifacts) Open(ctx context.Context, ref store.ArtifactRef) (io.ReadCloser, error) {
	return p.t.OpenArtifact(ctx, p.lease, ref)
}

func (p leaseArtifacts) Put(ctx context.Context, name string, r io.Reader) (store.ArtifactRef, error) {
	return p.t.PutArtifact(ctx, p.lease, name, r)
}

// heartbeat renews every held lease and cancels the deliveries whose lease is lost.
func (l *loop) heartbeat() {
	l.mu.Lock()
	refs := make([]store.LeaseRef, 0, len(l.held))
	for _, r := range l.held {
		refs = append(refs, r.ref)
	}
	l.mu.Unlock()
	if len(refs) == 0 && !l.opts.IdleHeartbeat {
		return
	}
	ctx, cancel := context.WithTimeout(l.base, l.opts.ReportTimeout)
	defer cancel()
	lost, err := l.t.Heartbeat(ctx, l.opts.Owner, refs)
	if err != nil {
		l.log.Warn("heartbeat", "error", err.Error())
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ref := range lost {
		if r, ok := l.held[ref.Key()]; ok && r.ref == ref {
			r.log.Warn("lease lost, cancelling delivery")
			r.cancel()
		}
	}
}

// drain waits for in-flight deliveries, their leases renewed by the heartbeat meanwhile, and cancels those still
// running at the drain timeout.
func (l *loop) drain() error {
	n := l.holding()
	if n == 0 {
		return nil
	}
	l.log.Info("draining", "inFlight", n)
	timeout, stop := l.newTimer(l.opts.DrainTimeout, false)
	defer stop()
	abandoned := 0
	for l.holding() > 0 {
		select {
		case id := <-l.finished:
			l.release(id)
		case <-timeout:
			abandoned = l.cancelAll()
		}
	}
	if abandoned > 0 {
		return fmt.Errorf("%w: %d deliveries abandoned after %s", ErrDrainTimeout, abandoned, l.opts.DrainTimeout)
	}
	return nil
}
