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
	// reportTimeout bounds each heartbeat, Done and Fail call.
	reportTimeout = 30 * time.Second
)

// ErrAbandon, as the cause of Run's context cancellation (context.WithCancelCause), makes the node abandon its
// deliveries in flight rather than drain them.
var ErrAbandon = errors.New("abandon deliveries in flight")

// ErrDrainTimeout is returned, wrapped, by Run when in-flight deliveries were abandoned at the drain timeout. Their
// leases expire and the deliveries are retried elsewhere.
var ErrDrainTimeout = errors.New("drain timeout")

// Assignment is one leased delivery, resolved by the engine: the node only runs it.
type Assignment struct {
	Lease store.Lease
	// Spec is the action process to run, with the step's config interpolated for the run.
	Spec    exec.Spec
	ItemMsg protocol.Item
	// Timeout is the step's timeout; zero means none.
	Timeout time.Duration
	// Redact removes the values of the secrets resolved for the delivery (spec section 4.5) from its logs, the
	// action's stderr included, and from the error messages reported for it. Nil redacts nothing.
	Redact *redact.Redactor
}

// Artifacts is the artifact store as a node reaches it (spec section 5.10): the engine's local store for the
// embedded node; remote nodes (M4) move contents through signed URLs behind the same calls.
type Artifacts interface {
	// PutArtifact stores an output artifact of pipeline and returns its reference under name.
	PutArtifact(ctx context.Context, pipeline, name string, r io.Reader) (store.ArtifactRef, error)
	// OpenArtifact opens an input artifact of pipeline.
	OpenArtifact(ctx context.Context, pipeline string, ref store.ArtifactRef) (io.ReadCloser, error)
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
	DrainTimeout      time.Duration
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
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = DefaultDrainTimeout
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

// loop is the state of one Run. Only Run's goroutine touches it; deliveries report back on finished.
type loop struct {
	*Node
	base     context.Context // outlives Run's ctx, so in-flight work and its reports survive the drain
	held     map[store.DeliveryKey]*running
	finished chan store.DeliveryKey
}

// Run pulls and runs deliveries until ctx is cancelled, then drains. It returns nil after a clean drain.
func (n *Node) Run(ctx context.Context) error {
	l := &loop{Node: n, base: context.WithoutCancel(ctx), held: map[store.DeliveryKey]*running{},
		finished: make(chan store.DeliveryKey)}
	pull, stopPull := n.newTimer(n.opts.PullInterval, true)
	defer stopPull()
	heartbeat, stopHeartbeat := n.newTimer(n.opts.HeartbeatInterval, true)
	defer stopHeartbeat()
	l.pull(ctx)
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-pull:
			l.pull(ctx)
		case <-heartbeat:
			l.heartbeat()
		case id := <-l.finished:
			delete(l.held, id)
			l.pull(ctx)
		}
	}
	if errors.Is(context.Cause(ctx), ErrAbandon) {
		l.abandon()
		return nil
	}
	return l.drain(heartbeat)
}

// abandon cancels every delivery in flight and waits for their goroutines, which drop their results.
func (l *loop) abandon() {
	if len(l.held) == 0 {
		return
	}
	l.log.Warn("abandoning deliveries in flight", "inFlight", len(l.held))
	for _, r := range l.held {
		r.cancel()
	}
	for len(l.held) > 0 {
		delete(l.held, <-l.finished)
	}
}

func (l *loop) pull(ctx context.Context) {
	free := l.opts.Slots - len(l.held)
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
	l.held[ref.Key()] = &running{ref: ref, log: log, cancel: cancel}
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
		Item: a.ItemMsg, Inputs: a.Lease.Item.Artifacts, Artifacts: pipelineArtifacts{l.t, a.Lease.Pipeline},
		Timeout: a.Timeout, Log: log,
	})
	if ctx.Err() != nil {
		log.Warn("delivery abandoned, result dropped")
		return
	}
	rctx, cancel := context.WithTimeout(l.base, reportTimeout)
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

// pipelineArtifacts binds the transport's artifact store to one pipeline, as the runtime moves a delivery's files.
type pipelineArtifacts struct {
	t        Artifacts
	pipeline string
}

func (p pipelineArtifacts) Open(ctx context.Context, ref store.ArtifactRef) (io.ReadCloser, error) {
	return p.t.OpenArtifact(ctx, p.pipeline, ref)
}

func (p pipelineArtifacts) Put(ctx context.Context, name string, r io.Reader) (store.ArtifactRef, error) {
	return p.t.PutArtifact(ctx, p.pipeline, name, r)
}

// heartbeat renews every held lease and cancels the deliveries whose lease is lost.
func (l *loop) heartbeat() {
	if len(l.held) == 0 {
		return
	}
	refs := make([]store.LeaseRef, 0, len(l.held))
	for _, r := range l.held {
		refs = append(refs, r.ref)
	}
	ctx, cancel := context.WithTimeout(l.base, reportTimeout)
	defer cancel()
	lost, err := l.t.Heartbeat(ctx, l.opts.Owner, refs)
	if err != nil {
		l.log.Warn("heartbeat", "error", err.Error())
		return
	}
	for _, ref := range lost {
		if r, ok := l.held[ref.Key()]; ok && r.ref == ref {
			r.log.Warn("lease lost, cancelling delivery")
			r.cancel()
		}
	}
}

// drain waits for in-flight deliveries, renewing their leases, and cancels those still running at the drain
// timeout.
func (l *loop) drain(heartbeat <-chan time.Time) error {
	if len(l.held) == 0 {
		return nil
	}
	l.log.Info("draining", "inFlight", len(l.held))
	timeout, stop := l.newTimer(l.opts.DrainTimeout, false)
	defer stop()
	abandoned := 0
	for len(l.held) > 0 {
		select {
		case id := <-l.finished:
			delete(l.held, id)
		case <-heartbeat:
			l.heartbeat()
		case <-timeout:
			abandoned = len(l.held)
			for _, r := range l.held {
				r.cancel()
			}
		}
	}
	if abandoned > 0 {
		return fmt.Errorf("%w: %d deliveries abandoned after %s", ErrDrainTimeout, abandoned, l.opts.DrainTimeout)
	}
	return nil
}
