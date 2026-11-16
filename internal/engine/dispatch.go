package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

var _ node.Transport = (*Engine)(nil)

// resolveTimeout bounds resolving the deliveries one Pull leased.
const resolveTimeout = 30 * time.Second

// Pull implements node.Transport: it leases up to maxN deliveries within the steps' concurrency, the pipelines'
// limits and the steps' rates, and resolves each into an assignment. Builtin steps are the engine's own loop's
// (RunBuiltins), never a node's. Stubbed deliveries are completed on the spot by the engine and leasing continues, so
// a fully stubbed pipeline runs to the end in one call; deliveries that cannot be dispatched (an unsupported action
// kind, a config that fails to interpolate) are failed instead of returned.
func (e *Engine) Pull(ctx context.Context, owner string, maxN int) ([]node.Assignment, error) {
	return e.PullWithin(ctx, owner, maxN, e.opts.LeaseTTL/3)
}

// PullWithin is Pull, leasing for at most within (and never past a third of the lease): a remote node's pull
// returns within the wait it asked for, before its request times out. A node registered with slots takes no more
// than that many deliveries at once.
func (e *Engine) PullWithin(ctx context.Context, owner string, maxN int, within time.Duration) ([]node.Assignment,
	error,
) {
	maxN = e.capSlots(owner, maxN)
	resolve, cancel := resolving(ctx)
	defer cancel()
	// Stubs and undispatchable deliveries complete here, a lease at a time, and keep the loop going while the leases
	// already in out wait unrenewed for Pull to return. Past a third of the lease, or within, Pull returns what it
	// has; the next pull takes the rest. The first pass always runs.
	var out []node.Assignment
	start, bound := e.opts.Now(), min(within, e.opts.LeaseTTL/3)
	for first := true; len(out) < maxN && ctx.Err() == nil && (first || e.opts.Now().Sub(start) < bound); first = false {
		leases, err := e.lease(ctx, owner, maxN-len(out), false)
		if err != nil {
			if len(out) > 0 {
				break // the assignments already leased are returned; the error recurs at the next pull
			}
			return nil, err
		}
		completed := false
		for _, l := range leases {
			if e.settle(resolve, l) {
				completed = true
				continue
			}
			a, err := e.assign(resolve, owner, l)
			if err != nil {
				e.failDispatch(resolve, l, err)
				completed = true
				continue
			}
			out = append(out, a)
		}
		if !completed {
			break
		}
	}
	return out, nil
}

// resolving returns the context a lease taken under ctx is completed or resolved in: a lease taken is resolved into
// an assignment, or completed, or failed, even when the caller goes away meanwhile (a node's request cancelled, a
// drain), never left to expire, consuming an attempt, for work cancelled half-way. Leasing itself still ends with
// ctx, and resolving is bounded, so a store that stops answering cannot hold the caller. A caller abandoning its
// deliveries (its engine stepped down) reports nothing anyway: the context ends at once then.
func resolving(ctx context.Context) (context.Context, context.CancelFunc) {
	resolve, cancel := context.WithTimeout(context.WithoutCancel(ctx), resolveTimeout)
	stop := context.AfterFunc(ctx, func() {
		if errors.Is(context.Cause(ctx), node.ErrAbandon) {
			cancel()
		}
	})
	return resolve, func() { stop(); cancel() }
}

// settle completes a leased delivery in the engine and reports whether it did: with its stub (Stubber, then Stubs),
// else as a builtin (runBuiltin). A builtin a node leased (its step is an action in another open revision) runs here
// too.
func (e *Engine) settle(ctx context.Context, l store.Lease) bool {
	stub, ok := e.opts.Stubs[store.StepRef{Pipeline: l.Pipeline, Step: l.Step}]
	if e.opts.Stubber != nil {
		if s, stubbed := e.opts.Stubber(ctx, l); stubbed {
			stub, ok = s, true
		}
	}
	if ok {
		e.completeStub(ctx, l, stub)
		return true
	}
	return !l.Audit && e.runBuiltin(ctx, l)
}

// builtinBatch bounds the builtin deliveries one pass of RunBuiltins leases: each is completed before the next
// pass, so none waits long unrenewed.
const builtinBatch = 32

// builtinOwner is the owner the engine leases builtin deliveries under: the engine's, apart from every node's (a
// remote node may not register an id starting embedded-).
func (e *Engine) builtinOwner() string {
	if id := e.opts.Leadership.ID; id != "" {
		return "embedded-engine-" + id
	}
	return "embedded-engine"
}

// RunBuiltins runs the builtin steps (spec section 4.7) until ctx ends: it leases their deliveries under the
// engine's owner, within the same limits as a node's pull, and completes each in the engine, whatever the nodes'
// free slots. It passes again when a lease in flight ends (a completion makes the next step's deliveries due) and
// every TickInterval (runs fired, retries past their backoff, reviews decided). Run runs it; an engine driven
// without Run (millstream test pipeline, a backtest's replay) runs it beside its node.
func (e *Engine) RunBuiltins(ctx context.Context) {
	tick := time.NewTicker(e.opts.TickInterval)
	defer tick.Stop()
	for ctx.Err() == nil {
		changed := e.changes()
		e.builtinPass(ctx)
		select {
		case <-ctx.Done():
		case <-changed:
		case <-tick.C:
		}
	}
}

// builtinPass leases up to builtinBatch builtin deliveries and completes them, and returns how many it leased.
func (e *Engine) builtinPass(ctx context.Context) int {
	resolve, cancel := resolving(ctx)
	defer cancel()
	leases, err := e.lease(ctx, e.builtinOwner(), builtinBatch, true)
	if err != nil {
		if ctx.Err() == nil {
			e.log.Error("lease builtin deliveries", "error", err.Error())
		}
		return 0
	}
	for _, l := range leases {
		if !e.settle(resolve, l) { // its revision unreadable: retried
			e.failDispatch(resolve, l, dispatchErr(ClassEngine, true, "builtin step %s did not run", l.Step))
		}
	}
	return len(leases)
}

// lease leases up to maxN deliveries under the in-memory limits and records them in flight: those of builtin steps
// for the engine's own loop when builtins is set, a node's (owner's) otherwise.
func (e *Engine) lease(ctx context.Context, owner string, maxN int, builtins bool) ([]store.Lease, error) {
	src, err := e.limitSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("pull: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	ok, placed := true, (*nodeEntry)(nil)
	if !builtins {
		ok, placed = e.leasable(owner)
	}
	if !ok {
		return nil, nil // a draining or revoked node: nothing more
	}
	now := e.opts.Now()
	req := store.LeaseRequest{
		Owner: owner, Max: maxN, TTL: e.opts.LeaseTTL,
		Slots: map[store.StepRef]int{}, PipelineSlots: map[string]int{}, Reserve: src.reserve,
	}
	perStep, perPipeline := e.countInflight()
	for _, rev := range src.active {
		name := rev.p.Name
		if limit := rev.p.Limits.Concurrency; limit > 0 {
			req.PipelineSlots[name] = limit - perPipeline[name]
		}
	}
	for ref, step := range src.steps {
		if src.builtin[ref] != builtins {
			req.Exclude = append(req.Exclude, ref) // builtins are the engine's loop's, every other step a node's
			continue
		}
		if placed != nil && !src.placement[ref].Matches(placed.info.Runtimes, placed.info.Labels) {
			req.Exclude = append(req.Exclude, ref) // the node cannot run it (spec section 5.10)
			continue
		}
		free := max(step.Concurrency, 1) - perStep[ref]
		if b := e.bucket(ref, step.Rate, now); b != nil {
			tokens := b.Available(now)
			if tokens == 0 {
				req.Exclude = append(req.Exclude, ref)
				continue
			}
			free = min(free, tokens)
		}
		req.Slots[ref] = free
	}
	leases, err := e.st.Lease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("pull: %w", err)
	}
	if len(leases) > 0 && e.leased.CompareAndSwap(false, true) {
		e.log.Info("first dispatch", "leases", len(leases), "engine", e.opts.Leadership.ID,
			"epoch", e.opts.Leadership.Epoch)
	}
	for _, l := range leases {
		ref := store.StepRef{Pipeline: l.Pipeline, Step: l.Step}
		e.inflight[l.Ref().Key()] = flight{
			step: ref, runID: l.RunID, itemID: l.Item.ID, attempt: l.Attempt, token: l.FencingToken, owner: owner,
		}
		if b := e.buckets[ref]; b != nil {
			b.Take()
		}
	}
	return leases, nil
}

// sources are what the in-memory limits and placement read: each pipeline's active revision, whose limits apply;
// the definition each step's concurrency and rate come from, and its placement; the steps that are builtins in every
// open revision that has them, which the engine runs; and the agent steps of each revision
// with the budget reservation of each delivery (the maxCostUsd of its action in its run's revision, as budget.runUsd
// is that revision's).
type sources struct {
	active    []*revision
	steps     map[store.StepRef]pipeline.Step
	placement map[store.StepRef]node.Placement
	builtin   map[store.StepRef]bool
	reserve   map[store.AgentStep]float64
}

// limitSources returns the sources of the limits. A step's definition is the active revision's, or, for a step only
// older revisions have, that of the newest revision with unsettled runs that has it, so its remaining deliveries are
// still limited.
func (e *Engine) limitSources(ctx context.Context) (sources, error) {
	pipelines, err := e.st.Pipelines(ctx)
	if err != nil {
		return sources{}, err
	}
	open, err := e.st.OpenRevisions(ctx)
	if err != nil {
		return sources{}, err
	}
	src := sources{
		active: make([]*revision, 0, len(pipelines)), steps: map[store.StepRef]pipeline.Step{},
		placement: map[store.StepRef]node.Placement{}, builtin: map[store.StepRef]bool{},
		reserve: map[store.AgentStep]float64{},
	}
	add := func(rev *revision) {
		for name, step := range rev.p.Steps {
			ref := store.StepRef{Pipeline: rev.p.Name, Step: name}
			ra := e.action(rev, step.Action)
			if ra.err == nil && ra.a.Kind == pipeline.KindAgent {
				src.reserve[store.AgentStep{StepRef: ref, Revision: rev.id}] = ra.a.Agent.MaxCostUSD
			}
			if step.Fallback != nil {
				if fa := e.action(rev, step.Fallback.Action); fa.err == nil && fa.a.Kind == pipeline.KindAgent {
					src.reserve[store.AgentStep{StepRef: ref, Revision: rev.id, Fallback: true}] = fa.a.Agent.MaxCostUSD
				}
			}
			isBuiltin := pipeline.IsBuiltin(step.Action)
			if _, ok := src.steps[ref]; !ok {
				src.builtin[ref] = isBuiltin
				src.steps[ref] = step
				src.placement[ref] = stepPlacement(rev.p, step, ra)
			} else {
				src.builtin[ref] = src.builtin[ref] && isBuiltin
			}
		}
	}
	for _, ps := range pipelines {
		rev, err := e.revision(ctx, ps.ActiveRevision)
		if err != nil {
			return sources{}, err
		}
		src.active = append(src.active, rev)
		add(rev)
	}
	for _, id := range slices.Backward(open) { // newest first
		rev, err := e.revision(ctx, id)
		if err != nil {
			return sources{}, err
		}
		add(rev)
	}
	return src, nil
}

// countInflight counts the deliveries in flight per step and per pipeline. The caller holds e.mu.
func (e *Engine) countInflight() (map[store.StepRef]int, map[string]int) {
	perStep, perPipeline := map[store.StepRef]int{}, map[string]int{}
	for _, f := range e.inflight {
		perStep[f.step]++
		perPipeline[f.step.Pipeline]++
	}
	return perStep, perPipeline
}

// bucket returns the step's rate bucket, nil when it has no rate. A changed rate replaces the bucket. The caller
// holds e.mu.
func (e *Engine) bucket(ref store.StepRef, rate string, now time.Time) *Bucket {
	if rate == "" {
		delete(e.buckets, ref)
		return nil
	}
	old := e.buckets[ref]
	if old != nil && old.rate == rate {
		return old
	}
	count, per, err := pipeline.ParseRate(rate)
	if err != nil {
		e.log.Error("invalid step rate, step not rate limited", "pipeline", ref.Pipeline, "step", ref.Step,
			"error", err.Error())
		return nil
	}
	b := NewBucket(rate, count, per, now)
	if old == nil && e.opts.Takeover {
		b.tokens = 0 // the step's first bucket under this engine: no burst on a takeover
	}
	e.buckets[ref] = b
	return b
}

// release forgets the delivery in flight under ref's lease, wakes WaitSettled, and returns what was recorded about
// it. A newer lease of the same delivery, under another fencing token, is left alone.
func (e *Engine) release(ref store.LeaseRef) (flight, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	f, ok := e.inflight[ref.Key()]
	if !ok || f.token != ref.FencingToken {
		return flight{}, false
	}
	delete(e.inflight, ref.Key())
	if e.changed != nil {
		close(e.changed)
		e.changed = nil
	}
	return f, true
}

// lookup returns what is recorded about the delivery in flight under ref's lease, without forgetting it.
func (e *Engine) lookup(ref store.LeaseRef) flight {
	e.mu.Lock()
	defer e.mu.Unlock()
	if f, ok := e.inflight[ref.Key()]; ok && f.token == ref.FencingToken {
		return f
	}
	return flight{}
}

// ended reports whether a Done or Fail outcome ended the lease: the store applied it, or refused it as stale
// because the lease had already ended. Any other error leaves the delivery leased until it reports again or its
// lease expires, so it keeps counting in flight.
func ended(err error) bool { return err == nil || errors.Is(err, store.ErrStaleLease) }

// Heartbeat implements node.Transport: it renews owner's leases and returns those lost, which stop counting in
// flight.
func (e *Engine) Heartbeat(ctx context.Context, owner string, refs []store.LeaseRef) ([]store.LeaseRef, error) {
	lost, err := e.st.Renew(ctx, owner, refs, e.opts.LeaseTTL)
	if err != nil {
		return nil, fmt.Errorf("heartbeat: %w", err)
	}
	for _, ref := range lost {
		e.release(ref)
	}
	return lost, nil
}

// Done implements node.Transport. The delivery stops counting in flight once its lease ended (see ended). An
// audit's lease ends the audit instead (EndAudit).
func (e *Engine) Done(ctx context.Context, req store.DoneRequest) error {
	if ids.IsAudit(req.DeliveryID) {
		return e.endAudit(ctx, store.AuditEnd{LeaseRef: req.LeaseRef, Owner: req.Owner, Outputs: req.Outputs,
			CostUSD: req.CostUSD, Metrics: req.Metrics})
	}
	state, err := e.st.Done(ctx, req)
	f := e.settleFlight(req.LeaseRef, err)
	if err != nil {
		return fmt.Errorf("done %s: %w", req.DeliveryID, err)
	}
	e.flightLog(req.LeaseRef, f).Info("delivery done", "state", string(state), "outputs", len(req.Outputs))
	return nil
}

// Fail implements node.Transport, counting in flight like Done. The message is redacted of the delivery's secrets
// before it is stored, so neither the delivery's error nor its dead letter holds them.
func (e *Engine) Fail(ctx context.Context, req store.FailRequest) error {
	req.Message = e.lookup(req.LeaseRef).redact.String(req.Message)
	if ids.IsAudit(req.DeliveryID) {
		return e.endAudit(ctx, store.AuditEnd{LeaseRef: req.LeaseRef, Owner: req.Owner, Class: req.Class,
			Message: req.Message, CostUSD: req.CostUSD, Unreported: req.Unreported, Metrics: req.Metrics})
	}
	state, err := e.st.Fail(ctx, req)
	f := e.settleFlight(req.LeaseRef, err)
	if err != nil {
		return fmt.Errorf("fail %s: %w", req.DeliveryID, err)
	}
	e.flightLog(req.LeaseRef, f).Info("delivery failed", "state", string(state), "class", req.Class,
		"retryable", req.Retryable)
	return nil
}

// endAudit ends an audit's lease with the fallback's outcome, counting in flight like Done.
func (e *Engine) endAudit(ctx context.Context, req store.AuditEnd) error {
	err := e.st.EndAudit(ctx, req)
	f := e.settleFlight(req.LeaseRef, err)
	if err != nil {
		return fmt.Errorf("end audit %s: %w", req.DeliveryID, err)
	}
	e.flightLog(req.LeaseRef, f).Info("audit ended", "class", req.Class, "outputs", len(req.Outputs))
	return nil
}

// settleFlight releases the delivery in flight when the store's answer err ended its lease, and returns what was
// recorded about it either way.
func (e *Engine) settleFlight(ref store.LeaseRef, err error) flight {
	if ended(err) {
		f, _ := e.release(ref)
		return f
	}
	return e.lookup(ref)
}

func (e *Engine) flightLog(ref store.LeaseRef, f flight) *slog.Logger {
	return f.redact.Logger(e.log).With("pipeline", ref.Pipeline, "runId", f.runID, "step", f.step.Step,
		"itemId", f.itemID, "deliveryId", ref.DeliveryID, "attempt", f.attempt)
}

// completeStub completes a stubbed delivery with the stub's outcome.
func (e *Engine) completeStub(ctx context.Context, l store.Lease, s Stub) {
	var err error
	switch {
	case s.Filter != "":
		ref := l.Ref()
		err = e.complete(ref, func() (store.DeliveryState, error) {
			return e.st.Filter(ctx, store.FilterRequest{LeaseRef: ref, Attempt: l.Attempt, Reason: s.Filter})
		})
	case s.Err != nil:
		err = e.Fail(ctx, store.FailRequest{
			LeaseRef: l.Ref(), Attempt: l.Attempt, Class: node.ClassAction, Message: s.Err.Message,
			Retryable: s.Err.Retryable,
		})
	default:
		err = e.Done(ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Outputs: s.Outputs})
	}
	if err != nil {
		e.leaseLog(l).Error("complete stubbed delivery", "error", err.Error())
	}
}

// failDispatch fails a delivery the engine could not turn into an assignment.
func (e *Engine) failDispatch(ctx context.Context, l store.Lease, err error) {
	de := &dispatchError{class: ClassEngine, retryable: true, err: err}
	errors.As(err, &de)
	e.lookup(l.Ref()).redact.Logger(e.leaseLog(l)).Warn("delivery not dispatched", "class", de.class,
		"retryable", de.retryable, "error", err.Error())
	if ferr := e.Fail(ctx, store.FailRequest{
		LeaseRef: l.Ref(), Attempt: l.Attempt, Class: de.class, Message: err.Error(), Retryable: de.retryable,
	}); ferr != nil {
		e.leaseLog(l).Error("fail undispatched delivery", "error", ferr.Error())
	}
}

func (e *Engine) leaseLog(l store.Lease) *slog.Logger {
	return e.log.With("pipeline", l.Pipeline, "runId", l.RunID, "step", l.Step, "itemId", l.Item.ID,
		"deliveryId", l.DeliveryID, "attempt", l.Attempt)
}
