package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

var _ node.Transport = (*Engine)(nil)

// Pull implements node.Transport: it leases up to maxN deliveries within the steps' concurrency, the pipelines'
// limits and the steps' rates, and resolves each into an assignment. Stubbed deliveries and builtin deliveries
// (filter, dedupe) are completed on the spot by the engine and leasing continues, so a node never receives a builtin
// and a fully stubbed pipeline runs to the end in one call; deliveries that cannot be dispatched (an unsupported
// action kind, a config that fails to interpolate) are failed instead of returned.
func (e *Engine) Pull(ctx context.Context, owner string, maxN int) ([]node.Assignment, error) {
	var out []node.Assignment
	for len(out) < maxN {
		leases, err := e.lease(ctx, owner, maxN-len(out))
		if err != nil {
			if len(out) > 0 {
				break // the assignments already leased are returned; the error recurs at the next pull
			}
			return nil, err
		}
		completed := false
		for _, l := range leases {
			if stub, ok := e.opts.Stubs[store.StepRef{Pipeline: l.Pipeline, Step: l.Step}]; ok {
				e.completeStub(ctx, l, stub)
				completed = true
				continue
			}
			if e.runBuiltin(ctx, l) {
				completed = true
				continue
			}
			a, err := e.assign(ctx, l)
			if err != nil {
				e.failDispatch(ctx, l, err)
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

// lease leases up to maxN deliveries under the in-memory limits and records them in flight.
func (e *Engine) lease(ctx context.Context, owner string, maxN int) ([]store.Lease, error) {
	active, steps, reserve, err := e.limitSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("pull: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.opts.Now()
	req := store.LeaseRequest{
		Owner: owner, Max: maxN, TTL: e.opts.LeaseTTL,
		Slots: map[store.StepRef]int{}, PipelineSlots: map[string]int{}, Reserve: reserve,
	}
	perStep, perPipeline := e.countInflight()
	for _, rev := range active {
		name := rev.p.Name
		if limit := rev.p.Limits.Concurrency; limit > 0 {
			req.PipelineSlots[name] = limit - perPipeline[name]
		}
	}
	for ref, step := range steps {
		free := max(step.Concurrency, 1) - perStep[ref]
		if b := e.bucket(ref, step.Rate, now); b != nil {
			tokens := b.available(now)
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
	for _, l := range leases {
		ref := store.StepRef{Pipeline: l.Pipeline, Step: l.Step}
		e.inflight[l.DeliveryID] = flight{
			step: ref, runID: l.RunID, itemID: l.Item.ID, attempt: l.Attempt, token: l.FencingToken,
		}
		if b := e.buckets[ref]; b != nil {
			b.take()
		}
	}
	return leases, nil
}

// limitSources returns each pipeline's active revision, whose limits apply, the definition each step's concurrency
// and rate come from, and the agent steps with the budget reservation of each delivery (its action's maxCostUsd).
// A step's definition is the active revision's, or, for a step only older revisions have, that of the newest
// revision with unsettled runs that has it, so its remaining deliveries are still limited.
func (e *Engine) limitSources(ctx context.Context) ([]*revision, map[store.StepRef]pipeline.Step,
	map[store.StepRef]float64, error,
) {
	pipelines, err := e.st.Pipelines(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	open, err := e.st.OpenRevisions(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	active := make([]*revision, 0, len(pipelines))
	steps := map[store.StepRef]pipeline.Step{}
	reserve := map[store.StepRef]float64{}
	add := func(rev *revision) {
		for name, step := range rev.p.Steps {
			ref := store.StepRef{Pipeline: rev.p.Name, Step: name}
			if _, ok := steps[ref]; ok {
				continue
			}
			steps[ref] = step
			if ra := e.action(rev, step.Action); ra.err == nil && ra.a.Kind == pipeline.KindAgent {
				reserve[ref] = ra.a.Agent.MaxCostUSD
			}
		}
	}
	for _, ps := range pipelines {
		rev, err := e.revision(ctx, ps.ActiveRevision)
		if err != nil {
			return nil, nil, nil, err
		}
		active = append(active, rev)
		add(rev)
	}
	for _, id := range slices.Backward(open) { // newest first
		rev, err := e.revision(ctx, id)
		if err != nil {
			return nil, nil, nil, err
		}
		add(rev)
	}
	return active, steps, reserve, nil
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
func (e *Engine) bucket(ref store.StepRef, rate string, now time.Time) *bucket {
	if rate == "" {
		delete(e.buckets, ref)
		return nil
	}
	if b := e.buckets[ref]; b != nil && b.rate == rate {
		return b
	}
	count, per, err := pipeline.ParseRate(rate)
	if err != nil {
		e.log.Error("invalid step rate, step not rate limited", "pipeline", ref.Pipeline, "step", ref.Step,
			"error", err.Error())
		return nil
	}
	b := newBucket(rate, count, per, now)
	e.buckets[ref] = b
	return b
}

// release forgets the delivery in flight under ref's lease and returns what was recorded about it. A newer lease of
// the same delivery, under another fencing token, is left alone.
func (e *Engine) release(ref store.LeaseRef) (flight, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	f, ok := e.inflight[ref.DeliveryID]
	if !ok || f.token != ref.FencingToken {
		return flight{}, false
	}
	delete(e.inflight, ref.DeliveryID)
	return f, true
}

// lookup returns what is recorded about the delivery in flight under ref's lease, without forgetting it.
func (e *Engine) lookup(ref store.LeaseRef) flight {
	e.mu.Lock()
	defer e.mu.Unlock()
	if f, ok := e.inflight[ref.DeliveryID]; ok && f.token == ref.FencingToken {
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

// Done implements node.Transport. The delivery stops counting in flight once its lease ended (see ended).
func (e *Engine) Done(ctx context.Context, req store.DoneRequest) error {
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
	state, err := e.st.Fail(ctx, req)
	f := e.settleFlight(req.LeaseRef, err)
	if err != nil {
		return fmt.Errorf("fail %s: %w", req.DeliveryID, err)
	}
	e.flightLog(req.LeaseRef, f).Info("delivery failed", "state", string(state), "class", req.Class,
		"retryable", req.Retryable)
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
	if s.Err != nil {
		err = e.Fail(ctx, store.FailRequest{
			LeaseRef: l.Ref(), Attempt: l.Attempt, Class: node.ClassAction, Message: s.Err.Message,
			Retryable: s.Err.Retryable,
		})
	} else {
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
