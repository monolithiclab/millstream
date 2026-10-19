package backtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// FilterNotReplayed is the filter reason of a replay's delivery the stubber cut short: off the sampled lineages, or
// an external action or a review without a recorded outcome for its input.
const FilterNotReplayed = "notReplayed"

// decision is how the stubber settled a delivery; a delivery it let run has none.
type decision int

const (
	ran decision = iota
	// recorded: the delivery took its historical outcome.
	recorded
	// offSample: the delivery's item is a historical one off the sampled lineages.
	offSample
	// unrecorded: an external action or a review met an input the history has no outcome for.
	unrecorded
)

// stubber decides each delivery of a replay (the package documentation lists the rules).
type stubber struct {
	h        *history
	keep     map[string]bool
	changed  map[string]bool
	external map[string]bool
	review   map[string]bool

	mu        sync.Mutex
	decisions map[string]decision
}

func (s *stubber) decide(id string, d decision) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.decisions[id] = d
}

// stub is the engine's Stubber.
func (s *stubber) stub(_ context.Context, l store.Lease) (engine.Stub, bool) {
	cut := engine.Stub{Filter: FilterNotReplayed}
	if l.Audit { // an audit is no part of the run, and would spend for nothing here
		s.decide(l.DeliveryID, offSample)
		return engine.Stub{Err: &exec.ActionError{Message: "audits do not run in a backtest"}}, true
	}
	h, inHistory := s.h.items[l.Item.ID]
	if inHistory && !s.keep[l.Item.ID] {
		s.decide(l.DeliveryID, offSample)
		return cut, true
	}
	if s.changed[l.Step] && !s.external[l.Step] && !s.review[l.Step] {
		return engine.Stub{}, false // a changed step runs, whatever its history
	}
	var outcome engine.Stub
	has := false
	if inHistory && canon(h.Payload) == canon(l.Item.Payload) {
		if d, ok := s.h.delivery(l.Step, l.Item.ID); ok && ended(d) {
			outcome, has = s.recorded(d), true
		}
	}
	switch {
	case s.external[l.Step] || s.review[l.Step] && !s.changed[l.Step]:
		if !has {
			s.decide(l.DeliveryID, unrecorded)
			return cut, true
		}
	case s.changed[l.Step] || !has:
		return engine.Stub{}, false
	}
	s.decide(l.DeliveryID, recorded)
	return outcome, true
}

// recorded is a historical delivery's outcome as a stub.
func (s *stubber) recorded(d store.Delivery) engine.Stub {
	switch d.State {
	case store.DeliveryFiltered:
		return engine.Stub{Filter: d.FilterReason}
	case store.DeliveryDead:
		return engine.Stub{Err: &exec.ActionError{Message: d.ErrorMessage}}
	}
	out := outputs(s.h.children, d)
	if out == nil {
		out = []store.Output{}
	}
	return engine.Stub{Outputs: out}
}

// replayed is what a replay left in its scratch store.
type replayed struct {
	deliveries map[string]store.Delivery
	items      map[string]store.Item
	children   map[string][]store.Item
	decisions  map[string]decision
	costUSD    float64
}

// replayOptions are one replay's: the package to replay, the runs to fire, the stubber deciding its deliveries, and
// what it may spend when limited.
type replayOptions struct {
	pkg     *pipeline.Package
	firings []firing
	stubber *stubber
	limit   float64
	limited bool
}

// replay fires the runs again in a scratch store on o.pkg, its artifacts under work, and waits until they settle.
func (b *Backtester) replay(ctx context.Context, work string, o replayOptions) (*replayed, error) {
	ctx, cancel := context.WithTimeout(ctx, b.opts.Timeout)
	defer cancel()
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = st.Close() }()
	artsDir, err := os.MkdirTemp(work, "artifacts-")
	if err != nil {
		return nil, err
	}
	e, err := engine.New(engine.Options{Store: st, Env: b.opts.Env, Secrets: b.opts.Secrets, NoCron: true,
		Resolver: b.opts.Resolver, Artifacts: artifacts.New(artsDir), Agents: b.opts.Agents, Stubber: o.stubber.stub,
		Logger: b.log})
	if err != nil {
		return nil, err
	}
	if _, err := e.Apply(ctx, o.pkg); err != nil {
		return nil, err
	}
	name := o.pkg.Pipeline.Name
	var refs []engine.RunRef
	for _, f := range o.firings {
		if _, err := st.Fire(ctx, store.Firing{Pipeline: name, Trigger: f.trigger, Key: f.key, RunID: f.runID,
			Source: store.SourceManual, Payload: f.payload, Vars: f.vars, VarSources: f.sources}); err != nil {
			return nil, fmt.Errorf("fire run %s again: %w", f.runID, err)
		}
		refs = append(refs, engine.RunRef{Pipeline: name, ID: f.runID})
	}
	pool := exec.NewPool(exec.PoolOptions{Options: exec.Options{Launchers: b.opts.Launchers, Logger: b.log}})
	defer func() { _ = pool.Close() }()
	n, err := node.New(e, node.Options{Owner: "backtest", Slots: b.opts.Slots, Pool: pool,
		PullInterval: 20 * time.Millisecond, Logger: b.log})
	if err != nil {
		return nil, err
	}
	runCtx, stop := context.WithCancel(ctx)
	nodeDone := make(chan error, 1)
	go func() { nodeDone <- n.Run(runCtx) }()
	waitErr := wait(ctx, e, st, refs, o.limit, o.limited)
	stop()
	if err := <-nodeDone; err != nil && waitErr == nil {
		waitErr = err
	}
	r, err := read(ctx, st, name)
	if err != nil {
		return nil, errors.Join(waitErr, err)
	}
	r.decisions = o.stubber.decisions
	return r, waitErr
}

// wait waits until the runs settle (or wait only on reviews), or, when limited, their cost passes limit: checked on
// each tick and once settled, rather than at every change, which every delivery ending makes.
func wait(ctx context.Context, e *engine.Engine, st store.Store, refs []engine.RunRef, limit float64,
	limited bool,
) error {
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	overspent := func() error {
		if !limited {
			return nil
		}
		cost, err := runsCost(ctx, st, refs[0].Pipeline)
		if err == nil && cost > limit {
			err = fmt.Errorf("%w: the replay spent %.4f USD", ErrBudget, cost)
		}
		return err
	}
	for {
		changed := e.Changes()
		settled, err := e.Settled(ctx, refs)
		if err != nil {
			return err
		}
		if settled {
			return overspent()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("replay: %w", ctx.Err())
		case <-changed:
		case <-poll.C:
			if err := overspent(); err != nil {
				return err
			}
		}
	}
}

// runsCost is what the scratch store's runs cost.
func runsCost(ctx context.Context, st store.Store, pipelineName string) (float64, error) {
	runs, err := st.Runs(ctx, store.RunFilter{Pipeline: pipelineName})
	if err != nil {
		return 0, err
	}
	var sum float64
	for _, r := range runs {
		sum += r.CostUSD
	}
	return sum, nil
}

// read reads back what a replay left.
func read(ctx context.Context, st store.Store, pipelineName string) (*replayed, error) {
	r := &replayed{deliveries: map[string]store.Delivery{}, items: map[string]store.Item{},
		children: map[string][]store.Item{}}
	ds, err := st.Deliveries(ctx, store.DeliveryFilter{Pipeline: pipelineName})
	if err != nil {
		return nil, err
	}
	for _, d := range ds {
		r.deliveries[d.ID] = d
	}
	items, err := st.Items(ctx, store.ItemFilter{Pipeline: pipelineName})
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		r.items[it.ID] = it
		for _, p := range it.Parents {
			r.children[p] = append(r.children[p], it)
		}
	}
	r.costUSD, err = runsCost(ctx, st, pipelineName)
	return r, err
}
