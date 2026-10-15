package engine

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// hookStore runs test hooks inside store calls, to order the engine's work against a store answer deterministically.
type hookStore struct {
	store.Store
	afterDeliveries func() // after Deliveries read, before it returns
	afterExpire     func() // after ExpireLeases committed, before it returns
}

func (h *hookStore) Deliveries(ctx context.Context, f store.DeliveryFilter) ([]store.Delivery, error) {
	ds, err := h.Store.Deliveries(ctx, f)
	if h.afterDeliveries != nil {
		h.afterDeliveries()
	}
	return ds, err
}

func (h *hookStore) ExpireLeases(ctx context.Context) ([]store.LeaseRef, error) {
	refs, err := h.Store.ExpireLeases(ctx)
	if h.afterExpire != nil {
		h.afterExpire()
	}
	return refs, err
}

// inflightIDs returns the delivery ids the engine counts in flight, with their fencing tokens.
func inflightIDs(e *Engine) map[string]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := map[string]int64{}
	for id, f := range e.inflight {
		out[id] = f.token
	}
	return out
}

// newHookEnv is newEnv with the engine over a hookStore.
func newHookEnv(t *testing.T) (*env, *hookStore) {
	t.Helper()
	v := newEnv(t, Options{})
	h := &hookStore{Store: v.st}
	e, err := New(Options{Store: h, Now: v.clock.Now, Logger: newLogger(v.logs), LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	v.e = e
	return v, h
}

// pullOne pulls one assignment and returns its lease.
func (v *env) pullOne() store.Lease {
	v.t.Helper()
	as, err := v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 {
		v.t.Fatalf("Pull = %v, %v; want one", as, err)
	}
	return as[0].Lease
}

// An expiry tick racing a Done and a new lease of the expired delivery forgets only the lease it expired: the
// delivery done meanwhile leaves no phantom entry, and the delivery leased again keeps its new entry.
func TestExpiryTickRacingDoneAndPull(t *testing.T) {
	t.Parallel()
	v, h := newHookEnv(t)
	v.apply(writePackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc, concurrency: 2 }\n"),
		"a"))
	v.fire("p", "adhoc", "k1")
	v.fire("p", "adhoc", "k2")
	a := v.pullOne()
	v.clock.Advance(40 * time.Second)
	b := v.pullOne()
	v.clock.Advance(30 * time.Second) // a's lease expired, b's still holds

	var again store.Lease
	race := func() {
		if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: b.Ref(), Attempt: b.Attempt}); err != nil {
			t.Errorf("Done b: %v", err)
		}
		v.clock.Advance(time.Hour) // past a's backoff
		again = v.pullOne()
	}
	h.afterExpire = race
	h.afterDeliveries = race // where a resync after expiry would read the store
	v.e.expire(v.ctx)

	if again.DeliveryID != a.DeliveryID || again.FencingToken == a.FencingToken {
		t.Fatalf("leased again %+v; want %s under a new token", again, a.DeliveryID)
	}
	want := map[string]int64{a.DeliveryID: again.FencingToken}
	if got := inflightIDs(v.e); !maps.Equal(got, want) {
		t.Fatalf("in flight %v, want %v", got, want)
	}
}

// Done and Fail release a delivery only when the store ended its lease: a store error leaves it counted until it
// reports again or expires, and a stale report never releases a newer lease of the same delivery.
func TestReleaseOnlyWhenTheLeaseEnded(t *testing.T) {
	t.Parallel()
	v, _ := newHookEnv(t)
	v.apply(writePackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc }\n"), "a"))
	v.fire("p", "adhoc", "k1")
	l := v.pullOne()

	canceled, cancel := context.WithCancel(v.ctx)
	cancel()
	if err := v.e.Done(canceled, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt}); err == nil {
		t.Fatal("Done with a cancelled context succeeded")
	}
	if got := inflightIDs(v.e); len(got) != 1 {
		t.Fatalf("in flight after a failed report = %v; want the delivery still counted", got)
	}
	stale := l.Ref()
	stale.FencingToken--
	if err := v.e.Fail(v.ctx, store.FailRequest{LeaseRef: stale, Attempt: l.Attempt, Class: "x"}); err == nil {
		t.Fatal("stale Fail succeeded")
	}
	if got := inflightIDs(v.e); got[l.DeliveryID] != l.FencingToken {
		t.Fatalf("in flight after a stale report = %v; want the current lease kept", got)
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt}); err != nil {
		t.Fatal(err)
	}
	if got := inflightIDs(v.e); len(got) != 0 {
		t.Fatalf("in flight after done = %v", got)
	}
}

// The rebuild at start-up is ordered against a concurrent pull: a lease taken while Run reads the store is counted
// once the rebuild is done, never dropped by it.
func TestStartupResyncRacingPull(t *testing.T) {
	t.Parallel()
	v, h := newHookEnv(t)
	v.apply(writePackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc }\n"), "a"))
	v.fire("p", "adhoc", "k1")
	pulled := make(chan store.Lease, 1)
	h.afterDeliveries = func() {
		h.afterDeliveries = nil
		go func() {
			as, err := v.e.Pull(v.ctx, "node-a", 1)
			if err != nil || len(as) != 1 {
				t.Errorf("Pull = %v, %v", as, err)
				close(pulled)
				return
			}
			pulled <- as[0].Lease
		}()
		// Give the pull the chance to run before the rebuild swaps its map in: it must wait for the rebuild.
		select {
		case <-pulled:
			t.Error("a pull completed while the rebuild held its read")
		case <-time.After(50 * time.Millisecond): // a pull not blocked by the rebuild completes well within it
		}
	}
	ctx, cancel := context.WithCancel(v.ctx)
	done := make(chan error, 1)
	go func() { done <- v.e.Run(ctx) }()
	<-v.e.Ready()
	l, ok := <-pulled
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.FailNow()
	}
	if got := slices.Collect(maps.Keys(inflightIDs(v.e))); !slices.Equal(got, []string{l.DeliveryID}) {
		t.Fatalf("in flight %v; want the delivery pulled during start-up", got)
	}
}
