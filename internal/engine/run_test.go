package engine

import (
	"context"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// WaitSettled returns once the runs settled, woken by the end of a lease in flight, and honours its context.
func TestWaitSettled(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	v.apply(newPackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc }\n"), "a"))
	id := v.fire("p", "adhoc", "k")
	runs := []RunRef{{Pipeline: "p", ID: id}}
	ctx, cancel := context.WithCancel(v.ctx)
	cancel()
	if err := v.e.WaitSettled(ctx, runs); err == nil {
		t.Fatal("WaitSettled with a cancelled context succeeded")
	}
	if _, err := v.e.Settled(v.ctx, []RunRef{{Pipeline: "p", ID: "nope"}}); err == nil {
		t.Fatal("Settled of an unknown run succeeded")
	}
	l := v.pullOne()
	waited := make(chan error, 1)
	go func() { waited <- v.e.WaitSettled(v.ctx, runs) }()
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt}); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err != nil {
		t.Fatalf("WaitSettled = %v", err)
	}
}

// TestRunExpiresLeases runs the engine loop: a lease that expires by the store clock consumes its attempt, returns
// to pending and is leased again after its backoff.
func TestRunExpiresLeases(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{LeaseTTL: time.Minute})
	v.apply(newPackage(t, manualPipeline("p", "  a: { action: ./actions/a, in: trigger.adhoc }\n"), "a"))
	id := v.fire("p", "adhoc", "k")
	if got := v.pull(1); len(got) != 1 {
		t.Fatalf("pull = %v", got)
	}
	v.clock.Advance(2 * time.Minute)
	ticked := make(chan struct{}, 1)
	v.e.ticked = func() {
		select {
		case ticked <- struct{}{}:
		default:
		}
	}
	ctx, cancel := context.WithCancel(v.ctx)
	done := make(chan error, 1)
	go func() { done <- v.e.Run(ctx) }()
	<-ticked // Run's first pass, on the advanced clock
	if d := v.deliveries(id)[0]; d.State != store.DeliveryPending {
		t.Fatalf("the lease did not expire: %+v", d)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if d := v.deliveries(id)[0]; d.Attempt != 2 || d.ErrorClass != store.ClassLeaseExpired {
		t.Fatalf("expired delivery = %+v; want attempt 2, class leaseExpired", d)
	}
	v.clock.Advance(time.Hour) // past the backoff
	if got := v.pull(1); len(got) != 1 {
		t.Fatalf("pull after expiry = %v", got)
	}
}

func TestRunReturnsWhenCancelledBeforeStart(t *testing.T) {
	t.Parallel()
	v := newEnv(t, Options{})
	ctx, cancel := context.WithCancel(v.ctx)
	cancel()
	if err := v.e.Run(ctx); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if err := v.st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.e.Run(v.ctx); err == nil {
		t.Fatal("Run on a closed store succeeded")
	}
}
