package sqlite

import (
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Review focus 5: a run cancelled while a delivery is leased discards its outputs and ends cancelled with its
// cost.
func TestCancelWhileLeased(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	e.fire("r1")
	l := e.lease(1)[0]
	if err := e.s.Cancel(e.ctx, "listings", "r1"); err != nil {
		t.Fatal(err)
	}
	r := e.run("r1")
	if r.State != store.RunCancelled || !r.SettledAt.IsZero() {
		t.Fatalf("run = %+v; want cancelled, not settled while a delivery is leased", r)
	}
	state, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1, CostUSD: 0.4,
		Outputs: []store.Output{out("a", `{"score":1}`)}})
	if err != nil || state != store.DeliveryCancelled {
		t.Fatalf("Done = %v, %v; want cancelled", state, err)
	}
	if n := len(e.items("listings.raw")); n != 0 {
		t.Fatalf("%d outputs published for a cancelled run", n)
	}
	r = e.run("r1")
	if r.State != store.RunCancelled || r.SettledAt.IsZero() || r.CostUSD != 0.4 {
		t.Fatalf("run = %+v; want settled cancelled with its cost", r)
	}
	if d := e.deliveries("r1")[0]; d.State != store.DeliveryCancelled || d.CostUSD != 0.4 {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestCancelFailAndExpiry(t *testing.T) {
	t.Parallel()
	t.Run("fail after cancel", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, fixture)
		e.fire("r1")
		l := e.lease(1)[0]
		if err := e.s.Cancel(e.ctx, "listings", "r1"); err != nil {
			t.Fatal(err)
		}
		wantState(t, "state", e.fail(l, true), store.DeliveryCancelled)
		wantState(t, "run", e.run("r1").State, store.RunCancelled)
	})
	t.Run("lease expires after cancel", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, fixture)
		e.fire("r1")
		e.lease(1)
		if err := e.s.Cancel(e.ctx, "listings", "r1"); err != nil {
			t.Fatal(err)
		}
		e.clock.Advance(2 * time.Minute)
		if n, err := e.s.ExpireLeases(e.ctx); n != 1 || err != nil {
			t.Fatalf("ExpireLeases = %d, %v", n, err)
		}
		if d := e.deliveries("r1")[0]; d.State != store.DeliveryCancelled {
			t.Fatalf("delivery = %s, want cancelled", d.State)
		}
		if r := e.run("r1"); r.SettledAt.IsZero() {
			t.Fatal("run not settled")
		}
	})
}

func TestCancelPendingAndRetrying(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{"score":1}`), out("b", `{"score":2}`))
	e.fail(e.leaseStep("score"), true) // a retry waiting for its backoff
	if err := e.s.Cancel(e.ctx, "listings", "r1"); err != nil {
		t.Fatal(err)
	}
	for _, d := range e.deliveries("r1") {
		if d.Step != "crawl" && d.State != store.DeliveryCancelled {
			t.Fatalf("delivery %s of %s is %s", d.ID, d.Step, d.State)
		}
	}
	r := e.run("r1")
	if r.State != store.RunCancelled || r.SettledAt.IsZero() {
		t.Fatalf("run = %+v", r)
	}
	if err := e.s.Cancel(e.ctx, "listings", "r1"); err != nil {
		t.Fatalf("cancelling twice: %v", err)
	}
	if ls := e.lease(10); len(ls) != 0 {
		t.Fatalf("leased %d deliveries of a cancelled run", len(ls))
	}
}

func TestCancelErrors(t *testing.T) {
	t.Parallel()
	e := newEnv(t, fixture)
	e.firePipeline("listings", "adhoc", "m1") // settles at once
	wantErr(t, e.s.Cancel(e.ctx, "listings", "m1"), store.ErrConflict)
	wantErr(t, e.s.Cancel(e.ctx, "listings", "nope"), store.ErrNotFound)
}
