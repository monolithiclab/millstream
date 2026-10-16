package sqlite

import (
	"errors"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Review focus 5: a run cancelled while a delivery is leased discards its outputs and ends cancelled with its
// cost.
func TestCancelWhileLeased(t *testing.T) {
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
	base := newEnv(t, fixture)
	t.Run("fail after cancel", func(t *testing.T) {
		e := base.on(t)
		e.fire("r1")
		l := e.lease(1)[0]
		if err := e.s.Cancel(e.ctx, "listings", "r1"); err != nil {
			t.Fatal(err)
		}
		wantState(t, "state", e.fail(l, true), store.DeliveryCancelled)
		wantState(t, "run", e.run("r1").State, store.RunCancelled)
	})
	t.Run("lease expires after cancel", func(t *testing.T) {
		e := base.on(t)
		e.fire("r2")
		e.lease(1)
		if err := e.s.Cancel(e.ctx, "listings", "r2"); err != nil {
			t.Fatal(err)
		}
		e.clock.Advance(2 * time.Minute)
		if refs, err := e.s.ExpireLeases(e.ctx); len(refs) != 1 || err != nil {
			t.Fatalf("ExpireLeases = %v, %v", refs, err)
		}
		if d := e.deliveries("r2")[0]; d.State != store.DeliveryCancelled {
			t.Fatalf("delivery = %s, want cancelled", d.State)
		}
		if r := e.run("r2"); r.SettledAt.IsZero() {
			t.Fatal("run not settled")
		}
	})
	// Cancelling a run discards its open dead letters: a cancelled run is never replayed, so they would stay open
	// for ever and stop a replay of all the pipeline's dead letters.
	t.Run("open dead letters discarded", func(t *testing.T) {
		e := base.on(t)
		e.fire("r3")
		e.fail(e.leaseStep("crawl"), false) // its dead letter waits for alert, so the run is still running
		if err := e.s.Cancel(e.ctx, "listings", "r3"); err != nil {
			t.Fatal(err)
		}
		if dls := e.deadLetters("r3"); len(dls) != 1 || dls[0].State != store.DeadLetterDiscarded {
			t.Fatalf("dead letters = %+v", dls)
		}
		if evs := e.eventKinds(store.EventDeadLetterDiscarded); len(evs) != 1 {
			t.Fatalf("discard events = %d", len(evs))
		}
		if _, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r3", nil, false); !errors.Is(err, store.ErrConflict) {
			t.Fatalf("replaying a cancelled run: %v", err)
		}
	})
}

func TestCancelPendingAndRetrying(t *testing.T) {
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
	e := newEnv(t, fixture)
	e.firePipeline("listings", "adhoc", "m1") // settles at once
	wantErr(t, e.s.Cancel(e.ctx, "listings", "m1"), store.ErrConflict)
	wantErr(t, e.s.Cancel(e.ctx, "listings", "nope"), store.ErrNotFound)
}

func TestOpenRevisions(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	rev2 := e.apply(fixtureFixed)
	e.fire("r2")
	open := func() []store.RevisionID {
		t.Helper()
		ids, err := e.s.OpenRevisions(e.ctx)
		if err != nil {
			t.Fatal(err)
		}
		return ids
	}
	if got := open(); len(got) != 2 || got[0] != 1 || got[1] != rev2 {
		t.Fatalf("OpenRevisions = %v; want [1 %d]", got, rev2)
	}
	if err := e.s.Cancel(e.ctx, "listings", "r1"); err != nil {
		t.Fatal(err)
	}
	if got := open(); len(got) != 1 || got[0] != rev2 {
		t.Fatalf("OpenRevisions after r1 settled = %v", got)
	}
}
