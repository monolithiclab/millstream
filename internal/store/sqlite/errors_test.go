package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// TestUnitsSurfaceStoreErrors takes one table offline at a time and runs every unit and read: each either
// succeeds or returns an error, and once the table is back the invariants still hold, so no unit half-applied.
func TestUnitsSurfaceStoreErrors(t *testing.T) {
	t.Parallel()
	tables := []string{"events", "deliveries", "items", "dead_letters", "runs", "revisions", "pipelines",
		"steps_state", "packages"}
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			t.Parallel()
			m := &model{e: newEnv(t, fixture)}
			e := m.e
			// Work in every state: a dead letter of each kind, a held lease, a pending delivery.
			e.fire("r1")
			e.done(e.leaseStep("crawl"), out("a", `{"score":1}`), out("b", `{"score":2}`))
			e.done(e.leaseStep("score"), out("neg", `{"score":-1}`))
			e.fail(e.leaseStep("score"), false)
			e.fire("r2")
			held := e.leaseStep("crawl")
			dls := e.deadLetters("r1")
			e.apply(fixtureFixed)

			setTable(t, e, table, table+"_off")
			errCount := 0
			count := func(err error) {
				if err != nil {
					errCount++
				}
			}
			ctx := context.Background()
			_, err := e.s.ApplyRevision(ctx, store.PackageRecord{Hash: "new", Pipeline: parse(t, fixture)})
			count(err)
			_, err = e.s.Fire(ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: "k", RunID: "r9"})
			count(err)
			_, err = e.s.Lease(ctx, store.LeaseRequest{Owner: "n", Max: 5, TTL: time.Minute})
			count(err)
			_, err = e.s.Renew(ctx, "node1", []store.LeaseRef{held.Ref()}, time.Minute)
			count(err)
			_, err = e.s.Done(ctx, store.DoneRequest{LeaseRef: held.Ref(), Attempt: 1, Outputs: []store.Output{out("a", `{}`)}})
			count(err)
			_, err = e.s.Fail(ctx, store.FailRequest{LeaseRef: held.Ref(), Attempt: 1})
			count(err)
			_, err = e.s.ReplayDeadLetters(ctx, "listings", "r1", nil, true)
			count(err)
			_, err = e.s.DiscardDeadLetters(ctx, "listings", []string{dls[0].ID})
			count(err)
			count(e.s.SetPaused(ctx, "listings", "crawl", true))
			count(e.s.Cancel(ctx, "listings", "r2"))
			e.clock.Advance(time.Hour)
			_, err = e.s.ExpireLeases(ctx)
			count(err)
			for _, read := range []func() error{
				func() error { _, err := e.s.Runs(ctx, store.RunFilter{}); return err },
				func() error { _, err := e.s.Run(ctx, "listings", "r1"); return err },
				func() error { _, err := e.s.Items(ctx, store.ItemFilter{}); return err },
				func() error { _, err := e.s.Deliveries(ctx, store.DeliveryFilter{}); return err },
				func() error { _, err := e.s.DeadLetters(ctx, store.DeadLetterFilter{}); return err },
				func() error { _, err := e.s.Events(ctx, 0, 10); return err },
				func() error { _, err := e.s.ActiveRevision(ctx, "listings"); return err },
				func() error { _, err := e.s.Revision(ctx, 1); return err },
				func() error { _, err := e.s.PipelineState(ctx, "listings"); return err },
			} {
				count(read())
			}
			if errCount == 0 {
				t.Fatalf("no unit or read noticed table %s was missing", table)
			}
			setTable(t, e, table+"_off", table)
			if err := m.check(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func setTable(t *testing.T, e *env, from, to string) {
	t.Helper()
	err := e.s.submit(e.ctx, "test", func(w *wtx) error {
		_, err := w.exec(`ALTER TABLE ` + from + ` RENAME TO ` + to)
		return err
	})
	if err != nil {
		t.Fatalf("rename %s: %v", from, err)
	}
}
