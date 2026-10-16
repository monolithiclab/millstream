package storetest

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlstore"
)

// dedupePipeline is a crawl feeding a dedupe step (unique) and a filter step (gate) side by side.
const dedupePipeline = `
pipeline: dd
triggers:
  adhoc: { manual: {} }
steps:
  crawl: { action: ./actions/crawl, in: trigger.adhoc, out: raw }
  unique: { action: dedupe, in: raw, out: kept, config: { key: item.url, window: 30d } }
  gate: { action: filter, in: raw, out: [passed, audit], config: { expr: "true" } }
  sink: { action: ./actions/sink, in: [kept, passed] }
`

const window30d = 30 * 24 * time.Hour

// leaseOf leases the one due delivery of step in pipeline dd, holding the other steps back.
func (e *env) leaseOf(step string) store.Lease {
	e.t.Helper()
	return e.leaseOnly("dd", []string{"crawl", "unique", "gate", "sink"}, step)
}

// crawlOne fires a run of dd in generation gen whose crawl emits one listing with url, and leases the delivery of
// step that reads it.
func (e *env) crawlOne(runID string, gen int64, url, step string) store.Lease {
	e.t.Helper()
	if _, err := e.s.Fire(e.ctx, store.Firing{Pipeline: "dd", Trigger: "adhoc", Key: runID, RunID: runID,
		Generation: gen}); err != nil {
		e.t.Fatalf("Fire: %v", err)
	}
	e.done(e.leaseOf("crawl"), out("k-"+runID, `{"url":"`+url+`"}`))
	return e.leaseOf(step)
}

func (e *env) dedupe(l store.Lease, key string, window time.Duration) store.DeliveryState {
	e.t.Helper()
	state, err := e.s.Dedupe(e.ctx, store.DedupeRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Key: key, Window: window})
	if err != nil {
		e.t.Fatalf("Dedupe: %v", err)
	}
	return state
}

func (e *env) deliveryOf(runID, step string) store.Delivery {
	e.t.Helper()
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: "dd", RunID: runID})
	if err != nil {
		e.t.Fatal(err)
	}
	for _, d := range ds {
		if d.Step == step {
			return d
		}
	}
	e.t.Fatalf("run %s has no delivery of %s", runID, step)
	return store.Delivery{}
}

func TestFilter(t *testing.T, open Opener) {
	e := newEnv(t, open, dedupePipeline)
	l := e.crawlOne("r1", 0, "x", "gate")
	state, err := e.s.Filter(e.ctx, store.FilterRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Reason: store.FilterReasonExpr})
	if err != nil || state != store.DeliveryFiltered {
		t.Fatalf("Filter = %s, %v", state, err)
	}
	d := e.deliveryOf("r1", "gate")
	if d.State != store.DeliveryFiltered || d.FilterReason != store.FilterReasonExpr || d.LeaseOwner != "" {
		t.Fatalf("delivery = %+v", d)
	}
	evs := e.eventKinds(store.EventDeliveryFiltered)
	if len(evs) != 1 || !jsonHas(t, evs[0].Data, "reason", store.FilterReasonExpr) {
		t.Fatalf("filtered events = %v", evs)
	}
	// Filtering again is a stale report.
	_, err = e.s.Filter(e.ctx, store.FilterRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Reason: "again"})
	wantErr(t, err, store.ErrStaleLease)

	// The run settles once its other deliveries end: filtered is terminal.
	e.dedupe(e.leaseOf("unique"), "x", window30d)
	e.done(e.leaseOf("sink"))
	wantState(t, "run", e.runOf("r1").State, store.RunSucceeded)
}

func TestBuiltinUnitsOnCancelledRun(t *testing.T, open Opener) {
	e := newEnv(t, open, dedupePipeline)
	gate := e.crawlOne("r1", 0, "x", "gate")
	unique := e.leaseOf("unique")
	if err := e.s.Cancel(e.ctx, "dd", "r1"); err != nil {
		t.Fatal(err)
	}
	state, err := e.s.Filter(e.ctx, store.FilterRequest{LeaseRef: gate.Ref(), Attempt: 1, Reason: store.FilterReasonExpr})
	if err != nil || state != store.DeliveryCancelled {
		t.Fatalf("Filter on a cancelled run = %s, %v", state, err)
	}
	wantState(t, "dedupe on a cancelled run", e.dedupe(unique, "x", 0), store.DeliveryCancelled)
	wantState(t, "run", e.runOf("r1").State, store.RunCancelled)
	if !e.runOf("r1").SettledAt.After(time.Time{}) {
		t.Fatal("the cancelled run did not settle")
	}
	if _, err := e.s.Dedupe(e.ctx, store.DedupeRequest{LeaseRef: unique.Ref(), Attempt: 1, Window: -1}); err == nil {
		t.Fatal("a negative window was accepted")
	}
}

// TestDedupeWindow is review focus 1: a key let through 30 days and 1 second ago passes again; one let through 29
// days (or exactly 30 days) ago is a duplicate. The cases share a store, each with keys and runs of its own; the
// default build runs the two either side of the 30-day boundary.
func TestDedupeWindow(t *testing.T, open Opener) {
	tests := []struct {
		name   string
		window time.Duration
		after  time.Duration
		want   store.DeliveryState
		full   bool // the e2e build only: the default build keeps the two cases either side of the boundary
	}{
		{"30 days and 1 second ago passes", window30d, window30d + time.Second, store.DeliveryDone, false},
		{"29 days ago is a duplicate", window30d, 29 * 24 * time.Hour, store.DeliveryFiltered, true},
		{"exactly 30 days ago is a duplicate", window30d, window30d, store.DeliveryFiltered, false},
		{"no window holds for ever", 0, 3650 * 24 * time.Hour, store.DeliveryFiltered, true},
	}
	base := newEnv(t, open, dedupePipeline)
	for i, tt := range tests {
		if tt.full && !fullScale {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			e := base.on(t)
			key, firstRun, secondRun := fmt.Sprint("x", i), fmt.Sprint("w", i, "a"), fmt.Sprint("w", i, "b")
			first := e.crawlOne(firstRun, 0, key, "unique")
			wantState(t, "first", e.dedupe(first, key, tt.window), store.DeliveryDone)
			e.clock.Advance(tt.after)
			second := e.crawlOne(secondRun, 0, key, "unique")
			wantState(t, "second", e.dedupe(second, key, tt.window), tt.want)
			d := e.deliveryOf(secondRun, "unique")
			kept := len(e.itemsIn("dd", firstRun, "kept")) + len(e.itemsIn("dd", secondRun, "kept"))
			switch tt.want {
			case store.DeliveryDone:
				if kept != 2 || d.FilterReason != "" {
					t.Fatalf("kept %d items, reason %q", kept, d.FilterReason)
				}
				// The key now records the second item, seen now: the window restarts from it.
				var firstItem string
				var seenAt sqlstore.TimeCol
				if err := e.s.QueryRow(e.ctx, `SELECT first_item, seen_at FROM dedupe_keys
					WHERE pipeline = 'dd' AND step = 'unique' AND generation = 0 AND key = ?`, key).
					Scan(&firstItem, &seenAt); err != nil {
					t.Fatal(err)
				}
				if firstItem != second.Item.ID || !seenAt.Equal(e.clock.Now()) {
					t.Fatalf("dedupe key = %s %s", firstItem, seenAt)
				}
			default:
				if kept != 1 || d.FilterReason != store.FilterReasonDuplicate {
					t.Fatalf("kept %d items, reason %q", kept, d.FilterReason)
				}
			}
		})
	}
}

func (e *env) runOf(id string) store.Run {
	e.t.Helper()
	r, err := e.s.Run(e.ctx, "dd", id)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func (e *env) itemsOf(stream string) []store.Item {
	e.t.Helper()
	items, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: "dd", Stream: stream})
	if err != nil {
		e.t.Fatal(err)
	}
	return items
}

// jsonHas reports whether the JSON object data has key set to want.
func jsonHas(t *testing.T, data json.RawMessage, key, want string) bool {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m[key] == want
}
