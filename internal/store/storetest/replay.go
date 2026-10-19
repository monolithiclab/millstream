package storetest

import (
	"errors"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// crawled fires run r1 of dd and completes its crawl with listings keyed a and b; it returns their ids on raw.
func (e *env) crawled() []string {
	e.t.Helper()
	if _, err := e.s.Fire(e.ctx, store.Firing{Pipeline: "dd", Trigger: "adhoc", Key: "r1", RunID: "r1"}); err != nil {
		e.t.Fatal(err)
	}
	e.done(e.leaseOf("crawl"), out("a", `{"url":"x"}`), out("b", `{"url":"x"}`))
	var out []string
	for _, it := range e.itemsOf("raw") {
		out = append(out, it.ID)
	}
	return out
}

func (e *env) startReplay(runID, step string, items []string) (int64, bool, error) {
	e.t.Helper()
	return e.s.StartReplay(e.ctx, store.ReplayStart{Pipeline: "dd", Step: step, RunID: runID, Key: runID, Items: items})
}

func TestStartReplay(t *testing.T, open Opener) {
	e := newEnv(t, open, dedupePipeline)
	raw := e.crawled()
	// Generation 0 lets x through once: a passes, b is a duplicate.
	wantState(t, "a", e.dedupe(e.leaseOf("unique"), "x", 0), store.DeliveryDone)
	wantState(t, "b", e.dedupe(e.leaseOf("unique"), "x", 0), store.DeliveryFiltered)
	gen, created, err := e.startReplay("rp1", "unique", raw)
	if err != nil || !created || gen != 1 {
		t.Fatalf("StartReplay = %d, %v, %v", gen, created, err)
	}
	r := e.runOf("rp1")
	if r.Kind != store.RunKindReplay || r.Generation != 1 || r.State != store.RunRunning || r.TriggerKey != "rp1" ||
		r.Revision != 1 {
		t.Fatalf("replay run = %+v", r)
	}
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: "dd", RunID: "rp1"})
	if err != nil || len(ds) != 2 {
		t.Fatalf("deliveries = %v, %v", ds, err)
	}
	for i, d := range ds {
		if d.Step != "unique" || d.ItemID != raw[i] || d.Generation != 1 || d.State != store.DeliveryPending ||
			d.ID != ids.Delivery("unique", raw[i], 1) {
			t.Fatalf("delivery %d = %+v", i, d)
		}
	}
	// The request again: nothing new.
	gen, created, err = e.startReplay("rp1", "unique", nil)
	if err != nil || created || gen != 1 {
		t.Fatalf("StartReplay again = %d, %v, %v", gen, created, err)
	}

	// The replay dedupes in its own generation: a passes although generation 0 let x through, b is a duplicate
	// within the replay, and the outputs are new items carrying the replay run and generation.
	wantState(t, "replayed a", e.dedupe(e.leaseOf("unique"), "x", 0), store.DeliveryDone)
	wantState(t, "replayed b", e.dedupe(e.leaseOf("unique"), "x", 0), store.DeliveryFiltered)
	var replayed []store.Item
	for _, it := range e.itemsOf("kept") {
		if it.RunID == "rp1" {
			replayed = append(replayed, it)
		}
	}
	if len(replayed) != 1 || replayed[0].Generation != 1 || replayed[0].ID != ids.Item("unique", "kept", raw[0], 1, "a", 0) {
		t.Fatalf("replayed outputs = %+v", replayed)
	}
	// Downstream deliveries carry the replay generation.
	if d := e.deliveryOfItem("rp1", replayed[0].ID); d.Generation != 1 || d.Step != "sink" {
		t.Fatalf("downstream delivery = %+v", d)
	}
}

func (e *env) deliveryOfItem(runID, itemID string) store.Delivery {
	e.t.Helper()
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: "dd", RunID: runID})
	if err != nil {
		e.t.Fatal(err)
	}
	for _, d := range ds {
		if d.ItemID == itemID {
			return d
		}
	}
	e.t.Fatalf("no delivery of %s in %s", itemID, runID)
	return store.Delivery{}
}

func TestStartReplayRefusals(t *testing.T, open Opener) {
	e := newEnv(t, open, dedupePipeline)
	raw := e.crawled()
	other := e.apply(otherPipeline)
	trigger := ids.TriggerItem("adhoc", "r1")
	tests := []struct {
		name string
		req  store.ReplayStart
		want error
	}{
		{"missing fields", store.ReplayStart{Pipeline: "dd", Step: "unique"}, store.ErrInvalid},
		{"no item", store.ReplayStart{Pipeline: "dd", Step: "unique", RunID: "x", Key: "x"}, store.ErrInvalid},
		{"unknown pipeline", store.ReplayStart{Pipeline: "nope", Step: "unique", RunID: "x", Key: "x", Items: raw},
			store.ErrNotFound},
		{"revision of another pipeline", store.ReplayStart{Pipeline: "dd", Step: "unique", RunID: "x", Key: "x",
			Items: raw, Revision: other}, store.ErrInvalid},
		{"unknown step", store.ReplayStart{Pipeline: "dd", Step: "nope", RunID: "x", Key: "x", Items: raw},
			store.ErrConflict},
		{"item not on an input stream", store.ReplayStart{Pipeline: "dd", Step: "unique", RunID: "x", Key: "x",
			Items: []string{trigger}}, store.ErrInvalid},
		{"unknown item", store.ReplayStart{Pipeline: "dd", Step: "unique", RunID: "x", Key: "x",
			Items: []string{"it_nope"}}, store.ErrNotFound},
		{"a trigger run's id", store.ReplayStart{Pipeline: "dd", Step: "unique", RunID: "r1", Key: "x", Items: raw},
			store.ErrConflict},
	}
	for _, tt := range tests {
		_, _, err := e.s.StartReplay(e.ctx, tt.req)
		if !errors.Is(err, tt.want) {
			t.Errorf("%s: %v, want %v", tt.name, err, tt.want)
		}
	}
	if _, err := e.s.Run(e.ctx, "dd", "x"); err == nil {
		t.Fatal("a refused replay left a run")
	}
}

// TestReplayRefusesExpiredPayloads: an item whose payload was replaced by a tombstone is never replayed (spec
// section 5.7).
func TestReplayRefusesExpiredPayloads(t *testing.T, open Opener) {
	e := newEnv(t, open, dedupePipeline)
	raw := e.crawled()
	e.expire("dd", raw[1])
	_, _, err := e.startReplay("rp1", "unique", raw)
	wantErr(t, err, store.ErrConflict)
	if !strings.Contains(err.Error(), "expired") {
		t.Fatalf("error %v does not say the payload expired", err)
	}
	if _, err := e.s.Run(e.ctx, "dd", "rp1"); err == nil {
		t.Fatal("the refused replay left a run")
	}
	if _, created, err := e.startReplay("rp2", "unique", raw[:1]); err != nil || !created {
		t.Fatalf("replaying the other item = %v, %v", created, err)
	}
}
