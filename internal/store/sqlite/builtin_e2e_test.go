//go:build e2e

package sqlite

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store"
)

// Dedupe variants the e2e build adds: the default build covers the same paths through TestDedupeWindow.

// TestDedupeScopedByGeneration: a key let through in generation 0 does not hold back the same key in generation 1,
// and the reverse.
func TestDedupeScopedByGeneration(t *testing.T) {
	e := newEnv(t, dedupePipeline)
	wantState(t, "generation 0", e.dedupe(e.crawlOne("r1", 0, "x", "unique"), "x", 0), store.DeliveryDone)
	wantState(t, "generation 1", e.dedupe(e.crawlOne("r2", 1, "x", "unique"), "x", 0), store.DeliveryDone)
	wantState(t, "generation 1 again", e.dedupe(e.crawlOne("r3", 1, "x", "unique"), "x", 0), store.DeliveryFiltered)
	wantState(t, "generation 0 again", e.dedupe(e.crawlOne("r4", 0, "x", "unique"), "x", 0), store.DeliveryFiltered)
}

func TestDedupePassPublishesTheItemUnchanged(t *testing.T) {
	e := newEnv(t, dedupePipeline)
	l := e.crawlOne("r1", 0, "x", "unique")
	e.dedupe(l, "x", window30d)
	kept := e.itemsOf("kept")
	if len(kept) != 1 {
		t.Fatalf("kept = %v", kept)
	}
	got := kept[0]
	if got.Key != l.Item.Key || string(got.Payload) != string(l.Item.Payload) || got.Parents[0] != l.Item.ID ||
		got.Step != "unique" || got.Generation != 0 {
		t.Fatalf("kept item = %+v, from %+v", got, l.Item)
	}
	if d := e.deliveryOf("r1", "sink"); d.ItemID != got.ID {
		t.Fatalf("sink delivery reads %s, want %s", d.ItemID, got.ID)
	}
}
