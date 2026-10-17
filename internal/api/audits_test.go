package api

import (
	"net/http"
	"testing"
)

// The audits of the deliveries that read an item (spec section 11.5): one done, agreeing, with both actions'
// outputs; an item without any lists none; an unknown item is not found. An item records what produced it.
func TestItemAudits(t *testing.T) {
	f := newFixture(t)
	for _, q := range []string{
		`UPDATE items SET produced_by = 'fallback' WHERE pipeline = 'listings' AND id = ?`,
		`INSERT INTO audits (pipeline, id, delivery_id, run_id, step, item_id, state, primary_output, fallback_output,
		agreement, cost_usd, created_at, finished_at) VALUES ('listings', 'au_1', 'dl_1', 'r1', 'score', ?, 'done',
		'[{"key":"","payload":{"score":90}}]', '[{"key":"","payload":{"score":90}}]', 1, 0.25,
		'2026-10-15T06:00:00.000Z', '2026-10-15T06:00:01.000Z')`,
	} {
		if err := f.st.Exec(f.ctx, q, f.good.ID); err != nil {
			t.Fatal(err)
		}
	}
	srv := f.server()
	if it := get[Item](t, srv, "/api/v1/pipelines/listings/items/"+f.good.ID); it.ProducedBy != "fallback" {
		t.Errorf("item = %+v", it)
	}
	as := get[[]Audit](t, srv, "/api/v1/pipelines/listings/items/"+f.good.ID+"/audits")
	if len(as) != 1 || as[0].ID != "au_1" || as[0].Agreement == nil || *as[0].Agreement != 1 ||
		string(as[0].Fallback) != `[{"key":"","payload":{"score":90}}]` || as[0].FinishedAt == nil {
		t.Errorf("audits = %+v", as)
	}
	if as := get[[]Audit](t, srv, "/api/v1/pipelines/listings/items/"+f.trigger.ID+"/audits"); len(as) != 0 {
		t.Errorf("audits of an unaudited item = %+v", as)
	}
	wantError(t, do(t, srv, request{target: "/api/v1/pipelines/listings/items/nope/audits"}), http.StatusNotFound,
		codeNotFound)
}
