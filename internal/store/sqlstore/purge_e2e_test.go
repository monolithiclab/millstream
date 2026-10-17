//go:build e2e

package sqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// A purge's first unit reads the rows of its lineage only, whatever the store holds: beside a few thousand other
// items whose parents no query could read (malformed JSON, which reading them fails on), StartPurge fences the
// lineage of its key and nothing else.
func TestStartPurgeReadsItsLineage(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, fixture)
	if _, err := s.Fire(ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: "r1", RunID: "r1"}); err != nil {
		t.Fatal(err)
	}
	ls, err := s.Lease(ctx, store.LeaseRequest{Owner: "n", Max: 1, TTL: time.Minute})
	if err != nil || len(ls) != 1 {
		t.Fatalf("Lease = %+v, %v", ls, err)
	}
	if _, err := s.Done(ctx, store.DoneRequest{LeaseRef: ls[0].Ref(), Attempt: 1,
		Outputs: []store.Output{{Key: "k1"}, {Key: "k2"}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Exec(ctx, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3000)
		INSERT INTO items (pipeline, id, run_id, stream, step, key, parents, generation, ts, payload, payload_hash)
		SELECT 'listings', 'bulk' || i, 'r1', 'listings.raw', 'crawl', 'bulk', 'not json', 1, ?, '{}', 'h' FROM n`,
		t0.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	p, created, err := s.StartPurge(ctx, store.PurgeRequest{ID: "p1", Key: "k1"})
	if err != nil || !created || p.Items != 1 {
		t.Fatalf("StartPurge = %+v, %v, %v; want the one item of k1", p, created, err)
	}
}
