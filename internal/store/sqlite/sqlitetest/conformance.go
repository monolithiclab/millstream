package sqlitetest

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// Main runs the tests of a binary that opens Memory stores, and exits. It raises the collector's target (the
// transpiled SQLite allocates heavily, and the tests trade memory for time) and keeps one processor outside the race
// detector: the tests run one at a time, and every unit hands off to the store's writer goroutine and back, which
// with one processor is a goroutine switch, not an idle thread to wake. It then warms the process (Warm, warmLater)
// and a connection more for each test of storetest.SchemaChanging: Memory has those tests take that spare
// connection, so the others keep the one the tests before left with their statements prepared.
func Main(m *testing.M) {
	debug.SetGCPercent(400)
	if !raceEnabled {
		runtime.GOMAXPROCS(1)
	}
	Warm()
	warmLater()
	// Warm runs while the others are held, so it warms a connection of its own, and all go back to the pool.
	var held []*sqlite.Store
	for range storetest.SchemaChanging {
		if s, err := sqlite.Open(sqlite.Memory, sqlite.Options{}); err == nil {
			held = append(held, s)
			Warm()
		}
	}
	for _, s := range held {
		_ = s.Close()
	}
	os.Exit(m.Run())
}

// Memory is the conformance suite's opener: a Memory store, which reuses a closed one's connection and its prepared
// statements, where a clone would prepare them all again. For a test of storetest.SchemaChanging (the subtest
// storetest.RunArea names after it), it first holds, until the test ends, the pool's latest connection, which the
// tests before left with their statements prepared: the test takes the spare connection Main warmed, and the tests
// after it keep the other. A binary using it runs Main.
func Memory(t *testing.T, opts storetest.Options) storetest.Store {
	t.Helper()
	if name := strings.Split(t.Name(), "/"); len(name) > 1 && slices.Contains(storetest.SchemaChanging, name[1]) {
		held, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = held.Close() })
	}
	s, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: opts.Now, Rand: opts.Rand})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

// warmPipeline is the pipeline warmLater runs: a step whose outputs another reads.
const warmPipeline = `
pipeline: warm2
triggers:
  adhoc: { manual: {} }
steps:
  crawl: { action: ./actions/crawl, in: trigger.adhoc, out: listings }
  score: { action: ./actions/score, in: listings }
`

// warmLater runs, on the connection Warm left in the pool, the units Warm leaves out whose first statements cost the
// most (the rollup, a replay, a purge): the process's set-up, not the first test's to pay. Failures are ignored: the
// tests then pay it and report what fails.
func warmLater() {
	s, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		return
	}
	defer func() { _ = s.Close() }()
	p, err := pipeline.Parse([]byte(warmPipeline))
	if err != nil {
		return
	}
	ctx := context.Background()
	if _, err := s.ApplyRevision(ctx, store.PackageRecord{Hash: "warm2", Pipeline: p}); err != nil {
		return
	}
	if _, err := s.Fire(ctx, store.Firing{Pipeline: "warm2", Trigger: "adhoc", Key: "r1", RunID: "r1"}); err != nil {
		return
	}
	metric := []store.Metric{{Name: "pages", Value: 1, Labels: map[string]string{"source": "x"}}}
	for range 2 { // crawl, then the first score
		ls, err := s.Lease(ctx, store.LeaseRequest{Owner: "warm", Max: 1, TTL: time.Minute})
		if err != nil || len(ls) != 1 {
			return
		}
		_, _ = s.Done(ctx, store.DoneRequest{LeaseRef: ls[0].Ref(), Attempt: ls[0].Attempt, CostUSD: 0.25,
			Metrics: metric, Outputs: []store.Output{{Key: "a", Payload: json.RawMessage(`{"n":1}`)},
				{Key: "b", Payload: json.RawMessage(`{"n":2}`)}}})
	}
	_, _ = s.Rollup(ctx)
	_, _ = s.MetricRollups(ctx, store.MetricFilter{Pipeline: "warm2", Name: "pages"})
	if items, err := s.Items(ctx, store.ItemFilter{Pipeline: "warm2", Stream: "listings"}); err == nil && len(items) > 0 {
		_, _, _ = s.StartReplay(ctx, store.ReplayStart{Pipeline: "warm2", Step: "score", RunID: "rp1", Key: "rp1",
			Items: []string{items[0].ID}})
	}
	req := store.PurgeRequest{ID: "p1", Key: "a"}
	pg, _, err := s.StartPurge(ctx, req)
	for err == nil && pg.State == store.PurgeErasing {
		pg, err = s.ErasePurge(ctx, req.ID)
	}
	if err == nil && pg.State == store.PurgeSweeping {
		_, _ = s.FinishPurge(ctx, req.ID)
	}
}
