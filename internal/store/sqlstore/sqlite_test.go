package sqlstore_test

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
	"github.com/monolithiclab/millstream/internal/store/sqlite/sqlitetest"
	"github.com/monolithiclab/millstream/internal/store/sqlstore"
	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// The conformance suite (store/storetest) runs here against SQLite, the default build's store: in this package,
// whose code it covers, so that coverage counts it. The Postgres store runs it under the e2e tag.

// TestMain raises the collector's target: the transpiled SQLite allocates heavily, and the tests trade memory for time.
func TestMain(m *testing.M) {
	debug.SetGCPercent(400)
	// The tests run one at a time, and every unit hands off to the store's writer goroutine and back: with one
	// processor the hand-off is a goroutine switch, not an idle thread to wake (the package took 107 ms instead of 93).
	if !raceEnabled {
		runtime.GOMAXPROCS(1)
	}
	sqlitetest.Warm()
	warmLaterUnits()
	// A warmed connection more for each test that changes the schema (schemaChanging): Warm runs while the others
	// are held, so it warms a connection of its own, and all go back to the pool.
	var held []*sqlite.Store
	for range schemaChanging {
		if s, err := sqlite.Open(sqlite.Memory, sqlite.Options{}); err == nil {
			held = append(held, s)
			sqlitetest.Warm()
		}
	}
	for _, s := range held {
		_ = s.Close()
	}
	os.Exit(m.Run())
}

// warmPipeline is the pipeline warmLaterUnits runs: a step whose outputs another reads.
const warmPipeline = `
pipeline: warm2
triggers:
  adhoc: { manual: {} }
steps:
  crawl: { action: ./actions/crawl, in: trigger.adhoc, out: listings }
  score: { action: ./actions/score, in: listings }
`

// warmLaterUnits runs, on the connection Warm left in the pool, the units Warm leaves out whose first statements cost
// the most (the rollup, a replay, a purge): the process's set-up, not the first test's to pay. Failures are ignored:
// the tests then pay it and report what fails.
func warmLaterUnits() {
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

func init() {
	sqlstore.OpenForTest = func(t *testing.T, opts sqlstore.Options) *sqlstore.Store {
		t.Helper()
		s, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: opts.Now, Rand: opts.Rand})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		})
		return s.Store
	}
}

// schemaChanging names the tests whose stores change the schema (they rename a table), so never go back to the
// Memory pool.
var schemaChanging = map[string]bool{"TestUnitsSurfaceStoreErrors": true, "TestAuditsSurfaceStoreErrors": true}

// memoryStore is the conformance suite's opener: a Memory store, which reuses a closed one's connection and its prepared
// statements, where a clone would prepare them all again. For a schemaChanging test it first holds, until the test
// ends, the pool's latest connection, which the tests before left with their statements prepared: the test takes
// the spare connection TestMain warmed, and the tests after it keep the other (the package's other tests took 3 ms
// longer when it was lost).
func memoryStore(t *testing.T, opts storetest.Options) storetest.Store {
	t.Helper()
	if top, _, _ := strings.Cut(t.Name(), "/"); schemaChanging[top] {
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

// TestConformanceListed checks that conformance_test.go and conformance_e2e_test.go run each test of
// storetest.Tests, the e2e file's under the e2e tag only.
func TestConformanceListed(t *testing.T) {
	var got []string
	for _, f := range conformanceFiles {
		src, err := os.ReadFile(f) // #nosec G304 -- the package's own source
		if err != nil {
			t.Fatal(err)
		}
		for rest := string(src); ; {
			i := strings.Index(rest, "\nfunc Test")
			if i < 0 {
				break
			}
			rest = rest[i+len("\nfunc "):]
			name, body, _ := strings.Cut(rest, "(t *testing.T) {\n\tstoretest.")
			if called, _, _ := strings.Cut(body, "(t, memoryStore)\n}"); called != name {
				t.Errorf("%s: %s does not run storetest.%s(t, memoryStore) alone", f, name, name)
			}
			got = append(got, name)
		}
	}
	var want []string
	for _, tc := range storetest.Tests {
		want = append(want, tc.Name)
	}
	slices.Sort(got)
	slices.Sort(want)
	for _, name := range want {
		if !slices.Contains(got, name) {
			t.Errorf("no wrapper runs storetest.%s", name)
		}
	}
	for _, name := range got {
		if !slices.Contains(want, name) {
			t.Errorf("%s is not in storetest.Tests, or not in this build", name)
		}
	}
}

// conformanceFiles are the files holding the build's wrappers; conformance_e2e_test.go adds itself.
var conformanceFiles = []string{"conformance_test.go"}
