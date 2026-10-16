package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"runtime/debug"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// TestMain raises the collector's target: the transpiled SQLite allocates heavily, and the tests trade memory for time.
func TestMain(m *testing.M) {
	debug.SetGCPercent(400)
	warm()
	os.Exit(m.Run())
}

// warm runs a unit or two on a Memory store, so that what the process does once (SQLite's initialisation, the
// template's load, the units' first statements, parsing a pipeline) is not counted to its first test.
func warm() {
	s, err := Open(Memory, Options{})
	if err != nil {
		return
	}
	defer func() { _ = s.Close() }()
	if p, err := pipeline.Parse([]byte(fixture)); err == nil {
		ctx := context.Background()
		_, _ = s.ApplyRevision(ctx, store.PackageRecord{Hash: hashOf(fixture), Pipeline: p})
		_, _ = s.Fire(ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: "warm", RunID: "warm"})
	}
}

// fixture is a pipeline for the tests of opening and reusing stores.
const fixture = `
pipeline: listings
triggers:
  daily: { cron: "0 6 * * *" }
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.daily
    out: [listings.raw]
  score:
    action: ./actions/score
    in: listings.raw
`

var t0 = time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)

func parse(t *testing.T, src string) *pipeline.Pipeline {
	t.Helper()
	p, err := pipeline.Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return p
}

func hashOf(src string) string {
	sum := sha256.Sum256([]byte(src))
	return hex.EncodeToString(sum[:])
}

// newStore opens a Memory store at t0 with the fixture applied.
func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Memory, Options{Now: func() time.Time { return t0 }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	apply(t, s)
	return s
}

func apply(t *testing.T, s store.Store) {
	t.Helper()
	if _, err := s.ApplyRevision(context.Background(), store.PackageRecord{Hash: hashOf(fixture), Bundle: []byte(fixture),
		Pipeline: parse(t, fixture)}); err != nil {
		t.Fatalf("ApplyRevision: %v", err)
	}
}

func fire(t *testing.T, s store.Store, runID string) {
	t.Helper()
	if created, err := s.Fire(context.Background(), store.Firing{Pipeline: "listings", Trigger: "daily", Key: runID,
		RunID: runID}); err != nil || !created {
		t.Fatalf("Fire %s: %v, %v", runID, created, err)
	}
}

func wantState[S comparable](t *testing.T, what string, got, want S) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

// errReader fails every read, as a write cut short would.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("disk full") }
