package sqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// The internal tests: what the conformance suite (store/storetest) cannot see from outside the units. They run on
// SQLite: the code they cover does not depend on the database.

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

// newStore opens a store at t0 with the given pipelines applied, each as a package of its own.
func newStore(t *testing.T, pipelines ...string) *Store {
	t.Helper()
	s := OpenForTest(t, Options{Now: func() time.Time { return t0 }})
	for _, src := range pipelines {
		apply(t, s, src)
	}
	return s
}

func apply(t *testing.T, s *Store, src string) store.RevisionID {
	t.Helper()
	p, err := pipeline.Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(src))
	id, err := s.ApplyRevision(context.Background(), store.PackageRecord{Hash: hex.EncodeToString(sum[:]), Pipeline: p})
	if err != nil {
		t.Fatalf("ApplyRevision: %v", err)
	}
	return id
}

func events(t *testing.T, s *Store) []store.Event {
	t.Helper()
	evs, err := s.Events(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

var errUnit = errors.New("unit failed on purpose")

// Revisions are decoded once and cached by package hash: two revisions of one package share the entry.
func TestRevisionCache(t *testing.T) {
	other := fixture + "    retry: { maxAttempts: 2 }\n"
	s := newStore(t, fixture, other, fixture) // revision 3: the package of revision 1
	var first, again, third, second *revision
	err := s.submit(context.Background(), "test", func(w *wtx) error {
		var err error
		for _, step := range []struct {
			id  store.RevisionID
			dst **revision
		}{{1, &first}, {1, &again}, {3, &third}, {2, &second}} {
			if *step.dst, err = w.revision(step.id); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if first != again || first != third || first == second {
		t.Fatalf("cache entries: 1=%p 1=%p 3=%p 2=%p; want 1 and 3 shared, 2 apart", first, again, third, second)
	}
}

func TestUnitsAreNamed(t *testing.T) {
	seen := map[string]bool{}
	for _, u := range Units {
		if u == "" || seen[u] {
			t.Fatalf("unit name %q empty or repeated", u)
		}
		seen[u] = true
	}
	if len(Units) != 27 {
		t.Fatalf("%d units, want 27", len(Units))
	}
}

// A unit whose savepoint cannot be rolled back leaves the transaction in an unknown state: the whole batch is
// aborted, with an error for every unit in it, and nothing commits.
func TestRollbackFailureAbortsBatch(t *testing.T) {
	s := newStore(t, fixture)
	unit := func(fn func(*wtx) error) *request {
		return &request{ctx: context.Background(), unit: "test", fn: fn, done: make(chan error, 1)}
	}
	good := func(w *wtx) error { return w.event("listings", "", "kept?", nil) }
	sabotage := func(w *wtx) error {
		if err := w.event("listings", "", "doomed", nil); err != nil {
			return err
		}
		if _, err := w.exec(`RELEASE unit`); err != nil { // ROLLBACK TO unit now has no savepoint to go back to
			return err
		}
		return errUnit
	}
	results := s.commit([]*request{unit(good), unit(sabotage), unit(good)}) // the writer idles meanwhile
	for i, err := range results {
		if err == nil {
			t.Errorf("unit %d committed", i)
		}
	}
	if !errors.Is(results[1], errUnit) {
		t.Errorf("sabotaged unit: %v, want its own error", results[1])
	}
	if evs := events(t, s); len(evs) != 1 || evs[0].Kind != store.EventRevisionApplied {
		t.Fatalf("events = %+v; want only the revision applied", evs)
	}
}

func TestTimeCol(t *testing.T) {
	paris := time.FixedZone("CEST", 2*3600)
	for _, tt := range []struct {
		in   any
		want time.Time
	}{
		{nil, time.Time{}},
		{"", time.Time{}},
		{"2026-10-15T06:00:00.123Z", t0.Add(123 * time.Millisecond)},
		{[]byte("2026-10-15T06:00:00.000Z"), t0},
		{t0.In(paris), t0},
	} {
		var c TimeCol
		if err := c.Scan(tt.in); err != nil || !c.Equal(tt.want) || c.Location() != time.UTC {
			t.Errorf("Scan(%v) = %v, %v; want %v in UTC", tt.in, c.Time, err, tt.want)
		}
	}
	var c TimeCol
	for _, bad := range []any{"yesterday", 42} {
		if err := c.Scan(bad); err == nil {
			t.Errorf("Scan(%v) succeeded", bad)
		}
	}
	if (TimeCol{}).stored() != "" || (TimeCol{t0}).stored() != "2026-10-15T06:00:00.000Z" {
		t.Error("stored() does not write TSLayout, or empty for none")
	}
	if nullTime(time.Time{}) != nil || nullTime(t0) != "2026-10-15T06:00:00.000Z" {
		t.Error("nullTime does not bind NULL for none and TSLayout otherwise")
	}
}

// brokenConn is a write connection that no longer reaches its database.
type brokenConn struct{}

var errReset = errors.New("connection reset by peer")

func (brokenConn) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)  { return nil, errReset }
func (brokenConn) PrepareContext(context.Context, string) (*sql.Stmt, error) { return nil, errReset }

// A batch that fails as a whole has the writer check its connection: a connection that fails the check is lost for
// good, Lost is closed with the error, and every unit from then on fails with ErrWriteLost without touching the connection.
// A connection that passes the check carries on.
func TestWriteLost(t *testing.T) {
	ctx := context.Background()
	healthy := true
	s := New(Config{Write: brokenConn{}, Read: NewReadDB(nil, Dialect{}, nil), Options: Options{Now: func() time.Time { return t0 }},
		Check: func(context.Context) error {
			if healthy {
				return nil
			}
			return errReset
		}})
	defer s.Stop()
	unit := func() error { return s.Exec(ctx, `SELECT 1`) }

	if err := unit(); !errors.Is(err, errReset) || errors.Is(err, store.ErrWriteLost) || s.LostErr() != nil {
		t.Fatalf("a failed begin with a healthy connection = %v; want the begin's error alone", err)
	}
	healthy = false
	if err := unit(); !errors.Is(err, errReset) {
		t.Fatalf("the batch that finds the connection lost = %v", err)
	}
	for range 2 {
		if err := unit(); !errors.Is(err, store.ErrWriteLost) {
			t.Fatalf("a unit after the loss = %v, want ErrWriteLost", err)
		}
	}
	healthy = true // a connection lost stays lost: its session, and the lock it held, are gone
	if err := unit(); !errors.Is(err, store.ErrWriteLost) {
		t.Fatalf("a unit once the check passes again = %v, want ErrWriteLost", err)
	}
	<-s.Lost()
	if err := s.LostErr(); !errors.Is(err, store.ErrWriteLost) || !errors.Is(err, errReset) {
		t.Fatalf("LostErr = %v; want ErrWriteLost and the check's error", err)
	}
}

// hungConn is a write connection the network silently cut: beginning waits until its context ends.
type hungConn struct{}

func (hungConn) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (hungConn) PrepareContext(ctx context.Context, _ string) (*sql.Stmt, error) {
	return nil, ctx.Err()
}

// A batch bounded by BatchTimeout fails once it is over, rather than waiting on the connection for good, and the
// check that follows is bounded by CheckTimeout: the connection is lost.
func TestBatchTimeout(t *testing.T) {
	var checkDeadline bool
	s := New(Config{Write: hungConn{}, Read: NewReadDB(nil, Dialect{}, nil), Options: Options{Now: func() time.Time { return t0 }},
		BatchTimeout: time.Millisecond, CheckTimeout: time.Hour,
		Check: func(ctx context.Context) error {
			_, checkDeadline = ctx.Deadline()
			return errReset
		}})
	defer s.Stop()
	if err := s.Exec(context.Background(), `SELECT 1`); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a batch on a hung connection = %v", err)
	}
	<-s.Lost()
	if !checkDeadline {
		t.Error("the check had no deadline")
	}
}
