//go:build e2e

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
	"github.com/monolithiclab/millstream/internal/store/postgres/pgtest"
)

// The leadership lock (spec section 5.9): one store per deployment holds it on its write connection; another blocks
// in Open until the holder closes or loses its session, and a store whose session dies loses the lock with it.

const waitFor = 30 * time.Second

func openStore(t *testing.T, ctx context.Context, dsn string, opts postgres.Options) *postgres.Store {
	t.Helper()
	s, err := postgres.Open(ctx, dsn, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// waiting reports how many sessions of database wait for an advisory lock.
func waiting(t *testing.T, database string) int {
	t.Helper()
	var n int
	pgtest.Admin(t, func(ctx context.Context, c *pgx.Conn) error {
		return c.QueryRow(ctx, `SELECT COUNT(*) FROM pg_locks l JOIN pg_database d ON d.oid = l.database
			WHERE l.locktype = 'advisory' AND NOT l.granted AND d.datname = $1`, database).Scan(&n)
	})
	return n
}

// untilWaiting waits until a session of database waits for the lock.
func untilWaiting(t *testing.T, database string) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for waiting(t, database) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no session waits for the leadership lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func dbName(t *testing.T, dsn string) string {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return cfg.Database
}

// A standby blocks in Open while the active store holds the lock, and takes over, migrated and writable, once it
// closes. Another deployment on the same database has a lock of its own.
func TestStandbyTakesOverOnClose(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	active := openStore(t, ctx, dsn, postgres.Options{})
	other := openStore(t, ctx, dsn, postgres.Options{Deployment: "other"})
	if other.LockKey() == active.LockKey() {
		t.Fatal("two deployments share a lock")
	}

	type opened struct {
		s   *postgres.Store
		err error
	}
	standby := make(chan opened, 1)
	go func() {
		s, err := postgres.Open(ctx, dsn, postgres.Options{})
		standby <- opened{s, err}
	}()
	untilWaiting(t, dbName(t, dsn))
	select {
	case o := <-standby:
		t.Fatalf("the standby opened while the active store held the lock: %v", o.err)
	default:
	}
	if err := active.SetPaused(ctx, "nope", "", true); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the active store cannot write: %v", err)
	}
	if err := active.Close(); err != nil {
		t.Fatal(err)
	}
	var o opened
	select {
	case o = <-standby:
	case <-time.After(waitFor):
		t.Fatal("the standby did not take over once the active store closed")
	}
	if o.err != nil {
		t.Fatalf("standby Open: %v", o.err)
	}
	defer func() { _ = o.s.Close() }()
	if err := o.s.SetPaused(ctx, "nope", "", true); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the new active store cannot write: %v", err)
	}
}

// A standby's Open gives up when its context ends.
func TestStandbyOpenCancelled(t *testing.T) {
	dsn := pgtest.DSN(t)
	openStore(t, context.Background(), dsn, postgres.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		s, err := postgres.Open(ctx, dsn, postgres.Options{})
		if s != nil {
			_ = s.Close()
		}
		done <- err
	}()
	untilWaiting(t, dbName(t, dsn))
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cancelled standby opened")
		}
	case <-time.After(waitFor):
		t.Fatal("a cancelled standby still waits")
	}
}

// terminate ends the sessions holding the leadership lock in database, as a database failover or a network cut
// would.
func terminate(t *testing.T, database string) {
	t.Helper()
	pgtest.Admin(t, func(ctx context.Context, c *pgx.Conn) error {
		var n int
		err := c.QueryRow(ctx, `SELECT COUNT(pg_terminate_backend(l.pid)) FROM pg_locks l
			JOIN pg_database d ON d.oid = l.database
			WHERE l.locktype = 'advisory' AND l.granted AND d.datname = $1`, database).Scan(&n)
		if err == nil && n != 1 {
			err = errors.New("no session holds the leadership lock")
		}
		return err
	})
}

// A store whose session dies loses the lock: the next unit fails with ErrWriteLost, Lost is closed, and every unit
// after fails without touching the database; the standby takes over.
func TestSessionLostOnWrite(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	active := openStore(t, ctx, dsn, postgres.Options{CheckEvery: time.Hour}) // only a failed write notices
	if active.LostErr() != nil {
		t.Fatal("a new store is lost")
	}
	standby := make(chan error, 1)
	go func() {
		s, err := postgres.Open(ctx, dsn, postgres.Options{})
		if err == nil {
			defer func() { _ = s.Close() }()
			err = s.SetPaused(ctx, "nope", "", true)
		}
		standby <- err
	}()
	untilWaiting(t, dbName(t, dsn))
	terminate(t, dbName(t, dsn))

	if err := active.SetPaused(ctx, "nope", "", true); err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a write on a dead session = %v", err)
	}
	select {
	case <-active.Lost():
	case <-time.After(waitFor):
		t.Fatal("Lost is still open after a write found the session dead")
	}
	if err := active.LostErr(); !errors.Is(err, store.ErrWriteLost) {
		t.Fatalf("LostErr = %v", err)
	}
	if err := active.SetPaused(ctx, "nope", "", true); !errors.Is(err, store.ErrWriteLost) {
		t.Fatalf("a write after the loss = %v, want ErrWriteLost", err)
	}
	select {
	case err := <-standby:
		if !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("the standby, once active: %v", err)
		}
	case <-time.After(waitFor):
		t.Fatal("the standby did not take over the lost lock")
	}
}

// An idle store notices its session died by its periodic check, without a write.
func TestSessionLostWhileIdle(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	active := openStore(t, ctx, dsn, postgres.Options{CheckEvery: 20 * time.Millisecond})
	terminate(t, dbName(t, dsn))
	select {
	case <-active.Lost():
	case <-time.After(waitFor):
		t.Fatal("an idle store did not notice its session died")
	}
	if err := active.LostErr(); !errors.Is(err, store.ErrWriteLost) {
		t.Fatalf("LostErr = %v", err)
	}
}

// The read pool cannot write: only the writer's session, which holds the lock, ever does.
func TestReadPoolIsReadOnly(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, pgtest.DSN(t), postgres.Options{})
	var n int
	if err := s.QueryRow(ctx, `WITH x AS (INSERT INTO projections (name, seq) VALUES ('x', 1) RETURNING seq)
		SELECT seq FROM x`).Scan(&n); err == nil {
		t.Fatal("a write through the read pool succeeded")
	}
}
