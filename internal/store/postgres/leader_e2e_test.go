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
	untilWaiting(t, pgtest.Database(t, dsn))
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
	untilWaiting(t, pgtest.Database(t, dsn))
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
	untilWaiting(t, pgtest.Database(t, dsn))
	terminate(t, pgtest.Database(t, dsn))

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
	terminate(t, pgtest.Database(t, dsn))
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

// Each engine taking the lock records its acquisition with the next epoch; a store without an engine id (a
// command's --direct store) records nothing. Engines lists the engine holding the lock with its row, and the
// engines waiting for it, but not a command's session.
func TestLeadershipAndEngines(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	a := openStore(t, ctx, dsn, postgres.Options{Engine: postgres.Engine{ID: "mill-a", Version: "v1", Address: "10.0.0.1:7070"}})
	if l := a.Leadership(); l.Epoch != 1 || l.Engine.ID != "mill-a" || l.Since.IsZero() {
		t.Fatalf("first leadership %+v", l)
	}
	es, err := a.Engines(ctx)
	if err != nil || len(es) != 1 || !es[0].Active || es[0].Epoch != 1 || es[0].Address != "10.0.0.1:7070" ||
		es[0].Version != "v1" {
		t.Fatalf("engines with one leader: %+v, %v", es, err)
	}
	type opened struct {
		s   *postgres.Store
		err error
	}
	waitOpen := func(opts postgres.Options) chan opened {
		c := make(chan opened, 1)
		go func() {
			s, err := postgres.Open(ctx, dsn, opts)
			c <- opened{s, err}
		}()
		untilWaiting(t, pgtest.Database(t, dsn))
		return c
	}
	b := waitOpen(postgres.Options{Engine: postgres.Engine{ID: "mill-b", Version: "v2"}})
	es, err = a.Engines(ctx)
	if err != nil || len(es) != 2 || es[0].ID != "mill-a" || es[1].ID != "mill-b" || es[1].Active ||
		es[1].Epoch != 0 || es[1].Since.IsZero() || es[1].Version != "v2" {
		t.Fatalf("engines with a standby: %+v, %v", es, err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	o := <-b
	if o.err != nil {
		t.Fatal(o.err)
	}
	if l := o.s.Leadership(); l.Epoch != 2 || l.Engine.ID != "mill-b" {
		t.Fatalf("second leadership %+v", l)
	}
	// A read-only store sees the same, and so does the standby's.
	ro, err := postgres.OpenReadOnly(ctx, dsn, postgres.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	if es, err := ro.Engines(ctx); err != nil || len(es) != 1 || es[0].ID != "mill-b" || es[0].Epoch != 2 {
		t.Fatalf("engines read-only: %+v, %v", es, err)
	}
	// A command's store takes the lock without an epoch, and is no engine.
	if err := o.s.Close(); err != nil {
		t.Fatal(err)
	}
	direct := openStore(t, ctx, dsn, postgres.Options{})
	if direct.Leadership().Epoch != 0 {
		t.Fatalf("a command's store led: %+v", direct.Leadership())
	}
	if es, err := ro.Engines(ctx); err != nil || len(es) != 0 {
		t.Fatalf("engines while a command holds the lock: %+v, %v", es, err)
	}
	var epoch int64
	if err := ro.QueryRow(ctx, `SELECT epoch FROM leadership`).Scan(&epoch); err != nil || epoch != 2 {
		t.Fatalf("epoch after a command's store = %d, %v", epoch, err)
	}
}

// The write session is watched from the server's end with the store's periods: keepalives every k after k idle,
// two probes, the user timeout and the transaction timeout at 3k, and silence ended after CheckEvery + 2k.
func TestWriteSessionWatched(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, pgtest.DSN(t), postgres.Options{CheckEvery: 2 * time.Second, Keepalive: 3 * time.Second})
	if err := s.Exec(ctx, `INSERT INTO projections (name, seq) SELECT name, CAST(setting AS bigint) FROM pg_settings
		WHERE name IN ('tcp_keepalives_idle', 'tcp_keepalives_interval', 'tcp_keepalives_count', 'tcp_user_timeout',
			'idle_session_timeout', 'idle_in_transaction_session_timeout')`); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]int64{
		"tcp_keepalives_idle": 3, "tcp_keepalives_interval": 3, "tcp_keepalives_count": 2, "tcp_user_timeout": 9000,
		"idle_session_timeout": 8000, "idle_in_transaction_session_timeout": 9000,
	} {
		var got int64
		if err := s.QueryRow(ctx, `SELECT seq FROM projections WHERE name = ?`, name).Scan(&got); err != nil ||
			got != want {
			t.Errorf("%s = %d, %v; want %d", name, got, err, want)
		}
	}
}
