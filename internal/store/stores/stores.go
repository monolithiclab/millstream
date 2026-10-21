// Package stores opens the log store store.dsn names (spec section 14): SQLite, the default,
// or Postgres. It sits apart from store, which both implementations import.
//
// A store opens in one of two modes. Open is the writer's: on Postgres it takes the deployment's leadership lock,
// which makes its holder the only writer (spec section 5.9), and waits while another holds it; serve, run and the
// commands that change state on SQLite (or on Postgres with --direct) open it so. OpenReader is for commands that
// only read: on Postgres it opens the read pool alone, without the lock, so it never waits for the active engine,
// and its units fail with store.ErrReadOnly; on SQLite it is Open, the file's locking serialising the writers.
package stores

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// Open opens the store dsn names (store.dsn): SQLite at sqlite://<path>, creating its directory, or Postgres at a
// postgres:// URI, which pgx completes from libpq's PG* variables. Opening Postgres takes the deployment's leadership
// lock, waiting while another engine holds it until ctx ends (spec section 5.9).
func Open(ctx context.Context, dsn string) (store.Store, error) {
	return OpenLeader(ctx, dsn, postgres.Options{})
}

// OpenLeader is Open with the options a Postgres store takes the lock under: serve names its engine, the session's
// watch periods, and the callback that logs it stands by while another engine holds the lock.
func OpenLeader(ctx context.Context, dsn string, opts postgres.Options) (store.Store, error) {
	return open(dsn, func() (store.Store, error) { return orNil(postgres.Open(ctx, dsn, opts)) })
}

// OpenReader opens the store dsn names for a command that only reads: on Postgres without the leadership lock and
// read-only, on SQLite as Open does.
func OpenReader(ctx context.Context, dsn string) (store.Store, error) {
	return open(dsn, func() (store.Store, error) { return orNil(postgres.OpenReadOnly(ctx, dsn, postgres.Options{})) })
}

// open opens SQLite when dsn is sqlite://<path>, else Postgres with pg.
func open(dsn string, pg func() (store.Store, error)) (store.Store, error) {
	path, ok := (pipeline.StoreConfig{DSN: dsn}).SQLitePath()
	if !ok {
		return pg()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	return orNil(sqlite.Open(path, sqlite.Options{}))
}

// orNil returns a store that failed to open as a nil interface, never a typed nil a caller would find non-nil.
func orNil[S store.Store](s S, err error) (store.Store, error) {
	if err != nil {
		return nil, err
	}
	return s, nil
}
