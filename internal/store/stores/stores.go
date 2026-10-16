// Package stores opens the log store the environment file's store key names (spec section 14): SQLite, the default,
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

// Open opens the store cfg names: SQLite at cfg.SQLitePath, or at defaultPath (the --db flag) without a store key,
// creating its directory; or Postgres at the DSN cfg's source holds, read through lookup and never part of an
// error. Opening Postgres takes the deployment's leadership lock, waiting while another engine holds it until ctx
// ends (spec section 5.9).
func Open(ctx context.Context, cfg pipeline.StoreConfig, defaultPath string, lookup func(string) (string, bool)) (
	store.Store, error,
) {
	return OpenLeader(ctx, cfg, defaultPath, lookup, nil)
}

// OpenLeader is Open, calling waiting, when it is set, once Postgres's leadership lock is found held by another
// engine and before waiting for it: serve logs that it stands by.
func OpenLeader(ctx context.Context, cfg pipeline.StoreConfig, defaultPath string, lookup func(string) (string, bool),
	waiting func(),
) (store.Store, error) {
	return open(cfg, defaultPath, lookup, func(dsn string) (store.Store, error) {
		return postgres.Open(ctx, dsn, postgres.Options{Waiting: waiting})
	})
}

// OpenReader opens the store cfg names for a command that only reads: on Postgres without the leadership lock and
// read-only, on SQLite as Open does.
func OpenReader(ctx context.Context, cfg pipeline.StoreConfig, defaultPath string, lookup func(string) (string, bool)) (
	store.Store, error,
) {
	return open(cfg, defaultPath, lookup, func(dsn string) (store.Store, error) {
		return postgres.OpenReadOnly(ctx, dsn, postgres.Options{})
	})
}

// open opens Postgres with pg at the DSN cfg's source holds, else SQLite.
func open(cfg pipeline.StoreConfig, defaultPath string, lookup func(string) (string, bool),
	pg func(dsn string) (store.Store, error),
) (store.Store, error) {
	if cfg.PostgresDSN != nil {
		dsn, err := cfg.PostgresDSN.Value(lookup)
		if err != nil {
			return nil, fmt.Errorf("store.postgres.dsn: %w", err)
		}
		return pg(dsn)
	}
	return openSQLite(cfg, defaultPath)
}

func openSQLite(cfg pipeline.StoreConfig, defaultPath string) (store.Store, error) {
	path := cfg.Path(defaultPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	return sqlite.Open(path, sqlite.Options{})
}
