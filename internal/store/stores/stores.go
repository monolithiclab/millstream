// Package stores opens the log store the environment file's store key names (spec section 14): SQLite, the default,
// or Postgres. It sits apart from store, which both implementations import.
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
	if cfg.PostgresDSN != nil {
		dsn, err := cfg.PostgresDSN.Value(lookup)
		if err != nil {
			return nil, fmt.Errorf("store.postgres.dsn: %w", err)
		}
		return postgres.Open(ctx, dsn, postgres.Options{})
	}
	path := cfg.Path(defaultPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	return sqlite.Open(path, sqlite.Options{})
}
