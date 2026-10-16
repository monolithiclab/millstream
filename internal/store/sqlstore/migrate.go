package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
)

// Migrator is what Migrate runs on: a *sql.DB or a *sql.Conn.
type Migrator interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// Migrate applies every migration of migrations (migrations/NNN_name.sql, in the database's own DDL) newer than
// the version schema_version records, each in its own transaction with its version. A database recording a
// version newer than any migration was made by another build, or by an older schema since replaced: it is refused
// rather than written to (before the first release the schema changes without migrations, spec section 9.1).
func Migrate(ctx context.Context, db Migrator, d Dialect, migrations fs.FS) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	var current int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&current); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	slices.Sort(names)
	latest := 0
	for _, name := range names {
		base := path.Base(name)
		version, err := strconv.Atoi(strings.SplitN(base, "_", 2)[0])
		if err != nil {
			return fmt.Errorf("migrate: bad migration name %s", base)
		}
		latest = max(latest, version)
		if version <= current {
			continue
		}
		body, err := fs.ReadFile(migrations, name)
		if err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		if err := applyMigration(ctx, db, d, version, string(body)); err != nil {
			return fmt.Errorf("migrate %s: %w", base, err)
		}
	}
	if current > latest {
		return fmt.Errorf("migrate: the database is at schema version %d, newer than this build's %d: it was made by "+
			"another build of millstream; start from a new database", current, latest)
	}
	return nil
}

func applyMigration(ctx context.Context, db Migrator, d Dialect, version int, body string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, body); err != nil {
		return err
	}
	q := `INSERT INTO schema_version (version) VALUES (?)`
	if d.Rebind != nil {
		q = d.Rebind(q)
	}
	if _, err := tx.ExecContext(ctx, q, version); err != nil {
		return err
	}
	return tx.Commit()
}
