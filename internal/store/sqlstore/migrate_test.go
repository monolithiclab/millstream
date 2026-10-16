package sqlstore

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"testing/fstest"

	_ "modernc.org/sqlite" // the driver of the database Migrate runs on
)

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	migrations := fstest.MapFS{
		"migrations/001_init.sql":  {Data: []byte(`CREATE TABLE a (x INTEGER);`)},
		"migrations/002_more.sql":  {Data: []byte(`CREATE TABLE b (y INTEGER); INSERT INTO b VALUES (1);`)},
		"migrations/README.md":     {Data: []byte(`not a migration`)},
		"migrations/sub/x.sql.txt": {Data: []byte(`nor this`)},
	}
	versions := func() string {
		t.Helper()
		var v string
		if err := db.QueryRowContext(ctx, `SELECT group_concat(version) FROM schema_version`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	for range 2 { // the second pass finds nothing to apply
		if err := Migrate(ctx, db, Dialect{}, migrations); err != nil {
			t.Fatal(err)
		}
		if got := versions(); got != "1,2" {
			t.Fatalf("versions %s, want 1,2", got)
		}
	}

	failing := fstest.MapFS{"migrations/003_bad.sql": {Data: []byte(`CREATE TABLE c (z INTEGER); NOT SQL;`)}}
	for k, v := range migrations {
		failing[k] = v
	}
	if err := Migrate(ctx, db, Dialect{}, failing); err == nil || !strings.Contains(err.Error(), "003_bad.sql") {
		t.Fatalf("a failing migration = %v", err)
	}
	var tables int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name = 'c'`).Scan(&tables); err != nil ||
		tables != 0 || versions() != "1,2" {
		t.Fatalf("a failed migration left table c (%d, %v) or version %s", tables, err, versions())
	}

	if err := Migrate(ctx, db, Dialect{}, fstest.MapFS{"migrations/x_init.sql": {}}); err == nil {
		t.Fatal("a migration without a number applied")
	}
	older := fstest.MapFS{"migrations/001_init.sql": migrations["migrations/001_init.sql"]}
	if err := Migrate(ctx, db, Dialect{}, older); err == nil || !strings.Contains(err.Error(), "newer than this build") {
		t.Fatalf("a database newer than the build = %v", err)
	}
}

// CheckVersion accepts a database at the newest migration's version only, and changes nothing.
func TestCheckVersion(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	migrations := fstest.MapFS{
		"migrations/001_init.sql": {Data: []byte(`CREATE TABLE a (x INTEGER);`)},
		"migrations/002_more.sql": {Data: []byte(`CREATE TABLE b (y INTEGER);`)},
	}
	if err := CheckVersion(ctx, db, migrations); err == nil || !strings.Contains(err.Error(), "has an engine opened") {
		t.Fatalf("an empty database: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE schema_version (version INTEGER NOT NULL); INSERT INTO schema_version VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := CheckVersion(ctx, db, migrations); err == nil || !strings.Contains(err.Error(), "schema version 1, this build's is 2") {
		t.Fatalf("an older database: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO schema_version VALUES (2)`); err != nil {
		t.Fatal(err)
	}
	if err := CheckVersion(ctx, db, migrations); err != nil {
		t.Fatal(err)
	}
	if err := CheckVersion(ctx, db, fstest.MapFS{"migrations/x_bad.sql": {}}); err == nil {
		t.Fatal("a bad migration name: no error")
	}
}
