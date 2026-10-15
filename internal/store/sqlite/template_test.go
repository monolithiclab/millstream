package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"os"
	"testing"
)

var update = flag.Bool("update", false, "rewrite template.db from the migrations")

// The embedded template image is the database the migrations build: same schema, same recorded versions. -update
// rewrites it after a migration changes.
func TestTemplateImage(t *testing.T) {
	db, err := sql.Open("sqlite", "file:/millstream-template-test?vfs=memdb")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	if *update {
		if _, err := db.Exec(`VACUUM`); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile("template.db", serialize(t, db), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	tmpl, err := memoryTemplate()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := describe(t, tmpl), describe(t, db); got != want {
		t.Fatalf("template.db is stale, rewrite it: go test ./internal/store/sqlite -run TestTemplateImage -update"+
			"\ngot:  %s\nwant: %s", got, want)
	}
}

// describe returns a database's schema and recorded migration versions.
func describe(t *testing.T, db *sql.DB) string {
	t.Helper()
	var schema, versions string
	if err := db.QueryRow(`SELECT group_concat(type || ' ' || name || ' ' || coalesce(sql, ''), ';')
		FROM (SELECT type, name, sql FROM sqlite_master ORDER BY type, name)`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT group_concat(version) FROM (SELECT version FROM schema_version ORDER BY version)`).
		Scan(&versions); err != nil {
		t.Fatal(err)
	}
	return schema + "\nversions " + versions
}

func serialize(t *testing.T, db *sql.DB) []byte {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	var image []byte
	if err := conn.Raw(func(dc any) error {
		image, err = dc.(interface{ Serialize() ([]byte, error) }).Serialize()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(image)
}
