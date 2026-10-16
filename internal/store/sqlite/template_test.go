package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"io/fs"
	"os"
	"testing"

	"github.com/monolithiclab/millstream/internal/store/sqlstore"
)

var update = flag.Bool("update", false, "rewrite template.db from the migrations")

// The embedded template image is the database the migrations build: same schema, same recorded versions. -update
// rewrites it after a migration changes, with testdata/template.sum, the digest of the migrations and the image it
// was built from. The default build compares that digest, which costs nothing; the e2e build also migrates a
// database and compares schemas (TestTemplateImageSchema).
func TestTemplateImage(t *testing.T) {
	if *update {
		db := migrated(t)
		if _, err := db.Exec(`VACUUM`); err != nil {
			t.Fatal(err)
		}
		image := serialize(t, db)
		if err := os.WriteFile("template.db", image, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(templateSum, []byte(templateDigest(t, image)+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(templateSum)
	if err != nil {
		t.Fatal(err)
	}
	if got := templateDigest(t, templateImage); got+"\n" != string(want) {
		t.Fatalf("template.db is stale, rewrite it: go test ./internal/store/sqlite -run TestTemplateImage -update")
	}
}

// templateSum holds the digest of the migrations and the template image built from them.
const templateSum = "testdata/template.sum"

// templateDigest is the SHA-256 of each migration's name and content, then the image.
func templateDigest(t *testing.T, image []byte) string {
	t.Helper()
	h := sha256.New()
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names { // Glob sorts
		body, err := migrations.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		h.Write([]byte(name + "\x00"))
		h.Write(body)
		h.Write([]byte{0})
	}
	h.Write(image)
	return hex.EncodeToString(h.Sum(nil))
}

// migrated returns a fresh in-memory database the migrations built.
func migrated(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:/millstream-template-test?vfs=memdb")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if err := sqlstore.Migrate(context.Background(), db, dialect, migrations); err != nil {
		t.Fatal(err)
	}
	return db
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
