package sqlstore

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// createTable matches a CREATE TABLE statement of the migrations, its body ending at a line that opens with ");".
var createTable = regexp.MustCompile(`(?is)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?\s*\((.*?)\n\);`)

// tableConstraints are the first words of a CREATE TABLE body's entries that are not columns.
var tableConstraints = []string{"primary", "foreign", "unique", "check", "constraint", "exclude"}

// schemaColumns parses the CREATE TABLE statements of a dialect's migrations into each table's column names.
// Comments are dropped and a body is split at its top-level commas; an entry's first word names its column.
func schemaColumns(dir string) (map[string][]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("migrations in %s: %v, %d files", dir, err, len(files))
	}
	var sql strings.Builder
	for _, f := range files {
		b, err := os.ReadFile(f) // #nosec G304 -- the repository's migrations
		if err != nil {
			return nil, err
		}
		for line := range strings.Lines(string(b)) {
			if i := strings.Index(line, "--"); i >= 0 {
				line = line[:i] + "\n"
			}
			sql.WriteString(line)
		}
	}
	tables := map[string][]string{}
	for _, m := range createTable.FindAllStringSubmatch(sql.String(), -1) {
		var cols []string
		depth, start, body := 0, 0, m[2]+","
		for i, r := range body {
			switch r {
			case '(':
				depth++
			case ')':
				depth--
			case ',':
				if depth > 0 {
					continue
				}
				if fields := strings.Fields(body[start:i]); len(fields) > 0 {
					name := strings.ToLower(strings.Trim(fields[0], `"`))
					if !slices.Contains(tableConstraints, name) {
						cols = append(cols, name)
					}
				}
				start = i + 1
			}
		}
		tables[strings.ToLower(m[1])] = cols
	}
	return tables, nil
}

// The migrations, parsed once as the package initialises: the parsing is not what the test checks.
var (
	liteSchema, liteSchemaErr = schemaColumns(filepath.Join("..", "sqlite", "migrations"))
	pgSchema, pgSchemaErr     = schemaColumns(filepath.Join("..", "postgres", "migrations"))
)

// postgresOnly are the tables of Postgres alone: leadership is the standby's lock (spec section 5.9), which SQLite
// has no use for.
var postgresOnly = []string{"leadership"}

// The SQLite and Postgres migrations hold the same tables with the same columns, which the shared units read and
// write alike; the types differ. rowid is SQLite's implicit key, which Postgres declares.
func TestSchemaParity(t *testing.T) {
	if liteSchemaErr != nil || pgSchemaErr != nil {
		t.Fatal(liteSchemaErr, pgSchemaErr)
	}
	lite, pg := liteSchema, maps.Clone(pgSchema)
	for _, table := range postgresOnly {
		if _, ok := pg[table]; !ok {
			t.Errorf("postgresOnly names %s, which Postgres's migrations do not create", table)
		}
		delete(pg, table)
	}
	if len(lite) < 10 || len(lite["items"]) < 5 {
		t.Fatalf("parsed %d tables, items with %v: the parser no longer reads the migrations", len(lite), lite["items"])
	}
	diff := func(a, b map[string][]string, aName, bName string) {
		for table, cols := range a {
			other, ok := b[table]
			if !ok {
				t.Errorf("table %s is in %s's migrations, not in %s's", table, aName, bName)
				continue
			}
			for _, c := range cols {
				if c != "rowid" && !slices.Contains(other, c) {
					t.Errorf("column %s.%s is in %s's migrations, not in %s's", table, c, aName, bName)
				}
			}
		}
	}
	diff(lite, pg, "SQLite", "Postgres")
	diff(pg, lite, "Postgres", "SQLite")
}
