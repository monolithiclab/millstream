package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/monolithiclab/millstream/internal/store/sqlstore"
)

// SQLite compiles a statement once per connection, and compiling costs more than running most of the store's
// statements: reads run prepared (sqlstore.ReadDB), writes once the writer has prepared them, and a closed Memory
// store hands its connection, emptied, with its statements, to the next one (memoryPool).

// memoryDB is a Memory store's database, as the pool keeps it between stores.
type memoryDB struct {
	write  *sql.DB
	reads  *sqlstore.StmtCache // the read statements, on write
	uri    string
	stmts  map[string]*sql.Stmt // the writer's
	cookie int64                // the schema version (PRAGMA schema_version) as the template restored it
}

// maxIdleMemory bounds the idle connections the pool keeps.
const maxIdleMemory = 8

// memoryPool holds the emptied databases of closed Memory stores.
var memoryPool struct {
	sync.Mutex
	idle []*memoryDB
}

func takeMemory() *memoryDB {
	memoryPool.Lock()
	defer memoryPool.Unlock()
	n := len(memoryPool.idle)
	if n == 0 {
		return nil
	}
	m := memoryPool.idle[n-1]
	memoryPool.idle = memoryPool.idle[:n-1]
	return m
}

// putMemory empties m and keeps it for the next Memory store; it reports false, keeping nothing, when the pool is
// full or m's schema changed since the template was restored (a test renamed a table, say).
func putMemory(m *memoryDB) bool {
	if emptyMemory(m) != nil {
		return false
	}
	memoryPool.Lock()
	defer memoryPool.Unlock()
	if len(memoryPool.idle) >= maxIdleMemory {
		return false
	}
	memoryPool.idle = append(memoryPool.idle, m)
	return true
}

// templateTables lists the template's tables but schema_version, which every store keeps as migrated, in an order
// that deletes a row before any it references: emptying with foreign keys deferred instead would expire every
// prepared statement, as a flag pragma does.
var templateTables = sync.OnceValues(func() ([]string, error) {
	db, err := memoryTemplate()
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(`SELECT m.name, coalesce(f."table", '') FROM sqlite_master m
		LEFT JOIN pragma_foreign_key_list(m.name) f WHERE m.type = 'table' AND m.name != 'schema_version'`)
	if err != nil {
		return nil, err
	}
	referrers := map[string]map[string]bool{} // table → the other tables referencing it
	for rows.Next() {
		var name, parent string
		if err := rows.Scan(&name, &parent); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if referrers[name] == nil {
			referrers[name] = map[string]bool{}
		}
		if parent != "" && parent != name {
			if referrers[parent] == nil {
				referrers[parent] = map[string]bool{}
			}
			referrers[parent][name] = true
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var order []string
	for len(referrers) > 0 {
		var next []string
		for t, refs := range referrers {
			if len(refs) == 0 {
				next = append(next, t)
			}
		}
		if len(next) == 0 {
			return nil, errors.New("template tables: a foreign key cycle")
		}
		slices.Sort(next)
		for _, t := range next {
			delete(referrers, t)
			for _, refs := range referrers {
				delete(refs, t)
			}
		}
		order = append(order, next...)
	}
	return order, nil
})

// emptyMemory deletes every row but schema_version's, in one transaction, once it checked the schema did not change
// since the template was restored: any change to it, a table renamed and renamed back included, moves the schema
// version on.
func emptyMemory(m *memoryDB) error {
	tables, err := templateTables()
	if err != nil {
		return err
	}
	version, err := schemaVersion(m.write)
	if err != nil {
		return err
	}
	if version != m.cookie {
		return errors.New("empty memory store: the schema changed")
	}
	deletes := make([]*sql.Stmt, len(tables))
	for i, t := range tables { // before the transaction, which holds the only connection
		q := `DELETE FROM "` + strings.ReplaceAll(t, `"`, `""`) + `"`
		if deletes[i], err = m.prepare(q); err != nil {
			return fmt.Errorf("empty memory store: %w", err)
		}
	}
	tx, err := m.write.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, st := range deletes {
		if _, err := tx.Stmt(st).Exec(); err != nil {
			return fmt.Errorf("empty memory store: %w", err)
		}
	}
	return tx.Commit()
}

// prepare returns query prepared on m, from the writer's statements.
func (m *memoryDB) prepare(query string) (*sql.Stmt, error) {
	if st := m.stmts[query]; st != nil {
		return st, nil
	}
	st, err := m.write.Prepare(query)
	if err != nil {
		return nil, err
	}
	m.stmts[query] = st
	return st, nil
}

// schemaVersion reads db's schema version: a query pragma, which expires no prepared statement.
func schemaVersion(db *sql.DB) (int64, error) {
	var v int64
	err := db.QueryRow(`PRAGMA schema_version`).Scan(&v)
	return v, err
}
