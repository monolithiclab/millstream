package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/monolithiclab/millstream/internal/store"
)

// limitParam is a bound LIMIT clause. SQLite plans a query around a LIMIT bound as a bare parameter, so binding
// that parameter expires the statement, and every run compiles it again (sqlite3_reprepare): an expression keeps
// the value out of the plan, and the statement prepared. Postgres reads it as the same integer.
const limitParam = ` LIMIT ?+0`

// maxStmts bounds the prepared statements: the queries are constants, but nothing stops a unit from building one.
const maxStmts = 256

// ReadDB is a store's read pool. With Dialect.Prepare, it runs each query prepared, keeping the statements; a
// SQLite Memory store's pool goes with its connection to the next store on Close (Hand), leaving this one closed.
type ReadDB struct {
	*sql.DB
	stmts   *StmtCache
	rebind  func(string) string
	prepare bool
	closed  atomic.Bool
	// unavailable is the dialect's (Dialect.Unavailable).
	unavailable func(error) bool
}

// NewReadDB wraps db as the read pool of a store speaking d; stmts holds statements an earlier store prepared on
// db, nil for none.
func NewReadDB(db *sql.DB, d Dialect, stmts *StmtCache) *ReadDB {
	if stmts == nil {
		stmts = &StmtCache{m: map[string]*sql.Stmt{}}
	}
	return &ReadDB{DB: db, stmts: stmts, rebind: d.Rebind, prepare: d.Prepare, unavailable: d.Unavailable}
}

// Row is a row a read returns, whose Scan classifies the error (Dialect.Unavailable).
type Row struct {
	row         *sql.Row
	unavailable func(error) bool
}

// Scan scans the row as sql.Row.Scan does.
func (r *Row) Scan(dest ...any) error { return classify(r.unavailable, r.row.Scan(dest...)) }

// Row runs query as QueryRowContext does, its error classified.
func (r *ReadDB) Row(ctx context.Context, query string, args ...any) *Row {
	return &Row{row: r.QueryRowContext(ctx, query, args...), unavailable: r.unavailable}
}

// StmtCache holds the statements a read pool prepared, up to maxStmts of them.
type StmtCache struct {
	mu sync.Mutex
	m  map[string]*sql.Stmt
}

// Hand marks r closed, its reads failing with store.ErrClosed from now on, and returns its statements for the
// next store on the same connection.
func (r *ReadDB) Hand() *StmtCache {
	r.closed.Store(true)
	return r.stmts
}

// Closed reports whether Hand closed r.
func (r *ReadDB) Closed() bool { return r.closed.Load() }

// stmt returns query prepared, or nil when the dialect does not prepare, the cache is full or the query cannot be
// prepared: the caller runs it unprepared, and gets the error then.
func (r *ReadDB) stmt(ctx context.Context, query string) *sql.Stmt {
	if !r.prepare {
		return nil
	}
	c := r.stmts
	c.mu.Lock()
	st, ok := c.m[query]
	full := len(c.m) >= maxStmts
	c.mu.Unlock()
	if ok || full {
		return st
	}
	st, err := r.PrepareContext(ctx, query) // unlocked: other reads go on while SQLite compiles
	if err != nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev, ok := c.m[query]; ok { // another read prepared it meanwhile
		_ = st.Close()
		return prev
	}
	c.m[query] = st
	return st
}

// closedCtx fails the reads of a closed store whose connection went back to the pool.
var closedCtx = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// QueryContext runs query, prepared when the dialect prepares.
func (r *ReadDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if r.closed.Load() {
		return nil, store.ErrClosed
	}
	if r.rebind != nil {
		query = r.rebind(query)
	}
	var rows *sql.Rows
	var err error
	if st := r.stmt(ctx, query); st != nil {
		rows, err = st.QueryContext(ctx, args...)
	} else {
		rows, err = r.DB.QueryContext(ctx, query, args...)
	}
	return rows, classify(r.unavailable, err)
}

// QueryRowContext runs query, prepared when the dialect prepares.
func (r *ReadDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if r.rebind != nil {
		query = r.rebind(query)
	}
	if r.closed.Load() {
		ctx = closedCtx
	} else if st := r.stmt(ctx, query); st != nil {
		return st.QueryRowContext(ctx, args...)
	}
	return r.DB.QueryRowContext(ctx, query, args...)
}

// CloseStmts closes the statements r prepared.
func (r *ReadDB) CloseStmts() {
	c := r.stmts
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, st := range c.m {
		_ = st.Close()
	}
	clear(c.m)
}

// classify wraps err with store.ErrUnavailable when unavailable reports it so.
func classify(unavailable func(error) bool, err error) error {
	if err != nil && unavailable != nil && unavailable(err) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	return err
}
