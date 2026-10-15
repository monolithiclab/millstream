package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/store"
)

// Batch bounds: a batch takes every queued unit, up to maxBatch, gathering for at most batchWindow. Gathering
// never waits for a unit that is not queued yet, so a lone unit commits at once; under load, units queue while the
// previous batch commits and the next batch takes them together (group commit).
const (
	maxBatch    = 64
	batchWindow = 5 * time.Millisecond
)

// request is one unit waiting for the writer.
type request struct {
	ctx  context.Context
	unit string
	fn   func(*wtx) error
	done chan error
}

// wtx is what a unit sees: the batch transaction and its instant.
type wtx struct {
	ctx context.Context
	tx  *sql.Tx
	now time.Time
	s   *Store
}

// submit queues a unit and waits until the batch holding it commits or fails. Once queued, the unit's outcome is
// always awaited, so a nil error means it committed and an error means it left no trace.
func (s *Store) submit(ctx context.Context, unit string, fn func(*wtx) error) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return store.ErrClosed
	}
	r := &request{ctx: ctx, unit: unit, fn: fn, done: make(chan error, 1)}
	select {
	case s.reqs <- r:
		s.mu.RUnlock()
	case <-ctx.Done():
		s.mu.RUnlock()
		return fmt.Errorf("%s: %w", unit, ctx.Err())
	}
	if err := <-r.done; err != nil {
		return fmt.Errorf("%s: %w", unit, err)
	}
	return nil
}

// writer is the single writer loop. It exits when Close closes reqs, after applying what was queued.
func (s *Store) writer() {
	defer close(s.done)
	defer s.closeStmts()
	for first := range s.reqs {
		batch := []*request{first}
		deadline := time.Now().Add(batchWindow)
	gather:
		for len(batch) < maxBatch && time.Now().Before(deadline) {
			select {
			case r, ok := <-s.reqs:
				if !ok {
					break gather
				}
				batch = append(batch, r)
			default:
				break gather
			}
		}
		results := s.commit(batch)
		for i, r := range batch {
			r.done <- results[i]
		}
		s.prepareUnseen()
	}
}

// maxStmts bounds the prepared statements: the queries are constants, but nothing stops a unit from building one.
const maxStmts = 256

// prepareUnseen prepares the queries the last batch ran unprepared. It runs between batches: the write connection
// belongs to the batch transaction while it lasts. A query that cannot be prepared runs unprepared again next time.
func (s *Store) prepareUnseen() {
	for q := range s.unseen {
		if st, err := s.write.Prepare(q); err == nil {
			s.stmts[q] = st
		}
	}
	clear(s.unseen)
}

func (s *Store) closeStmts() {
	for _, st := range s.stmts {
		_ = st.Close()
	}
}

// stmt returns query prepared for the batch transaction, or nil when it is not prepared yet: the writer prepares it
// after the batch, so SQLite parses a unit's statements once per store rather than once per run.
func (w *wtx) stmt(query string) *sql.Stmt {
	if st, ok := w.s.stmts[query]; ok {
		return w.tx.StmtContext(w.ctx, st)
	}
	if len(w.s.stmts)+len(w.s.unseen) < maxStmts {
		w.s.unseen[query] = true
	}
	return nil
}

// execTx runs a statement in the batch transaction, prepared once it can be.
func (w *wtx) execTx(query string, args ...any) (sql.Result, error) {
	if st := w.stmt(query); st != nil {
		return st.ExecContext(w.ctx, args...)
	}
	return w.tx.ExecContext(w.ctx, query, args...)
}

// commit applies a batch in one transaction and returns each unit's outcome.
func (s *Store) commit(batch []*request) []error {
	results := make([]error, len(batch))
	if err := s.commitBatch(batch, results); err != nil {
		for i := range results {
			if results[i] == nil {
				results[i] = err
			}
		}
	}
	return results
}

// commitBatch runs the units, each under a savepoint, then the failpoints and the commit. A failure that is not a
// single unit's (beginning, a savepoint that cannot be rolled back, committing, a failpoint) is returned and applies
// to every unit without an error of its own.
func (s *Store) commitBatch(batch []*request, results []error) (err error) {
	ctx := context.Background() // a caller's cancellation must not abort the others' transaction
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		if r := recover(); r != nil {
			crash, ok := r.(failpoint.Crash)
			if !ok {
				panic(r)
			}
			err = crash
		}
		if !committed {
			_ = tx.Rollback()
		}
	}()
	now, err := s.clock(ctx, tx)
	if err != nil {
		return err
	}
	for i, r := range batch {
		if err := r.ctx.Err(); err != nil {
			results[i] = err
			continue
		}
		var abort error
		results[i], abort = applyUnit(&wtx{ctx: ctx, tx: tx, now: now, s: s}, r.fn)
		if abort != nil {
			return abort // rolled back whole; units without an error of their own get this one
		}
	}
	for i, r := range batch {
		if results[i] == nil {
			failpoint.Hit(r.unit + ".beforeCommit")
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	for i, r := range batch {
		if results[i] == nil {
			failpoint.Hit(r.unit + ".afterCommit")
		}
	}
	return nil
}

// applyUnit runs one unit under a savepoint, rolling back to it when the unit fails, and returns the unit's error.
// When the savepoint itself cannot be set, rolled back to or released, the transaction's state is unknown: abort is
// set, and the caller must roll the whole batch back.
func applyUnit(w *wtx, fn func(*wtx) error) (err, abort error) {
	if _, err := w.execTx(`SAVEPOINT unit`); err != nil {
		return nil, fmt.Errorf("savepoint: %w", err)
	}
	if err := fn(w); err != nil {
		if _, rbErr := w.execTx(`ROLLBACK TO unit`); rbErr != nil {
			return err, fmt.Errorf("roll back a failed unit (%w): %w", err, rbErr)
		}
		if _, rbErr := w.execTx(`RELEASE unit`); rbErr != nil {
			return err, fmt.Errorf("release a failed unit (%w): %w", err, rbErr)
		}
		return err, nil
	}
	if _, err := w.execTx(`RELEASE unit`); err != nil {
		return nil, fmt.Errorf("release savepoint: %w", err)
	}
	return nil, nil
}

func (w *wtx) exec(query string, args ...any) (sql.Result, error) {
	res, err := w.execTx(query, args...)
	if err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}
	return res, nil
}

// affected runs an update and returns how many rows it changed.
func (w *wtx) affected(query string, args ...any) (int64, error) {
	res, err := w.exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return n, nil
}

func (w *wtx) queryRow(query string, args ...any) *sql.Row {
	if st := w.stmt(query); st != nil {
		return st.QueryRowContext(w.ctx, args...)
	}
	return w.tx.QueryRowContext(w.ctx, query, args...)
}

func (w *wtx) query(query string, args ...any) (*sql.Rows, error) {
	var rows *sql.Rows
	var err error
	if st := w.stmt(query); st != nil {
		rows, err = st.QueryContext(w.ctx, args...)
	} else {
		rows, err = w.tx.QueryContext(w.ctx, query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	return rows, nil
}

// ts is the batch instant as stored.
func (w *wtx) ts() string { return formatTime(w.now) }

// day is the batch instant's UTC day, the key of the day budget (spec section 7.5).
func (w *wtx) day() string { return w.now.UTC().Format(time.DateOnly) }

// event appends an event; its sequence number is the next rowid, in commit order.
func (w *wtx) event(pipeline, runID, kind string, data map[string]any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("encode %s event: %w", kind, err)
	}
	_, err = w.exec(`INSERT INTO events (ts, pipeline, run_id, kind, data) VALUES (?, ?, ?, ?, ?)`,
		w.ts(), pipeline, runID, kind, string(body))
	return err
}

// notFound reports sql.ErrNoRows as store.ErrNotFound naming what was looked up.
func notFound(err error, what string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", what, store.ErrNotFound)
	}
	return fmt.Errorf("%s: %w", what, err)
}
