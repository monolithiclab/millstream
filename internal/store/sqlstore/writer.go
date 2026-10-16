package sqlstore

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
	// lastSeq is the sequence number of the last event the unit recorded, zero for none.
	lastSeq int64
	// chain holds the pipelines an item is being copied through by the Stream import unit (importCopies).
	chain []string
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

// writer is the single writer loop. It exits when Stop closes reqs, after applying what was queued.
func (s *Store) writer() {
	defer close(s.done)
	var tick <-chan time.Time
	if s.cfg.Check != nil && s.cfg.CheckEvery > 0 {
		t := time.NewTicker(s.cfg.CheckEvery)
		defer t.Stop()
		tick = t.C
	}
	for {
		var first *request
		select {
		case r, ok := <-s.reqs:
			if !ok {
				return
			}
			first = r
		case <-tick:
			s.check(nil)
			continue
		}
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
		s.prepareUnseen() // before the replies: once a unit returns, the writer is idle
		for i, r := range batch {
			r.done <- results[i]
		}
	}
}

// check asks Config.Check whether the write connection still works, once a batch failed as a whole (failed) or
// while idle (nil), and marks it lost for good when it does not.
func (s *Store) check(failed error) {
	if s.cfg.Check == nil || s.lost != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	err := s.cfg.Check(ctx)
	if err == nil {
		return
	}
	if failed != nil {
		err = fmt.Errorf("%w (after %w)", err, failed)
	}
	s.lost = fmt.Errorf("%w: %w", store.ErrWriteLost, err)
	close(s.lostCh)
}

// checkTimeout bounds one Config.Check.
const checkTimeout = 5 * time.Second

// prepareUnseen prepares the queries the last batch ran unprepared. It runs between batches: the write connection
// belongs to the batch transaction while it lasts. A query that cannot be prepared runs unprepared again next time.
func (s *Store) prepareUnseen() {
	for q := range s.unseen {
		if st, err := s.cfg.Write.PrepareContext(context.Background(), q); err == nil {
			s.stmts[q] = st
		}
	}
	clear(s.unseen)
}

// stmt returns query prepared for the batch transaction, or nil when it is not prepared yet: the writer prepares it
// after the batch, so SQLite parses a unit's statements once per store rather than once per run.
func (w *wtx) stmt(query string) *sql.Stmt {
	if !w.s.d.Prepare {
		return nil
	}
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
	query = w.s.rebind(query)
	if st := w.stmt(query); st != nil {
		return st.ExecContext(w.ctx, args...)
	}
	return w.tx.ExecContext(w.ctx, query, args...)
}

// commit applies a batch in one transaction and returns each unit's outcome.
func (s *Store) commit(batch []*request) []error {
	results := make([]error, len(batch))
	if s.lost != nil {
		for i := range results {
			results[i] = s.lost
		}
		return results
	}
	if err := s.commitBatch(batch, results); err != nil {
		if _, crash := errors.AsType[failpoint.Crash](err); !crash {
			s.check(err)
		}
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
	tx, err := s.cfg.Write.BeginTx(ctx, nil)
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
	var lastSeq int64 // the batch's last event: units apply in order, so a later unit's events come after
	for i, r := range batch {
		if err := r.ctx.Err(); err != nil {
			results[i] = err
			continue
		}
		var abort error
		w := &wtx{ctx: ctx, tx: tx, now: now, s: s}
		results[i], abort = applyUnit(w, r.fn)
		if abort != nil {
			return abort // rolled back whole; units without an error of their own get this one
		}
		if results[i] == nil && w.lastSeq > 0 {
			lastSeq = w.lastSeq // a unit rolled back to its savepoint gave its numbers back
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
	if lastSeq > 0 {
		if fn := s.onCommit.Load(); fn != nil {
			(*fn)(lastSeq)
		}
	}
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
func applyUnit(w *wtx, fn func(*wtx) error) (err, abort error) { return savepoint(w, "unit", fn) }

// savepoint runs fn under the named savepoint, rolling back to it when fn fails, and returns fn's error; abort is
// set when the savepoint itself fails. A unit uses a nested one to give up part of its work and keep the rest.
func savepoint(w *wtx, name string, fn func(*wtx) error) (err, abort error) {
	if _, err := w.execTx(`SAVEPOINT ` + name); err != nil {
		return nil, fmt.Errorf("savepoint: %w", err)
	}
	seq := w.lastSeq
	if err := fn(w); err != nil {
		w.lastSeq = seq // the events fn recorded are rolled back
		if _, rbErr := w.execTx(`ROLLBACK TO ` + name); rbErr != nil {
			return err, fmt.Errorf("roll back a failed unit (%w): %w", err, rbErr)
		}
		if _, rbErr := w.execTx(`RELEASE ` + name); rbErr != nil {
			return err, fmt.Errorf("release a failed unit (%w): %w", err, rbErr)
		}
		return err, nil
	}
	if _, err := w.execTx(`RELEASE ` + name); err != nil {
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
	query = w.s.rebind(query)
	if st := w.stmt(query); st != nil {
		return st.QueryRowContext(w.ctx, args...)
	}
	return w.tx.QueryRowContext(w.ctx, query, args...)
}

func (w *wtx) query(query string, args ...any) (*sql.Rows, error) {
	var rows *sql.Rows
	var err error
	query = w.s.rebind(query)
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

// event appends an event. Its sequence number follows the last one committed: the single writer appends in commit
// order, and a unit rolled back to its savepoint gives its numbers back, so the sequence has no gaps.
func (w *wtx) event(pipeline, runID, kind string, data map[string]any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("encode %s event: %w", kind, err)
	}
	if w.lastSeq, err = w.insertKey(`INSERT INTO events (seq, ts, pipeline, run_id, kind, data)
		VALUES ((SELECT COALESCE(MAX(seq), 0) + 1 FROM events), ?, ?, ?, ?, ?)`, "seq",
		w.ts(), pipeline, runID, kind, string(body)); err != nil {
		return fmt.Errorf("append %s event: %w", kind, err)
	}
	return nil
}

// insertKey runs an insert into a table whose integer key is key, and returns the key of the row inserted: through
// the driver's last insert id where the dialect has one (SQLite, where it is the row's key: reading a RETURNING row
// back cost a unit about 4.5 µs of its 22, measured on SetPaused), RETURNING otherwise.
func (w *wtx) insertKey(query, key string, args ...any) (int64, error) {
	var id int64
	if !w.s.d.LastInsertID {
		err := w.queryRow(query+` RETURNING `+key, args...).Scan(&id)
		return id, err
	}
	res, err := w.exec(query, args...)
	if err == nil {
		id, err = res.LastInsertId()
	}
	return id, err
}

// notFound reports sql.ErrNoRows as store.ErrNotFound naming what was looked up.
func notFound(err error, what string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", what, store.ErrNotFound)
	}
	return fmt.Errorf("%s: %w", what, err)
}
