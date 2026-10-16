// Package sqlstore is the log store's implementation over database/sql, shared by the SQLite and Postgres stores
// (spec section 9.1): the writer loop, the atomic units and the reads. Each store opens its databases, migrates
// them and hands sqlstore a write connection, a read pool and a Dialect for what the two databases spell
// differently (placeholders, the clock, JSON arrays); the SQL is otherwise the same.
//
// Writes go through a single writer goroutine that owns the only write connection (D23): units queue on a
// channel, and the writer applies every queued unit (at most 64, gathered for at most 5 ms) in one transaction,
// each under its own savepoint, so a unit that fails is rolled back alone and the others commit. Event sequence
// numbers are assigned in commit order, without gaps.
//
// Clock: every timestamp a batch writes, and every comparison it makes (due retries, expired leases, deadlines),
// uses one instant read when the batch begins. That instant is Options.Now when set (tests), and otherwise the
// database clock (Dialect.Now): lease expiry uses the database clock, never a node's (spec section 5.11). Times are
// stored to the millisecond.
//
// Failpoints: each unit calls failpoint.Hit("<unit>.beforeCommit") before the batch commits and
// failpoint.Hit("<unit>.afterCommit") after, for every unit of the batch in batch order; Units lists the names.
// A failpoint armed in-process makes the whole batch fail with the failpoint.Crash error: rolled back before the
// commit, committed after it.
package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Units names the atomic units, as their failpoints spell them.
var Units = []string{
	unitApplyRevision, unitFire, unitLease, unitRenew, unitDone, unitFail, unitCancel,
	unitReplay, unitDiscard, unitExpireLeases, unitSetPaused, unitSkipFirings, unitFilter, unitDedupe,
	unitStartReplay, unitRecordEval, unitRollup, unitRecordInsights, unitSetInsightState, unitPark,
	unitReviewDecision, unitSampleDecision, unitReviewTimeout, unitDisable, unitRollback,
	unitRegisterNode, unitSetNodeState, unitTriggerTokens, unitRetain, unitStartPurge, unitErasePurge, unitFinishPurge,
	unitDeleteArtifacts,
}

const (
	unitApplyRevision   = "applyRevision"
	unitFire            = "fire"
	unitLease           = "lease"
	unitRenew           = "renew"
	unitDone            = "done"
	unitFail            = "fail"
	unitCancel          = "cancel"
	unitReplay          = "replay"
	unitDiscard         = "discard"
	unitExpireLeases    = "expireLeases"
	unitSetPaused       = "setPaused"
	unitSkipFirings     = "skipFirings"
	unitFilter          = "filter"
	unitDedupe          = "dedupe"
	unitStartReplay     = "startReplay"
	unitRecordEval      = "recordEval"
	unitRollup          = "rollup"
	unitRecordInsights  = "recordInsights"
	unitSetInsightState = "setInsightState"
	unitPark            = "park"
	unitRegisterNode    = "registerNode"
	unitSetNodeState    = "setNodeState"
	unitReviewDecision  = "reviewDecision"
	unitSampleDecision  = "sampleDecision"
	unitReviewTimeout   = "reviewTimeout"
	unitDisable         = "disable"
	unitRollback        = "rollback"
	unitTriggerTokens   = "triggerTokens"
	unitRetain          = "retain"
	unitStartPurge      = "startPurge"
	unitErasePurge      = "erasePurge"
	unitFinishPurge     = "finishPurge"
	unitDeleteArtifacts = "deleteArtifacts"
)

// Dialect is what the SQLite and Postgres stores spell differently under the shared SQL.
type Dialect struct {
	// Rebind rewrites a query's ? placeholders (and ?N) for the driver; nil leaves them as they are.
	Rebind func(query string) string
	// Now is the SQL expression of the database clock: a time, or text in the stored layout.
	Now string
	// JSONEach returns the table expression over the elements of the JSON array expr, a column, with each
	// element's text in a column named value.
	JSONEach func(expr string) string
	// JSONParam returns the subquery selecting the elements of a JSON array bound as one parameter, as integers or
	// as text.
	JSONParam func(integers bool) string
	// LastInsertID says the driver reports an insert's integer key (SQLite's rowid), which the writer reads instead
	// of a RETURNING clause.
	LastInsertID bool
	// Prepare has the writer and the read pool prepare their statements and keep them: SQLite compiles a
	// statement once per connection, which costs more than running most of them. Postgres's driver caches the
	// statements it prepares itself.
	Prepare bool
}

// Conn is the write connection: a *sql.DB holding one connection (SQLite), or a *sql.Conn, one session that
// never reconnects behind the store's back (Postgres, whose session holds the leadership lock).
type Conn interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// Options configures the units. The zero value is the production configuration.
type Options struct {
	// Now replaces the database clock; see the package comment.
	Now func() time.Time
	// Rand returns the retry jitter fraction in [0, 1); nil uses math/rand/v2.
	Rand func() float64
}

// Config is what a store hands New.
type Config struct {
	Dialect Dialect
	Write   Conn
	Read    *ReadDB
	Options Options
	// Stmts are write statements prepared on Write by an earlier store (a SQLite Memory store's pooled
	// connection); New takes them over. Nil starts with none.
	Stmts map[string]*sql.Stmt
	// Check, when set, tells whether the write connection still works. The writer calls it every CheckEvery while
	// idle, and after a batch fails as a whole; once it fails, the write connection is lost for good: Lost is
	// closed, every unit queued or submitted from then on fails with store.ErrWriteLost, and the writer no longer
	// touches the connection.
	Check      func(ctx context.Context) error
	CheckEvery time.Duration
	// CheckTimeout bounds one Check; zero means 5 seconds.
	CheckTimeout time.Duration
	// BatchTimeout, when set, bounds a batch's transaction: a batch that takes longer fails as a whole, and the
	// writer checks the connection. On Postgres it is what notices a write connection the network silently cut
	// while a batch was on it, which TCP alone may take minutes to report (spec section 5.9).
	BatchTimeout time.Duration
	// ReadOnly makes every unit fail with store.ErrReadOnly: a store opened for reading only, Write nil.
	ReadOnly bool
}

// Store applies the units and serves the reads. It is safe for concurrent use. The store that created it owns the
// databases: Stop ends the writer, and the store closes the connections after.
type Store struct {
	d    Dialect
	cfg  Config
	opts Options
	read *ReadDB

	batches batchSizes // the writer's batch sizes, read by WriterBatches

	// onCommit, when set, is told the last event sequence number of each batch that recorded events; see OnCommit.
	onCommit atomic.Pointer[func(lastSeq int64)]

	mu     sync.RWMutex // guards closed against sends on reqs
	closed bool
	reqs   chan *request
	done   chan struct{} // closed when the writer exits

	// Writer goroutine only.
	revs   map[string]*revision // revisions by package hash; see revision
	rrLast string               // the pipeline the round-robin served last
	stmts  map[string]*sql.Stmt // prepared write statements by query; see wtx.stmt
	unseen map[string]bool      // queries of the last batch to prepare once it ends
	lost   error                // set once Check failed: the write connection is gone; lostCh closes after

	lostCh chan struct{} // closed once lost is set

	// leasePassHook, set by tests before the store is used, sees each pass of a Lease unit with its candidates.
	leasePassHook func(queues map[string][]candidate)
}

// New starts the writer of a store on cfg's connections.
func New(cfg Config) *Store {
	s := &Store{d: cfg.Dialect, cfg: cfg, opts: cfg.Options, read: cfg.Read, reqs: make(chan *request),
		done: make(chan struct{}), lostCh: make(chan struct{}), revs: map[string]*revision{}, stmts: cfg.Stmts, unseen: map[string]bool{}}
	if s.stmts == nil {
		s.stmts = map[string]*sql.Stmt{}
	}
	if s.opts.Rand == nil {
		s.opts.Rand = rand.Float64 // #nosec G404 -- retry jitter, not a secret
	}
	go s.writer()
	return s
}

// OnCommit sets fn to be called, on the writer goroutine, after each batch that recorded events commits, with the
// sequence number of the batch's last event: the notifier live event streams wake on (spec section 12.1). fn must
// not block: the writer waits for it. A nil fn stops the calls. Writes another process makes are not seen.
func (s *Store) OnCommit(fn func(lastSeq int64)) {
	if fn == nil {
		s.onCommit.Store(nil)
		return
	}
	s.onCommit.Store(&fn)
}

// Lost is closed when the store loses its write connection for good (Config.Check failed), and on Postgres the
// leadership lock with it: the engine must stop dispatching at once and step down (spec section 5.9). LostErr then
// says why. A store without a Check never closes it.
func (s *Store) Lost() <-chan struct{} { return s.lostCh }

// LostErr returns why the write connection was lost, wrapping store.ErrWriteLost, or nil while it is not.
func (s *Store) LostErr() error {
	select {
	case <-s.lostCh:
		return s.lost // written before lostCh was closed
	default:
		return nil
	}
}

// Stop stops accepting units and waits for the writer to apply the ones already queued. It returns the writer's
// prepared statements, which the caller closes or hands on; a second Stop returns nil.
func (s *Store) Stop() map[string]*sql.Stmt {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.reqs)
	s.mu.Unlock()
	<-s.done
	return s.stmts
}

// Exec runs a statement as a unit of its own, as Unit does.
func (s *Store) Exec(ctx context.Context, query string, args ...any) error {
	return s.Unit(ctx, func(exec func(string, ...any) error) error { return exec(query, args...) })
}

// rebind returns query as the driver spells it.
func (s *Store) rebind(query string) string {
	if s.d.Rebind == nil {
		return query
	}
	return s.d.Rebind(query)
}

// clock returns the batch's instant: Options.Now, or the database clock.
func (s *Store) clock(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	if s.opts.Now != nil {
		return s.opts.Now().UTC().Truncate(time.Millisecond), nil
	}
	var now TimeCol
	if err := tx.QueryRowContext(ctx, `SELECT `+s.d.Now).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read database clock: %w", err)
	}
	return now.Truncate(time.Millisecond), nil
}

// TSLayout is how times are written: fixed width in UTC, so that SQLite's text compares in time order and
// Postgres parses it as a timestamptz.
const TSLayout = "2006-01-02T15:04:05.000Z"

func formatTime(t time.Time) string {
	return t.UTC().Format(TSLayout)
}

// nullTime is t as bound: its text, or NULL for the zero time, an absent instant.
func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return formatTime(t)
}

// TimeCol scans a stored time: SQLite's text in TSLayout, Postgres's timestamptz, or NULL as the zero time.
type TimeCol struct{ time.Time }

// Scan implements sql.Scanner.
func (c *TimeCol) Scan(v any) error {
	switch x := v.(type) {
	case nil:
		c.Time = time.Time{}
	case time.Time:
		c.Time = x.UTC()
	case string:
		return c.parse(x)
	case []byte:
		return c.parse(string(x))
	default:
		return fmt.Errorf("scan a stored time from %T", v)
	}
	return nil
}

func (c *TimeCol) parse(s string) error {
	if s == "" {
		c.Time = time.Time{}
		return nil
	}
	t, err := time.Parse(TSLayout, s)
	if err != nil {
		return fmt.Errorf("parse stored time %q: %w", s, err)
	}
	c.Time = t
	return nil
}

// stored is a stored time as the units carry it in event data and messages: TSLayout, "" for none.
func (c TimeCol) stored() string {
	if c.IsZero() {
		return ""
	}
	return formatTime(c.Time)
}

// Unit runs fn as a unit of its own, named "exec": exec runs a statement in the unit's savepoint, and an error from
// fn rolls back what it ran, alone. Maintenance, and the tests that change records behind the units' back.
func (s *Store) Unit(ctx context.Context, fn func(exec func(query string, args ...any) error) error) error {
	return s.submit(ctx, "exec", func(w *wtx) error {
		return fn(func(query string, args ...any) error {
			_, err := w.exec(query, args...)
			return err
		})
	})
}

// QueryRow runs a query, written with ? placeholders, on the read pool.
func (s *Store) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.read.QueryRowContext(ctx, query, args...)
}
