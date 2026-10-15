// Package sqlite is the SQLite log store (spec section 9.1), on the pure-Go modernc.org/sqlite driver.
//
// Writes go through a single writer goroutine that owns the only write connection (D23): units queue on a
// channel, and the writer applies every queued unit (at most 64, gathered for at most 5 ms) in one transaction,
// each under its own savepoint, so a unit that fails is rolled back alone and the others commit. Reads use a
// separate pool of query-only connections, which WAL lets run alongside the writer.
//
// Clock: every timestamp a batch writes, and every comparison it makes (due retries, expired leases), uses one
// instant read when the batch begins. That instant is Options.Now when set (tests), and otherwise the database
// clock, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'): lease expiry uses the database clock, never a node's (spec 5.11).
//
// Failpoints: each unit calls failpoint.Hit("<unit>.beforeCommit") before the batch commits and
// failpoint.Hit("<unit>.afterCommit") after, for every unit of the batch in batch order; Units lists the names.
// A failpoint armed in-process makes the whole batch fail with the failpoint.Crash error: rolled back before the
// commit, committed after it.
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/monolithiclab/millstream/internal/store"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

// Units names the atomic units, as their failpoints spell them.
var Units = []string{
	unitApplyRevision, unitFire, unitLease, unitRenew, unitDone, unitFail, unitCancel,
	unitReplay, unitDiscard, unitExpireLeases, unitSetPaused, unitSkipFirings, unitFilter, unitDedupe,
	unitStartReplay, unitRecordEval,
}

const (
	unitApplyRevision = "applyRevision"
	unitFire          = "fire"
	unitLease         = "lease"
	unitRenew         = "renew"
	unitDone          = "done"
	unitFail          = "fail"
	unitCancel        = "cancel"
	unitReplay        = "replay"
	unitDiscard       = "discard"
	unitExpireLeases  = "expireLeases"
	unitSetPaused     = "setPaused"
	unitSkipFirings   = "skipFirings"
	unitFilter        = "filter"
	unitDedupe        = "dedupe"
	unitStartReplay   = "startReplay"
	unitRecordEval    = "recordEval"
)

// Memory opens a store on a fresh temporary database file, removed by Close. A real file rather than SQLite's
// :memory:, because each connection to :memory: is its own database and a shared-cache memory database has
// neither WAL nor busy waiting, so the writer and the read pool could not share it.
const Memory = ":memory:"

// Options configures a store. The zero value is the production configuration.
type Options struct {
	// Now replaces the database clock; see the package comment.
	Now func() time.Time
	// Rand returns the retry jitter fraction in [0, 1); nil uses math/rand/v2.
	Rand func() float64
	// BusyTimeout bounds how long a connection waits on a lock; zero means 5 seconds.
	BusyTimeout time.Duration
	// ReadConns caps the read pool; zero means 4.
	ReadConns int
}

// Store is the SQLite log store. It is safe for concurrent use.
type Store struct {
	write *sql.DB // one connection, used only by the writer goroutine
	read  *sql.DB
	opts  Options
	tmp   string // the temporary directory of a Memory store

	mu     sync.RWMutex // guards closed against sends on reqs
	closed bool
	reqs   chan *request
	done   chan struct{} // closed when the writer exits

	// Writer goroutine only.
	revs map[string]*revision // revisions by package hash; see revision
	rr   int                  // round-robin offset across pipelines
}

var _ store.Store = (*Store)(nil)

//go:embed migrations/*.sql
var migrations embed.FS

// Open opens (creating if needed) the store at path, or a temporary one for Memory, and applies pending
// migrations.
func Open(path string, opts Options) (*Store, error) {
	s := &Store{opts: opts, reqs: make(chan *request), done: make(chan struct{}), revs: map[string]*revision{}}
	if s.opts.BusyTimeout == 0 {
		s.opts.BusyTimeout = 5 * time.Second
	}
	if s.opts.ReadConns == 0 {
		s.opts.ReadConns = 4
	}
	if s.opts.Rand == nil {
		s.opts.Rand = rand.Float64 // #nosec G404 -- retry jitter, not a secret
	}
	if path == Memory {
		dir, err := os.MkdirTemp("", "millstream-store-")
		if err != nil {
			return nil, fmt.Errorf("open store: %w", err)
		}
		s.tmp, path = dir, filepath.Join(dir, "millstream.db")
	}
	if err := s.open(path); err != nil {
		_ = s.closeDBs() // the open error is the one to report
		return nil, err
	}
	go s.writer()
	return s, nil
}

func (s *Store) open(path string) error {
	pragmas := "_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)" +
		"&_pragma=busy_timeout(" + strconv.FormatInt(s.opts.BusyTimeout.Milliseconds(), 10) + ")"
	var err error
	if s.write, err = sql.Open("sqlite", path+"?"+pragmas+"&_txlock=immediate"); err != nil {
		return fmt.Errorf("open store %s: %w", path, err)
	}
	s.write.SetMaxOpenConns(1)
	s.write.SetMaxIdleConns(1)
	s.write.SetConnMaxLifetime(0)
	if err := migrate(s.write); err != nil {
		return fmt.Errorf("open store %s: %w", path, err)
	}
	if s.read, err = sql.Open("sqlite", path+"?"+pragmas+"&_pragma=query_only(1)"); err != nil {
		return fmt.Errorf("open store %s: %w", path, err)
	}
	s.read.SetMaxOpenConns(s.opts.ReadConns)
	return nil
}

// migrate applies every embedded migration newer than the recorded schema version, each in its own transaction.
func migrate(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	var current int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&current); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	slices.Sort(names)
	for _, name := range names {
		base := filepath.Base(name)
		version, err := strconv.Atoi(strings.SplitN(base, "_", 2)[0])
		if err != nil {
			return fmt.Errorf("migrate: bad migration name %s", base)
		}
		if version <= current {
			continue
		}
		body, err := migrations.ReadFile(name)
		if err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		if err := applyMigration(db, version, string(body)); err != nil {
			return fmt.Errorf("migrate %s: %w", base, err)
		}
	}
	return nil
}

func applyMigration(db *sql.DB, version int, body string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(body); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO schema_version (version) VALUES (?)`, version); err != nil {
		return err
	}
	return tx.Commit()
}

// Close stops accepting units, lets the writer apply the ones already queued, and closes the database.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.reqs)
	s.mu.Unlock()
	<-s.done
	return s.closeDBs()
}

func (s *Store) closeDBs() error {
	var errs []error
	for _, db := range []*sql.DB{s.read, s.write} {
		if db != nil {
			errs = append(errs, db.Close())
		}
	}
	if s.tmp != "" {
		errs = append(errs, os.RemoveAll(s.tmp))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("close store: %w", err)
	}
	return nil
}

// tsLayout is how times are stored: fixed width in UTC, so text order is time order.
const tsLayout = "2006-01-02T15:04:05.000Z"

func formatTime(t time.Time) string {
	return t.UTC().Format(tsLayout)
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(tsLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse stored time %q: %w", s, err)
	}
	return t, nil
}

// clock returns the batch's instant: Options.Now, or the database clock.
func (s *Store) clock(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	if s.opts.Now != nil {
		return s.opts.Now().UTC().Truncate(time.Millisecond), nil
	}
	var now string
	if err := tx.QueryRowContext(ctx, `SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read database clock: %w", err)
	}
	return parseTime(now)
}
