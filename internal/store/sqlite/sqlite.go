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
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/monolithiclab/millstream/internal/store"

	msqlite "modernc.org/sqlite" // also registers the "sqlite" driver
)

// Units names the atomic units, as their failpoints spell them.
var Units = []string{
	unitApplyRevision, unitFire, unitLease, unitRenew, unitDone, unitFail, unitCancel,
	unitReplay, unitDiscard, unitExpireLeases, unitSetPaused, unitSkipFirings, unitFilter, unitDedupe,
	unitStartReplay, unitRecordEval, unitRollup, unitRecordInsights, unitSetInsightState,
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
)

// Memory opens a store on a fresh in-memory database, freed by Close: a database of SQLite's memdb VFS under a
// name of its own, which Clone copies from. It starts as a copy of the migrated template (templateImage), loaded once
// per process, or as a closed store's database emptied (memoryPool), so opening many (tests, millstream test) does
// not migrate each. It has no read pool: memdb has no write-ahead log, so its
// reads run on the writer's connection, between batches, where a file store's read pool runs them alongside.
const Memory = ":memory:"

// memoryTemplateURI names the template database Memory stores start as a copy of.
const memoryTemplateURI = "file:/millstream-template?vfs=memdb"

// memorySeq numbers the process's Memory stores, whose databases are named after it.
var memorySeq atomic.Uint64

// templateImage is the database the migrations build, serialised (TestTemplateImage checks it, and rewrites it with
// -update): loading it costs a tenth of migrating, which every process opening a Memory store would pay.
//
//go:embed template.db
var templateImage []byte

// memoryTemplate loads the template database once, and keeps a connection to it open for the life of the process: a
// memdb database lives as long as a connection to it.
var memoryTemplate = sync.OnceValues(func() (*sql.DB, error) {
	db, err := sql.Open("sqlite", memoryTemplateURI)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := loadImage(templateImage, memoryTemplateURI); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load the template image: %w", err)
	}
	return db, nil
})

// loadImage copies a serialised database into the database at dstURI, which a connection must hold open: it
// deserialises the image on a connection of its own, then backs that up to dstURI.
func loadImage(image []byte, dstURI string) error {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return conn.Raw(func(dc any) error {
		c, ok := dc.(interface {
			Deserialize(buf []byte) error
			NewBackup(dstURI string) (*msqlite.Backup, error)
		})
		if !ok {
			return fmt.Errorf("%T has no deserialize or backup API", dc)
		}
		if err := c.Deserialize(image); err != nil {
			return err
		}
		b, err := c.NewBackup(dstURI)
		if err != nil {
			return err
		}
		_, err = b.Step(-1)
		if ferr := b.Finish(); err == nil {
			err = ferr
		}
		return err
	})
}

// restore copies the database at srcURI, with SQLite's backup API, into the database db is open on.
func restore(db *sql.DB, srcURI string) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return conn.Raw(func(dc any) error {
		r, ok := dc.(interface {
			NewRestore(srcURI string) (*msqlite.Backup, error)
		})
		if !ok {
			return fmt.Errorf("restore: %T has no backup API", dc)
		}
		b, err := r.NewRestore(srcURI)
		if err != nil {
			return fmt.Errorf("restore: %w", err)
		}
		_, err = b.Step(-1)
		if ferr := b.Finish(); err == nil {
			err = ferr
		}
		if err != nil {
			return fmt.Errorf("restore: %w", err)
		}
		return nil
	})
}

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
	write *sql.DB // one connection, the writer goroutine's (and a Memory store's reads, between batches)
	read  *readDB // the read pool; on write for a Memory store
	opts  Options
	uri   string // the database, for Clone
	// cookie is the schema version a Memory store's database had as the migrated template, which goes back to
	// memoryPool on Close; zero for a file store, and for a clone, whose statements the restore expired.
	cookie int64

	mu     sync.RWMutex // guards closed against sends on reqs
	closed bool
	reqs   chan *request
	done   chan struct{} // closed when the writer exits

	// Writer goroutine only.
	revs   map[string]*revision // revisions by package hash; see revision
	rr     int                  // round-robin offset across pipelines
	stmts  map[string]*sql.Stmt // prepared write statements by query; see wtx.stmt
	unseen map[string]bool      // queries of the last batch to prepare once it ends
}

var _ store.Store = (*Store)(nil)

//go:embed migrations/*.sql
var migrations embed.FS

// Open opens (creating if needed) the store at path, or a temporary one for Memory, and applies pending
// migrations.
func Open(path string, opts Options) (*Store, error) {
	return open(path, "", opts)
}

// Clone opens a Memory store holding a copy of s's database as committed when it is called: a store to try
// something on without touching s, or a test's copy of a history built once.
func (s *Store) Clone(opts Options) (*Store, error) {
	if s.read.closed.Load() { // its database may be another store's now
		return nil, store.ErrClosed
	}
	return open(Memory, s.uri, opts)
}

// open opens the store at path; a Memory store starts as a copy of the database at from, or of the migrated
// template when from is empty.
func open(path, from string, opts Options) (*Store, error) {
	s := &Store{opts: opts, reqs: make(chan *request), done: make(chan struct{}), revs: map[string]*revision{},
		stmts: map[string]*sql.Stmt{}, unseen: map[string]bool{}}
	if s.opts.BusyTimeout == 0 {
		s.opts.BusyTimeout = 5 * time.Second
	}
	if s.opts.ReadConns == 0 {
		s.opts.ReadConns = 4
	}
	if s.opts.Rand == nil {
		s.opts.Rand = rand.Float64 // #nosec G404 -- retry jitter, not a secret
	}
	if err := s.open(path, from); err != nil {
		_ = s.closeDBs() // the open error is the one to report
		return nil, err
	}
	go s.writer()
	return s, nil
}

func (s *Store) open(path, from string) error {
	memory := path == Memory
	template := memory && from == ""
	if template { // a clone takes a connection of its own: the restore would expire a pooled one's statements
		if m := takeMemory(); m != nil {
			s.write, s.read, s.uri, s.stmts, s.cookie = m.write, newReadDB(m.write, m.reads), m.uri, m.stmts, m.cookie
			return nil
		}
	}
	sep := "?"
	if memory {
		path, sep = fmt.Sprintf("file:/millstream-%d?vfs=memdb", memorySeq.Add(1)), "&"
	}
	busy := "_pragma=busy_timeout(" + strconv.FormatInt(s.opts.BusyTimeout.Milliseconds(), 10) + ")"
	pragmas := "_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)&" + busy
	s.uri = path + sep + busy
	var err error
	if s.write, err = sql.Open("sqlite", path+sep+pragmas+"&_txlock=immediate"); err != nil {
		return fmt.Errorf("open store %s: %w", path, err)
	}
	s.write.SetMaxOpenConns(1)
	s.write.SetMaxIdleConns(1)
	s.write.SetConnMaxLifetime(0)
	if memory {
		if template {
			if _, err := memoryTemplate(); err != nil {
				return fmt.Errorf("open store: %w", err)
			}
			from = memoryTemplateURI
		}
		if err := restore(s.write, from); err != nil {
			return fmt.Errorf("open store: %w", err)
		}
	}
	if err := migrate(s.write); err != nil {
		return fmt.Errorf("open store %s: %w", path, err)
	}
	if memory {
		// The writer's connection serves the reads too: a read waits for the batch in progress, as it would on
		// memdb's locks, but without a connection of its own to open or busy_timeout to sleep through.
		s.read = newReadDB(s.write, nil)
		if template {
			if s.cookie, err = schemaVersion(s.write); err != nil {
				return fmt.Errorf("open store: %w", err)
			}
		}
		return nil
	}
	read, err := sql.Open("sqlite", path+sep+pragmas+"&_pragma=query_only(1)")
	if err != nil {
		return fmt.Errorf("open store %s: %w", path, err)
	}
	read.SetMaxOpenConns(s.opts.ReadConns)
	s.read = newReadDB(read, nil)
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
	if s.cookie != 0 {
		s.read.closed.Store(true) // its connection may serve another store from now on
		if putMemory(&memoryDB{write: s.write, reads: s.read.stmts, uri: s.uri, stmts: s.stmts, cookie: s.cookie}) {
			return nil
		}
	}
	return s.closeDBs()
}

func (s *Store) closeDBs() error {
	var errs []error
	for _, st := range s.stmts {
		errs = append(errs, st.Close())
	}
	dbs := []*sql.DB{s.write}
	if s.read != nil {
		s.read.closeStmts()
		if s.read.DB != s.write { // a Memory store reads on its write connection
			dbs = append(dbs, s.read.DB)
		}
	}
	for _, db := range dbs {
		if db != nil {
			errs = append(errs, db.Close())
		}
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
