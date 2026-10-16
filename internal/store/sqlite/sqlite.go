// Package sqlite is the SQLite log store (spec section 9.1), on the pure-Go modernc.org/sqlite driver. The writer
// loop, the units and the reads are store/sqlstore's; this package opens the database, migrates it and keeps the
// in-memory stores tests and millstream test open by the hundred cheap.
//
// The writer owns the only write connection (D23); reads use a separate pool of query-only connections, which WAL
// lets run alongside the writer. The database clock is strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), and times are stored as
// text in sqlstore.TSLayout, so that they compare as strings. CLI commands that change state write from their own
// process, through their own writer loop, under SQLite's file locking (spec section 5.9).
package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlstore"

	msqlite "modernc.org/sqlite" // also registers the "sqlite" driver
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
	return onRawConn(db, func(c rawConn) error {
		if err := c.Deserialize(image); err != nil {
			return err
		}
		return backup(c.NewBackup(dstURI))
	})
}

// restore copies the database at srcURI, with SQLite's backup API, into the database db is open on.
func restore(db *sql.DB, srcURI string) error {
	if err := onRawConn(db, func(c rawConn) error { return backup(c.NewRestore(srcURI)) }); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	return nil
}

// rawConn is what the store uses of a modernc.org/sqlite connection beyond database/sql.
type rawConn interface {
	Serialize() ([]byte, error)
	Deserialize(buf []byte) error
	NewBackup(dstURI string) (*msqlite.Backup, error)
	NewRestore(srcURI string) (*msqlite.Backup, error)
}

// onRawConn runs fn on one of db's driver connections.
func onRawConn(db *sql.DB, fn func(rawConn) error) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return conn.Raw(func(dc any) error {
		c, ok := dc.(rawConn)
		if !ok {
			return fmt.Errorf("%T has no deserialize or backup API", dc)
		}
		return fn(c)
	})
}

// backup runs a backup b, opened with err, to its end.
func backup(b *msqlite.Backup, err error) error {
	if err != nil {
		return err
	}
	_, err = b.Step(-1)
	if ferr := b.Finish(); err == nil {
		err = ferr
	}
	return err
}

// Options configures a store. The zero value is the production configuration.
type Options struct {
	// Now replaces the database clock; see sqlstore.
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
	*sqlstore.Store
	write *sql.DB          // one connection, the writer goroutine's (and a Memory store's reads, between batches)
	read  *sqlstore.ReadDB // the read pool; on write for a Memory store
	opts  Options
	uri   string // the database, for Clone
	// cookie is the schema version a Memory store's database had as the migrated template, which goes back to
	// memoryPool on Close; zero for a file store, and for a clone, whose statements the restore expired.
	cookie int64
	stmts  map[string]*sql.Stmt // a pooled connection's write statements, until the writer takes them over
}

var _ store.Store = (*Store)(nil)

// dialect is SQLite's: question-mark placeholders, the clock as text, json_each, and statements prepared once.
var dialect = sqlstore.Dialect{
	Now:       `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`,
	JSONEach:  func(expr string) string { return "json_each(" + expr + ")" },
	JSONParam: func(bool) string { return "SELECT value FROM json_each(?)" },
	Prepare:   true,
}

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
	if s.read.Closed() { // its database may be another store's now
		return nil, store.ErrClosed
	}
	return open(Memory, s.uri, opts)
}

// OpenImage opens a Memory store holding a copy of a database Image serialised: a history built once and kept as a
// file, which tests load instead of building it again. Migrations newer than the image apply as they would to a
// file store.
func OpenImage(image []byte, opts Options) (*Store, error) {
	uri := fmt.Sprintf("file:/millstream-image-%d?vfs=memdb", memorySeq.Add(1))
	hold, err := sql.Open("sqlite", uri) // a memdb database lives while a connection to it does
	if err != nil {
		return nil, err
	}
	defer func() { _ = hold.Close() }()
	hold.SetMaxOpenConns(1)
	if err := hold.Ping(); err != nil {
		return nil, err
	}
	if err := loadImage(image, uri); err != nil {
		return nil, fmt.Errorf("open image: %w", err)
	}
	return open(Memory, uri, opts)
}

// Image serialises the store's database as committed now, for OpenImage.
func (s *Store) Image() ([]byte, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var image []byte
	err = onRawConn(db, func(c rawConn) error {
		if err := backup(c.NewRestore(s.uri)); err != nil {
			return err
		}
		b, err := c.Serialize()
		image = bytes.Clone(b)
		return err
	})
	return image, err
}

// open opens the store at path; a Memory store starts as a copy of the database at from, or of the migrated
// template when from is empty.
func open(path, from string, opts Options) (*Store, error) {
	s := &Store{opts: opts}
	if s.opts.BusyTimeout == 0 {
		s.opts.BusyTimeout = 5 * time.Second
	}
	if s.opts.ReadConns == 0 {
		s.opts.ReadConns = 4
	}
	if err := s.open(path, from); err != nil {
		_ = s.closeDBs(nil) // the open error is the one to report
		return nil, err
	}
	s.Store = sqlstore.New(sqlstore.Config{Dialect: dialect, Write: s.write, Read: s.read, Stmts: s.stmts,
		Options: sqlstore.Options{Now: opts.Now, Rand: opts.Rand}})
	s.stmts = nil
	return s, nil
}

func (s *Store) open(path, from string) error {
	memory := path == Memory
	template := memory && from == ""
	if template { // a clone takes a connection of its own: the restore would expire a pooled one's statements
		if m := takeMemory(); m != nil {
			s.write, s.uri, s.stmts, s.cookie = m.write, m.uri, m.stmts, m.cookie
			s.read = sqlstore.NewReadDB(m.write, dialect, m.reads)
			return nil
		}
	}
	sep := "?"
	if memory {
		path, sep = fmt.Sprintf("file:/millstream-%d?vfs=memdb", memorySeq.Add(1)), "&"
	} else if err := createFromImage(path, bytes.NewReader(templateImage)); err != nil {
		return fmt.Errorf("open store %s: %w", path, err)
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
	if err := sqlstore.Migrate(context.Background(), s.write, dialect, migrations); err != nil {
		return fmt.Errorf("open store %s: %w", path, err)
	}
	if memory {
		// The writer's connection serves the reads too: a read waits for the batch in progress, as it would on
		// memdb's locks, but without a connection of its own to open or busy_timeout to sleep through.
		s.read = sqlstore.NewReadDB(s.write, dialect, nil)
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
	s.read = sqlstore.NewReadDB(read, dialect, nil)
	return nil
}

// Close stops accepting units, lets the writer apply the ones already queued, and closes the database.
func (s *Store) Close() error {
	stmts := s.Stop()
	if stmts == nil {
		return nil // closed already
	}
	if s.cookie != 0 {
		reads := s.read.Hand() // its connection may serve another store from now on
		if putMemory(&memoryDB{write: s.write, reads: reads, uri: s.uri, stmts: stmts, cookie: s.cookie}) {
			return nil
		}
	}
	return s.closeDBs(stmts)
}

func (s *Store) closeDBs(stmts map[string]*sql.Stmt) error {
	var errs []error
	for _, st := range stmts {
		errs = append(errs, st.Close())
	}
	for _, st := range s.stmts {
		errs = append(errs, st.Close())
	}
	dbs := []*sql.DB{s.write}
	if s.read != nil {
		s.read.CloseStmts()
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

// createFromImage writes the migrated template image at path when nothing is there yet, so that a new store
// starts migrated rather than running every migration; migrate then applies only those newer than the image. The
// image goes to a temporary file first, synced, then is hard-linked into place: a concurrent first open, or one after
// a crash, sees either no file or the complete image, never a part of it. A link that loses the race to another
// opener's leaves that opener's complete image in place.
func createFromImage(path string, image io.Reader) error {
	if _, err := os.Lstat(path); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	dir, base := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, "."+base+".new-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	_, err = io.Copy(tmp, image)
	if err == nil {
		err = fsync(tmp)
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Link(tmp.Name(), path); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	return syncDir(dir)
}

// fsync flushes a file to stable storage; tests that only check what a file holds replace it, macOS's full sync
// taking milliseconds.
var fsync = (*os.File).Sync

// syncDir makes a directory's entries durable: the link createFromImage added, here.
func syncDir(dir string) error {
	d, err := os.Open(dir) // #nosec G304 -- the store's own directory
	if err != nil {
		return err
	}
	err = fsync(d)
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
