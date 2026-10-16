// Package postgres is the Postgres log store (spec section 9.1), on pgx. The writer loop, the units and the reads
// are store/sqlstore's, as for SQLite; this package opens the connections, holds the deployment's leadership lock,
// and migrates the database.
//
// Leadership (spec section 5.9): Open opens one dedicated session, the write connection, and takes the deployment's
// session advisory lock on it (pg_advisory_lock(LockKey)), blocking while another engine holds it: a standby blocks
// in Open. That session is the store's only write connection, so a store without the lock cannot write. When the
// session fails, the store loses the lock with it: every unit fails with store.ErrWriteLost from then on, and Lost
// (store/sqlstore) is closed so the engine steps down. The writer checks the session after any batch that fails as a
// whole, and with a ping every Options.CheckEvery while idle.
//
// Both ends watch the write session (spec section 5.9), with Options.Keepalive as the period k: TCP keepalives
// every k after k idle, on the engine's socket and, through the session's settings, on the server's; a ping or a
// statement unanswered for 2k (3k for a whole batch) loses the session on the engine's side; and the server ends a
// session silent for CheckEvery + 2k (idle_session_timeout), which a healthy engine's pings never let happen, so an
// engine cut off from the database, even behind a proxy that keeps its TCP connection open, releases the lock
// within that window. With the defaults (5 s each) an engine whose host or network dies steps down, and its lock is
// free for the standby, within 15 seconds; an engine whose process dies releases it at once, its socket closed.
//
// An engine opening the store names itself (Options.Engine): the write session's application_name carries its id
// and version, so every engine sees which leads and which wait (Engines), and the engine records its acquisition in
// the leadership table, under the lock, with the next epoch (Leadership).
//
// Reads use a pool (pgxpool) whose sessions are read-only (default_transaction_read_only), so nothing but the
// writer's session ever writes. Leases, deadlines and timeouts use the database clock, now(): the start of the
// batch's transaction, read once per batch.
package postgres

import (
	"cmp"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlstore"
)

// Options configures a store. The zero value is the production configuration.
type Options struct {
	// Now replaces the database clock: tests only.
	Now func() time.Time
	// Rand returns the retry jitter fraction in [0, 1); nil uses math/rand/v2.
	Rand func() float64
	// ReadConns caps the read pool; zero means 4.
	ReadConns int
	// Deployment names the deployment whose leadership lock the store takes; empty means "millstream". Two
	// deployments sharing a database need two names.
	Deployment string
	// CheckEvery is how often the idle writer pings its session (engine.lockCheck); zero means DefaultCheckEvery.
	CheckEvery time.Duration
	// Keepalive is the period the write session is watched with (engine.keepalive): see the package comment; zero
	// means DefaultKeepalive.
	Keepalive time.Duration
	// Engine names the engine opening the store. With an ID, the write session carries it and Open records the
	// acquisition in the leadership table; without, the store is a command's (--direct) and records nothing.
	Engine Engine
	// Waiting, when set, is called once when Open finds the leadership lock held by another engine, before it waits
	// for it: the moment a standby starts waiting.
	Waiting func()
}

// Defaults for zero Options.
const (
	DefaultCheckEvery = 5 * time.Second
	DefaultKeepalive  = 5 * time.Second
)

// Engine is an engine of the deployment, as it names itself.
type Engine struct {
	ID      string
	Version string
	// Address is where the engine serves its API, recorded when it leads; empty when it serves none.
	Address string
}

// Leadership is an acquisition of the leadership lock, as the leadership table records it.
type Leadership struct {
	Engine Engine
	// Epoch counts the acquisitions of the deployment's lock by engines: one more at each.
	Epoch int64
	Since time.Time
}

// Store is the Postgres log store. It is safe for concurrent use.
type Store struct {
	*sqlstore.Store
	lead  Leadership // what Open recorded; zero for a store without an engine
	key   int64
	write *sql.DB   // the write connection's handle: one session, conn
	conn  *sql.Conn // the session holding the lock, the writer's
	pool  *pgxpool.Pool
	reads *sql.DB // the read pool, as database/sql sees it
	read  *sqlstore.ReadDB
}

var _ store.Store = (*Store)(nil)

//go:embed migrations/*.sql
var migrations embed.FS

// dialect is Postgres's: numbered placeholders, now(), jsonb, and statements the driver prepares and caches itself.
var dialect = sqlstore.Dialect{
	Rebind:   rebindCached,
	Now:      `now()`,
	JSONEach: jsonEach,
	JSONParam: func(integers bool) string {
		value := "value"
		if integers {
			value = "CAST(value AS bigint)"
		}
		return "SELECT " + value + " FROM " + jsonEach("CAST(? AS jsonb)")
	},
}

func jsonEach(expr string) string { return "jsonb_array_elements_text(" + expr + ") AS je(value)" }

// rebinds caches Rebind by query: the units' queries are a few hundred constant texts.
var rebinds sync.Map

func rebindCached(query string) string {
	if q, ok := rebinds.Load(query); ok {
		return q.(string)
	}
	q := Rebind(query)
	rebinds.Store(query, q)
	return q
}

// LockKey is the advisory lock key of a deployment: the 64-bit FNV-1a hash of its name.
func LockKey(deployment string) int64 {
	if deployment == "" {
		deployment = "millstream"
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte("millstream.leader:" + deployment))
	return int64(h.Sum64()) // #nosec G115 -- a lock key: any 64 bits will do, the sign included
}

// Open connects to the database dsn names, takes the deployment's leadership lock on the write connection, waiting
// while another engine holds it, applies pending migrations, and starts the writer. ctx bounds the whole of it, the
// wait for the lock included: a standby's Open returns once it leads, or with ctx's error. The DSN is never part of
// an error.
func Open(ctx context.Context, dsn string, opts Options) (*Store, error) {
	s := &Store{key: LockKey(opts.Deployment)}
	if opts.ReadConns == 0 {
		opts.ReadConns = 4
	}
	opts.CheckEvery = cmp.Or(opts.CheckEvery, DefaultCheckEvery)
	opts.Keepalive = cmp.Or(opts.Keepalive, DefaultKeepalive)
	if err := s.open(ctx, dsn, opts); err != nil {
		_ = s.closeDBs()
		return nil, fmt.Errorf("open postgres store: %w", err)
	}
	s.Store = sqlstore.New(sqlstore.Config{Dialect: dialect, Write: s.conn, Read: s.read,
		Options: sqlstore.Options{Now: opts.Now, Rand: opts.Rand},
		Check:   s.conn.PingContext, CheckEvery: opts.CheckEvery,
		CheckTimeout: 2 * opts.Keepalive, BatchTimeout: 3 * opts.Keepalive})
	return s, nil
}

// OpenReadOnly connects to the database dsn names for reading only: the read pool, without the leadership lock or
// a write connection, so it never waits for an engine and never stands in its way. Every unit fails with
// store.ErrReadOnly: on Postgres the active engine is the only writer (spec section 5.9). The database must be at
// this build's schema version, which an engine migrated it to. What CLI commands that only read open.
func OpenReadOnly(ctx context.Context, dsn string, opts Options) (*Store, error) {
	s := &Store{key: LockKey(opts.Deployment)}
	if opts.ReadConns == 0 {
		opts.ReadConns = 2
	}
	secrets := []string{dsn}
	if cfg, err := pgx.ParseConfig(dsn); err == nil {
		secrets = append(secrets, cfg.Password)
	}
	if err := s.openReads(ctx, dsn, opts, secrets); err != nil {
		_ = s.closeDBs()
		return nil, fmt.Errorf("open postgres store read-only: %w", err)
	}
	if err := sqlstore.CheckVersion(ctx, s.reads, migrations); err != nil {
		_ = s.closeDBs()
		return nil, fmt.Errorf("open postgres store read-only: %w", redact(err, secrets...))
	}
	s.Store = sqlstore.New(sqlstore.Config{Dialect: dialect, Read: s.read, ReadOnly: true,
		Options: sqlstore.Options{Now: opts.Now, Rand: opts.Rand}})
	return s, nil
}

func (s *Store) open(ctx context.Context, dsn string, opts Options) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse the DSN: %w", redact(err, dsn))
	}
	secrets := []string{dsn, cfg.Password}
	watchSession(cfg, opts)
	cfg.RuntimeParams["application_name"] = applicationName(opts.Engine)
	s.write = stdlib.OpenDB(*cfg)
	s.write.SetMaxOpenConns(1)
	s.write.SetMaxIdleConns(1)
	s.write.SetConnMaxLifetime(0)
	s.write.SetConnMaxIdleTime(0)
	if s.conn, err = s.write.Conn(ctx); err != nil {
		return fmt.Errorf("connect: %w", redact(err, secrets...))
	}
	var took bool
	if err := s.conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, s.key).Scan(&took); err != nil {
		return fmt.Errorf("take the leadership lock: %w", redact(err, secrets...))
	}
	if !took {
		if opts.Waiting != nil {
			opts.Waiting()
		}
		if _, err := s.conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, s.key); err != nil {
			return fmt.Errorf("take the leadership lock: %w", redact(err, secrets...))
		}
	}
	if err := sqlstore.Migrate(ctx, s.conn, dialect, migrations); err != nil {
		return err
	}
	if opts.Engine.ID != "" {
		if err := s.recordLeadership(ctx, opts.Engine); err != nil {
			return fmt.Errorf("record the leadership: %w", redact(err, secrets...))
		}
	}
	return s.openReads(ctx, dsn, opts, secrets)
}

// openReads opens the read pool, whose sessions are read-only.
func (s *Store) openReads(ctx context.Context, dsn string, opts Options, secrets []string) error {
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse the DSN: %w", redact(err, secrets...))
	}
	pcfg.MaxConns = int32(min(opts.ReadConns, 1<<16)) // #nosec G115 -- bounded above
	pcfg.ConnConfig.RuntimeParams["application_name"] = "millstream-read"
	dialer(pcfg.ConnConfig, cmp.Or(opts.Keepalive, DefaultKeepalive))
	pcfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	if s.pool, err = pgxpool.NewWithConfig(ctx, pcfg); err != nil {
		return fmt.Errorf("open the read pool: %w", redact(err, secrets...))
	}
	s.reads = stdlib.OpenDBFromPool(s.pool)
	s.read = sqlstore.NewReadDB(s.reads, dialect, nil)
	return nil
}

// redact removes the secrets (the DSN, its password) from err's text, should a driver error quote one. The error
// it returns keeps err's text only: the driver's error values, which may hold the secrets, are dropped.
func redact(err error, secrets ...string) error {
	text, found := err.Error(), false
	for _, s := range secrets {
		if s != "" && strings.Contains(text, s) {
			text, found = strings.ReplaceAll(text, s, "<redacted>"), true
		}
	}
	if !found {
		return err
	}
	return errors.New(text)
}

// LockKey returns the advisory lock key the store holds.
func (s *Store) LockKey() int64 { return s.key }

// Close stops accepting units, lets the writer apply the ones already queued, and closes the connections, which
// releases the leadership lock.
func (s *Store) Close() error {
	if stmts := s.Stop(); stmts == nil {
		return nil // closed already
	}
	if s.LostErr() != nil {
		// The session is gone, perhaps with the network: there is nothing to flush, and saying goodbye to the
		// read pool's sessions could wait seconds for a database that no longer answers.
		go func() { _ = s.closeDBs() }()
		return nil
	}
	return s.closeDBs()
}

func (s *Store) closeDBs() error {
	var errs []error
	if s.conn != nil {
		errs = append(errs, s.conn.Close())
	}
	if s.write != nil {
		errs = append(errs, s.write.Close())
	}
	if s.reads != nil {
		errs = append(errs, s.reads.Close())
	}
	if s.pool != nil {
		s.pool.Close()
	}
	if err := errors.Join(errs...); err != nil && !errors.Is(err, sql.ErrConnDone) {
		return fmt.Errorf("close store: %w", err)
	}
	return nil
}

// Rebind numbers a query's ? placeholders ($1, $2 …) for Postgres, outside quoted strings and identifiers; ?N
// becomes $N.
func Rebind(query string) string {
	if !strings.Contains(query, "?") {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 8)
	n := 0
	var quote byte
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '?':
			j := i + 1
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				j++
			}
			b.WriteByte('$')
			if j > i+1 {
				b.WriteString(query[i+1 : j])
			} else {
				n++
				b.WriteString(strconv.Itoa(n))
			}
			i = j - 1
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}
