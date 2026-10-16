package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// enginePrefix starts the application_name of an engine's write session, before its id and version.
const enginePrefix = "millstream-engine "

// maxApplicationName is the longest application_name Postgres keeps (NAMEDATALEN - 1 bytes).
const maxApplicationName = 63

// applicationName names the write session: after the engine opening it, so other engines see it lead or wait, else
// plain millstream (a command's --direct store).
func applicationName(e Engine) string {
	if e.ID == "" {
		return "millstream"
	}
	name := enginePrefix + e.ID + " " + e.Version
	if len(name) > maxApplicationName {
		name = name[:maxApplicationName]
	}
	return name
}

// parseApplicationName returns the engine an application_name names, ok false for a session that is not an
// engine's write session.
func parseApplicationName(name string) (Engine, bool) {
	rest, ok := strings.CutPrefix(name, enginePrefix)
	if !ok {
		return Engine{}, false
	}
	id, version, _ := strings.Cut(rest, " ")
	return Engine{ID: id, Version: version}, id != ""
}

// watchSession configures the write session to be watched from both ends with keepalive period k (package
// comment): the engine's socket through its dialer, the server's through the session's settings, which any role
// may set.
func watchSession(cfg *pgx.ConnConfig, opts Options) {
	k := opts.Keepalive
	p := cfg.RuntimeParams
	secs := strconv.FormatInt(int64(max(1, math.Ceil(k.Seconds()))), 10)
	p["tcp_keepalives_idle"] = secs
	p["tcp_keepalives_interval"] = secs
	p["tcp_keepalives_count"] = "2"
	p["tcp_user_timeout"] = millis(3 * k)
	p["idle_session_timeout"] = millis(opts.CheckEvery + 2*k)
	p["idle_in_transaction_session_timeout"] = millis(3 * k)
	dialer(cfg, k)
}

// dialer makes the engine's end of a connection watched with keepalive period k: TCP keepalives, a connection
// timeout, and the user timeout where it exists. The read pool's connections get it too, so a read on a cut-off
// database fails rather than hangs.
func dialer(cfg *pgx.ConnConfig, k time.Duration) {
	cfg.ConnectTimeout = 3 * k
	d := &net.Dialer{
		KeepAliveConfig: net.KeepAliveConfig{Enable: true, Idle: k, Interval: k, Count: 2},
		Control:         userTimeout(3 * k),
	}
	cfg.DialFunc = d.DialContext
}

// millis is a duration as a Postgres setting in milliseconds, at most the largest one it takes.
func millis(d time.Duration) string {
	return strconv.FormatInt(min(max(d.Milliseconds(), 1), math.MaxInt32), 10)
}

// recordLeadership records the engine's acquisition of the lock in the leadership table, on the write session that
// holds the lock, with the next epoch.
func (s *Store) recordLeadership(ctx context.Context, e Engine) error {
	s.lead = Leadership{Engine: e}
	return s.conn.QueryRowContext(ctx, `INSERT INTO leadership (lock_key, engine_id, version, address, epoch, since)
		VALUES ($1, $2, $3, $4, 1, now())
		ON CONFLICT (lock_key) DO UPDATE SET engine_id = EXCLUDED.engine_id, version = EXCLUDED.version,
			address = EXCLUDED.address, epoch = leadership.epoch + 1, since = EXCLUDED.since
		RETURNING epoch, since`, s.key, e.ID, e.Version, e.Address).Scan(&s.lead.Epoch, &s.lead.Since)
}

// Leadership returns the acquisition Open recorded: zero for a store opened without an engine id.
func (s *Store) Leadership() Leadership { return s.lead }

// EngineStatus is an engine of the deployment as the database sees it: holding the lock or waiting for it.
type EngineStatus struct {
	Engine
	Active bool
	// Since is when the active engine took the lock, or when a standby began to wait for it.
	Since time.Time
	// Epoch is the active engine's epoch; zero for a standby.
	Epoch int64
}

// Engines returns the engines holding or waiting for the deployment's lock, the active one first, then the standbys
// in the order they began to wait; sessions that are not an engine's (a command's --direct store) are left out.
// It reads through the read pool, so a store opened read-only (a standby's API) answers it too.
func (s *Store) Engines(ctx context.Context) ([]EngineStatus, error) {
	k := uint64(s.key) // #nosec G115 -- the key's bits, as pg_locks splits them
	rows, err := s.reads.QueryContext(ctx, `SELECT a.application_name, l.granted, COALESCE(a.query_start, a.backend_start)
		FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'advisory' AND l.objsubid = 1 AND l.classid::bigint = $1 AND l.objid::bigint = $2
			AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
		ORDER BY l.granted DESC, 3`, int64(k>>32), int64(k&math.MaxUint32))
	if err != nil {
		return nil, fmt.Errorf("engines: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var sessions []lockSession
	for rows.Next() {
		var (
			ls    lockSession
			since sql.NullTime
		)
		if err := rows.Scan(&ls.name, &ls.granted, &since); err != nil {
			return nil, fmt.Errorf("engines: %w", err)
		}
		ls.since = since.Time
		sessions = append(sessions, ls)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("engines: %w", err)
	}
	var lead *Leadership
	if slices.ContainsFunc(sessions, func(ls lockSession) bool { return ls.granted }) {
		l := Leadership{}
		err := s.reads.QueryRowContext(ctx, `SELECT engine_id, version, address, epoch, since FROM leadership
			WHERE lock_key = $1`, s.key).Scan(&l.Engine.ID, &l.Engine.Version, &l.Engine.Address, &l.Epoch, &l.Since)
		switch {
		case err == nil:
			lead = &l
		case !errors.Is(err, sql.ErrNoRows):
			return nil, fmt.Errorf("engines: %w", err)
		}
	}
	return engineStatuses(sessions, lead), nil
}

// lockSession is a session holding or waiting for the lock: its application_name, and since when it holds or waits.
type lockSession struct {
	name    string
	granted bool
	since   time.Time
}

// engineStatuses returns the engines among the sessions, in their order (the query's: the holder first), the
// holder completed by the leadership row's version, address, epoch and since when the row is its own.
func engineStatuses(sessions []lockSession, lead *Leadership) []EngineStatus {
	var out []EngineStatus
	for _, ls := range sessions {
		e, ok := parseApplicationName(ls.name)
		if !ok {
			continue
		}
		es := EngineStatus{Engine: e, Active: ls.granted, Since: ls.since}
		if ls.granted && lead != nil && lead.Engine.ID == e.ID {
			// The leader's row: its full version (the session's name may be cut short), address, epoch and since.
			es.Engine, es.Epoch, es.Since = lead.Engine, lead.Epoch, lead.Since
		}
		out = append(out, es)
	}
	return out
}
