package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestRebind(t *testing.T) {
	for _, tt := range []struct{ in, want string }{
		{`SELECT 1`, `SELECT 1`},
		{`SELECT ? + ?`, `SELECT $1 + $2`},
		{`WHERE a = ?1 AND b = ?2 OR a = ?1`, `WHERE a = $1 AND b = $2 OR a = $1`},
		{`SELECT '?', "a?b", ? FROM t`, `SELECT '?', "a?b", $1 FROM t`},
		{`SELECT 'it''s ?', ?`, `SELECT 'it''s ?', $1`},
		{`LIMIT ?+0`, `LIMIT $1+0`},
	} {
		if got := Rebind(tt.in); got != tt.want {
			t.Errorf("Rebind(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// The lock key is fixed: engines of two builds on one deployment must contend for the same lock.
func TestLockKey(t *testing.T) {
	if LockKey("") != 1753398078206285170 || LockKey("millstream") != LockKey("") {
		t.Errorf("the default deployment's lock key is %d", LockKey(""))
	}
	if LockKey("a") == LockKey("b") {
		t.Error("two deployments share a lock key")
	}
	if s := (&Store{key: LockKey("a")}); s.LockKey() != LockKey("a") {
		t.Error("a store reports another lock than it holds")
	}
}

// A DSN can hold a password: no error of Open repeats it.
func TestOpenNeverPrintsTheDSN(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // no connection is attempted for long
	for _, dsn := range []string{
		"postgres://millstream:s3cret-pw@db.invalid:notaport/millstream",
		"host=127.0.0.1 port=1 user=millstream password=s3cret-pw dbname=millstream connect_timeout=1",
	} {
		for name, open := range map[string]func() error{
			"Open":         func() error { _, err := Open(ctx, dsn, Options{}); return err },
			"OpenReadOnly": func() error { _, err := OpenReadOnly(ctx, dsn, Options{}); return err },
		} {
			err := open()
			if err == nil {
				t.Fatalf("%s(%q) succeeded", name, dsn)
			}
			if strings.Contains(err.Error(), "s3cret-pw") {
				t.Errorf("%s's error shows the password: %v", name, err)
			}
		}
	}
}

func Test_redact(t *testing.T) {
	dsn := "postgres://u:pa55word@h/db"
	for _, msg := range []string{"cannot connect to " + dsn, "bad password pa55word"} {
		err := redact(errDSN(msg), dsn, "pa55word")
		if strings.Contains(err.Error(), "pa55word") || !strings.Contains(err.Error(), "<redacted>") {
			t.Errorf("redact(%q) = %v", msg, err)
		}
	}
	if err := redact(errDSN("other"), "", dsn); err.Error() != "other" {
		t.Errorf("redact without a secret in the text = %v", err)
	}
}

type errDSN string

func (e errDSN) Error() string { return string(e) }

// An engine's write session carries its id and version, cut to what Postgres keeps; a command's carries neither.
func TestApplicationName(t *testing.T) {
	if got := applicationName(Engine{}); got != "millstream" {
		t.Errorf("a command's session: %q", got)
	}
	name := applicationName(Engine{ID: "mill-01-abc", Version: "v1.2.3"})
	if e, ok := parseApplicationName(name); !ok || e != (Engine{ID: "mill-01-abc", Version: "v1.2.3"}) {
		t.Errorf("%q parses as %+v, %v", name, e, ok)
	}
	long := applicationName(Engine{ID: "id", Version: strings.Repeat("v", 100)})
	if len(long) != maxApplicationName {
		t.Errorf("a long name keeps %d bytes", len(long))
	}
	for _, other := range []string{"millstream", "millstream-read", "millstream-engine ", "psql"} {
		if _, ok := parseApplicationName(other); ok {
			t.Errorf("%q parses as an engine", other)
		}
	}
}

// The write session's watch settings follow the periods: keepalives every k, the user and transaction timeouts at
// 3k, silence ended after CheckEvery + 2k, whole seconds at least one, milliseconds within Postgres's range.
func TestWatchSession(t *testing.T) {
	cfg, err := pgx.ParseConfig("postgres://u@localhost/db")
	if err != nil {
		t.Fatal(err)
	}
	watchSession(cfg, Options{CheckEvery: 2 * time.Second, Keepalive: 1500 * time.Millisecond})
	want := map[string]string{"tcp_keepalives_idle": "2", "tcp_keepalives_interval": "2", "tcp_keepalives_count": "2",
		"tcp_user_timeout": "4500", "idle_session_timeout": "5000", "idle_in_transaction_session_timeout": "4500"}
	for k, v := range want {
		if cfg.RuntimeParams[k] != v {
			t.Errorf("%s = %q, want %q", k, cfg.RuntimeParams[k], v)
		}
	}
	if cfg.ConnectTimeout != 4500*time.Millisecond || cfg.DialFunc == nil {
		t.Errorf("connect timeout %s, dialer set %v", cfg.ConnectTimeout, cfg.DialFunc != nil)
	}
	if millis(time.Duration(1<<62)) != "2147483647" || millis(0) != "1" {
		t.Error("millis does not clamp")
	}
}

// The engines of the lock's sessions, in order, the holder completed by its own leadership row only; sessions that
// are not an engine's are left out.
func TestEngineStatuses(t *testing.T) {
	t0 := time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)
	waiting := lockSession{name: applicationName(Engine{ID: "b", Version: "v2"}), since: t0.Add(time.Minute)}
	holder := lockSession{name: applicationName(Engine{ID: "a", Version: "v1"}), granted: true, since: t0}
	lead := &Leadership{Engine: Engine{ID: "a", Version: "v1-full", Address: "h:1"}, Epoch: 7, Since: t0.Add(-time.Hour)}
	got := engineStatuses([]lockSession{holder, {name: "millstream", granted: false}, waiting}, lead)
	want := []EngineStatus{
		{Engine: lead.Engine, Active: true, Since: lead.Since, Epoch: 7},
		{Engine: Engine{ID: "b", Version: "v2"}, Since: waiting.since},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("engines %+v, want %+v", got, want)
	}
	// Another engine's row (a holder that has not recorded itself yet) is not this holder's.
	got = engineStatuses([]lockSession{holder}, &Leadership{Engine: Engine{ID: "z"}, Epoch: 9})
	if len(got) != 1 || got[0].Epoch != 0 || !got[0].Since.Equal(t0) {
		t.Fatalf("engines with another's row %+v", got)
	}
	if got := engineStatuses([]lockSession{{name: "millstream", granted: true}}, nil); len(got) != 0 {
		t.Fatalf("a command's session listed: %+v", got)
	}
}

// Resource, connection and startup or shutdown errors are the database being unavailable (store.ErrUnavailable,
// 503); any other error is not.
func Test_unavailable(t *testing.T) {
	for code, want := range map[string]bool{"53300": true, "53100": true, "08006": true, "08001": true,
		"57P01": true, "57P03": true, "57014": false, "23505": false, "40001": false} {
		if got := unavailable(fmt.Errorf("unit: %w", &pgconn.PgError{Code: code})); got != want {
			t.Errorf("unavailable(%s) = %v", code, got)
		}
	}
	_, err := pgconn.Connect(t.Context(), "postgres://u@127.0.0.1:1/db?connect_timeout=1&sslmode=disable")
	if err == nil || !unavailable(err) {
		t.Errorf("a connection refused: %v", err)
	}
	if unavailable(errors.New("other")) {
		t.Error("an unrelated error is unavailable")
	}
}
