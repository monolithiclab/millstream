package postgres

import (
	"context"
	"strings"
	"testing"
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
