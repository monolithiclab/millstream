package redact

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestString(t *testing.T) {
	t.Parallel()
	r := New(map[string]string{"token": "s3cr3t-value", "inner": "cr3t", "pin": "123", "empty": ""})
	tests := []struct{ in, want string }{
		{"no secret here", "no secret here"},
		{"auth s3cr3t-value failed", "auth [redacted:token] failed"},
		{"just cr3t and 123", "just [redacted:inner] and 123"},               // 123 is shorter than MinLen
		{"cr3t then s3cr3t-value", "[redacted:inner] then [redacted:token]"}, // the longer value wins
	}
	for _, tt := range tests {
		if got := r.String(tt.in); got != tt.want {
			t.Errorf("String(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// JSON redacts decoded strings, keys included, whatever escaping the document used, and keeps numbers as written.
func TestJSON(t *testing.T) {
	t.Parallel()
	r := New(map[string]string{"token": "s<3>cr3t"})
	got, err := r.JSON([]byte(`{"a": ["x s\u003c3\u003ecr3t", 1.50], "s<3>cr3t": {"n": 7}}`))
	if want := `{"[redacted:token]":{"n":7},"a":["x [redacted:token]",1.50]}`; err != nil || string(got) != want {
		t.Fatalf("JSON = %s, %v; want %s", got, err, want)
	}
	if _, err := r.JSON([]byte(`{`)); err == nil {
		t.Fatal("JSON of a broken document succeeded")
	}
	var none *Redactor
	if got, err := none.JSON([]byte(`{"b":1, "a":2}`)); err != nil || string(got) != `{"b":1, "a":2}` {
		t.Fatalf("nil JSON = %s, %v", got, err)
	}
}

func TestNilRedactor(t *testing.T) {
	t.Parallel()
	var r *Redactor
	if r != New(nil) || New(map[string]string{"pin": "123"}) != nil {
		t.Fatal("New without a redactable secret returned a redactor")
	}
	if got := r.String("s3cr3t"); got != "s3cr3t" {
		t.Fatalf("nil String = %q", got)
	}
	l := slog.Default()
	if r.Logger(l) != l {
		t.Fatal("nil Logger wrapped the logger")
	}
}

func TestLogger(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	r := New(map[string]string{"token": "s3cr3t-value"})
	l := r.Logger(slog.New(slog.NewJSONHandler(&buf, nil))).With("with", "pre s3cr3t-value")
	l.WithGroup("g").Info("line s3cr3t-value", "s", "x s3cr3t-value", "err", errors.New("e s3cr3t-value"),
		slog.Group("sub", "deep", "s3cr3t-value"), "n", 7)
	out := buf.String()
	if strings.Contains(out, "s3cr3t-value") {
		t.Fatalf("secret logged: %s", out)
	}
	for _, want := range []string{`"msg":"line [redacted:token]"`, `"with":"pre [redacted:token]"`,
		`"s":"x [redacted:token]"`, `"err":"e [redacted:token]"`, `"deep":"[redacted:token]"`, `"n":7`} {
		if !strings.Contains(out, want) {
			t.Errorf("log %s lacks %s", out, want)
		}
	}
	if r.Logger(slog.New(slog.DiscardHandler)).Handler().Enabled(t.Context(), slog.LevelError) {
		t.Error("Enabled not delegated to the wrapped handler")
	}
}
