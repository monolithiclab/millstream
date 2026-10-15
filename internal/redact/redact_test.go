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
