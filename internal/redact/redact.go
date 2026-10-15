// Package redact removes secret values from text before it is logged or stored (spec section 4.5): every occurrence
// of a secret's value becomes [redacted:<name>]. Values shorter than MinLen are left alone, since replacing every
// "1" or "on" would destroy logs without protecting anything. A nil *Redactor redacts nothing, so callers need no
// checks when a pipeline has no secrets.
package redact

import (
	"cmp"
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
)

// MinLen is the shortest secret value redacted.
const MinLen = 4

// Redactor replaces secret values by their names. It is immutable and safe for concurrent use.
type Redactor struct {
	r *strings.Replacer
}

// New returns a redactor for the secrets, by name, or nil when none is long enough to redact.
func New(secrets map[string]string) *Redactor {
	names := slices.Collect(maps.Keys(secrets))
	// Longest value first, so a secret containing another is replaced whole; then by name, for a stable order.
	slices.SortFunc(names, func(a, b string) int {
		return cmp.Or(cmp.Compare(len(secrets[b]), len(secrets[a])), cmp.Compare(a, b))
	})
	var pairs []string
	for _, name := range names {
		if v := secrets[name]; len(v) >= MinLen {
			pairs = append(pairs, v, "[redacted:"+name+"]")
		}
	}
	if len(pairs) == 0 {
		return nil
	}
	return &Redactor{r: strings.NewReplacer(pairs...)}
}

// String returns s with every secret value replaced.
func (r *Redactor) String(s string) string {
	if r == nil {
		return s
	}
	return r.r.Replace(s)
}

// Logger returns l with its records redacted: the message and every string value, in groups too. A nil redactor
// returns l itself.
func (r *Redactor) Logger(l *slog.Logger) *slog.Logger {
	if r == nil {
		return l
	}
	return slog.New(&handler{h: l.Handler(), r: r})
}

// handler is a slog.Handler that redacts before passing records on.
type handler struct {
	h slog.Handler
	r *Redactor
}

func (h *handler) Enabled(ctx context.Context, level slog.Level) bool { return h.h.Enabled(ctx, level) }

func (h *handler) Handle(ctx context.Context, rec slog.Record) error {
	out := slog.NewRecord(rec.Time, rec.Level, h.r.String(rec.Message), rec.PC)
	rec.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(h.attr(a))
		return true
	})
	return h.h.Handle(ctx, out)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	red := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		red[i] = h.attr(a)
	}
	return &handler{h: h.h.WithAttrs(red), r: h.r}
}

func (h *handler) WithGroup(name string) slog.Handler {
	return &handler{h: h.h.WithGroup(name), r: h.r}
}

func (h *handler) attr(a slog.Attr) slog.Attr {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindString:
		return slog.String(a.Key, h.r.String(v.String()))
	case slog.KindGroup:
		group := v.Group()
		red := make([]any, len(group))
		for i, g := range group {
			red[i] = h.attr(g)
		}
		return slog.Group(a.Key, red...)
	case slog.KindAny:
		if err, ok := v.Any().(error); ok {
			return slog.String(a.Key, h.r.String(err.Error()))
		}
	}
	return slog.Attr{Key: a.Key, Value: v}
}
