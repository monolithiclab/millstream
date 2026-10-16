package nodeapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/monolithiclab/millstream/internal/nodewire"
)

// The log relay (spec section 10.3): the node's lines, its deliveries' and its actions' stderr among them, go to its
// own log and, every second, to the engine's log and live tail. Lines wait while the engine is unreachable, the
// oldest dropped beyond maxRelay.

// LogHandler returns a handler writing to next and queueing every record for the relay.
func (t *Client) LogHandler(next slog.Handler) slog.Handler {
	return &relayHandler{t: t, next: next}
}

type relayHandler struct {
	t      *Client
	next   slog.Handler
	attrs  []slog.Attr
	prefix string // the open groups, dot-joined
}

func (h *relayHandler) Enabled(ctx context.Context, l slog.Level) bool { return h.next.Enabled(ctx, l) }

func (h *relayHandler) Handle(ctx context.Context, r slog.Record) error {
	line := nodewire.LogLine{Time: r.Time, Level: r.Level.String(), Msg: r.Message, Attrs: map[string]any{}}
	for _, a := range h.attrs {
		addAttr(line.Attrs, "", a)
	}
	r.Attrs(func(a slog.Attr) bool {
		addAttr(line.Attrs, h.prefix, a)
		return true
	})
	h.t.enqueue(line)
	return h.next.Handle(ctx, r)
}

func (h *relayHandler) WithAttrs(as []slog.Attr) slog.Handler {
	c := *h
	c.next = h.next.WithAttrs(as)
	c.attrs = append(append([]slog.Attr{}, h.attrs...), prefixed(h.prefix, as)...)
	return &c
}

func (h *relayHandler) WithGroup(name string) slog.Handler {
	c := *h
	c.next = h.next.WithGroup(name)
	c.prefix = h.prefix + name + "."
	return &c
}

func prefixed(prefix string, as []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, 0, len(as))
	for _, a := range as {
		out = append(out, slog.Attr{Key: prefix + a.Key, Value: a.Value})
	}
	return out
}

// addAttr adds a, flattened, under prefix.
func addAttr(m map[string]any, prefix string, a slog.Attr) {
	v := a.Value.Resolve()
	if v.Kind() == slog.KindGroup {
		for _, g := range v.Group() {
			addAttr(m, prefix+a.Key+".", g)
		}
		return
	}
	switch v.Kind() {
	case slog.KindString, slog.KindInt64, slog.KindUint64, slog.KindFloat64, slog.KindBool:
		m[prefix+a.Key] = v.Any()
	default:
		m[prefix+a.Key] = v.String()
	}
}

func (t *Client) enqueue(l nodewire.LogLine) {
	t.relayMu.Lock()
	defer t.relayMu.Unlock()
	if len(t.relay) == maxRelay {
		t.relay = t.relay[1:]
		t.relayDropped++
	}
	t.relay = append(t.relay, l)
}

// RunRelay sends the queued lines every second until ctx ends, then once more.
func (t *Client) RunRelay(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			flush, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			t.flushRelay(flush)
			cancel()
			return
		case <-tick.C:
			t.flushRelay(ctx)
		}
	}
}

func (t *Client) flushRelay(ctx context.Context) {
	t.relayMu.Lock()
	lines, dropped := t.relay, t.relayDropped
	t.relay, t.relayDropped = nil, 0
	t.relayMu.Unlock()
	if dropped > 0 {
		lines = append([]nodewire.LogLine{{Time: time.Now(), Level: slog.LevelWarn.String(),
			Msg: "log lines dropped while the engine was unreachable", Attrs: map[string]any{"dropped": dropped}}},
			lines...)
	}
	if len(lines) == 0 {
		return
	}
	if err := t.call(ctx, http.MethodPost, nodewire.PathLogs, nil,
		nodewire.Logs{Node: t.opts.Node.Node, Lines: lines}, nil, false, attemptTimeout); err != nil {
		t.relayMu.Lock()
		t.relay = append(lines, t.relay...)
		if over := len(t.relay) - maxRelay; over > 0 {
			t.relay, t.relayDropped = t.relay[over:], t.relayDropped+over
		}
		t.relayMu.Unlock()
	}
}
