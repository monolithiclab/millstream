package api

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultLogLines is how many log lines a LogRing keeps when its size is not given.
const DefaultLogLines = 2000

// logBuffer is how many lines a log subscriber may fall behind before it is dropped.
const logBuffer = 256

// LogLine is a log line of a delivery (spec section 10.3): an action's stderr line, re-emitted by the node, or the
// node's own line about the delivery.
type LogLine struct {
	Seq        int64     `json:"-"`
	TS         time.Time `json:"ts"`
	Level      string    `json:"level"`
	Message    string    `json:"message"`
	Pipeline   string    `json:"pipeline"`
	RunID      string    `json:"runId"`
	Step       string    `json:"step"`
	ItemID     string    `json:"itemId"`
	DeliveryID string    `json:"deliveryId"`
	Attempt    int       `json:"attempt"`
	Node       string    `json:"node,omitempty"`
}

// LogFilter is the scope of a log subscription; empty fields match everything.
type LogFilter struct {
	Pipeline, RunID, Step, ItemID, DeliveryID string
}

func (f LogFilter) matches(l LogLine) bool {
	return (f.Pipeline == "" || l.Pipeline == f.Pipeline) && (f.RunID == "" || l.RunID == f.RunID) &&
		(f.Step == "" || l.Step == f.Step) && (f.ItemID == "" || l.ItemID == f.ItemID) &&
		(f.DeliveryID == "" || l.DeliveryID == f.DeliveryID)
}

// LogRing is the live log tail (spec section 10.3): the last lines of the deliveries the embedded node runs, kept
// in memory, never stored, and relayed to subscribers. It is best effort: a line id is the ring's epoch (the
// process) and the line's number, so a client reconnecting gets the lines it missed while the ring still holds them,
// and a gap marker when it does not, the ring having moved on or the process restarted.
type LogRing struct {
	mu    sync.Mutex
	epoch string
	lines []LogLine // a ring of cap(lines) once full; next is where the next line goes
	next  int
	seq   int64
	subs  map[*LogSubscription]struct{}
}

// NewLogRing returns a ring keeping the last size lines (DefaultLogLines when size is not positive), whose ids
// start a new epoch at now.
func NewLogRing(size int, now time.Time) *LogRing {
	if size <= 0 {
		size = DefaultLogLines
	}
	return &LogRing{epoch: strconv.FormatInt(now.UnixNano(), 36), lines: make([]LogLine, 0, size),
		subs: map[*LogSubscription]struct{}{}}
}

// LogSubscription receives the log lines of its scope.
type LogSubscription struct {
	r      *LogRing
	filter LogFilter
	c      chan LogLine
	done   chan struct{}
	// Backlog is the retained lines of the scope after the client's cursor (all of them without one), in order.
	Backlog []LogLine
	// Gap says lines of the scope may have been lost since the client's cursor.
	Gap bool
}

// C delivers the lines after the backlog.
func (s *LogSubscription) C() <-chan LogLine { return s.c }

// Done is closed when the subscription ends: closed, or dropped for falling behind.
func (s *LogSubscription) Done() <-chan struct{} { return s.done }

// Close ends the subscription.
func (s *LogSubscription) Close() {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	s.r.dropLocked(s)
}

func (r *LogRing) dropLocked(s *LogSubscription) {
	if _, ok := r.subs[s]; ok {
		delete(r.subs, s)
		close(s.done)
	}
}

// ID is a line's event id: the ring's epoch and the line's number.
func (r *LogRing) ID(l LogLine) string { return r.epoch + "." + strconv.FormatInt(l.Seq, 10) }

// Add numbers a line, keeps it and relays it to the subscribers it matches, dropping those that fell behind.
func (r *LogRing) Add(l LogLine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	l.Seq = r.seq
	if len(r.lines) < cap(r.lines) {
		r.lines = append(r.lines, l)
	} else {
		r.lines[r.next] = l
		r.next = (r.next + 1) % cap(r.lines)
	}
	for s := range r.subs {
		if !s.filter.matches(l) {
			continue
		}
		select {
		case s.c <- l:
		default:
			r.dropLocked(s)
		}
	}
}

// retained returns the lines kept, oldest first.
func (r *LogRing) retained() []LogLine {
	if len(r.lines) < cap(r.lines) {
		return slices.Clone(r.lines)
	}
	return append(slices.Clone(r.lines[r.next:]), r.lines[:r.next]...)
}

// Subscribe subscribes to the lines of a scope. cursor is the id of the last line the client received, empty for a
// first connection: the backlog then holds the retained lines of the scope.
func (r *LogRing) Subscribe(f LogFilter, cursor string) *LogSubscription {
	s := &LogSubscription{r: r, filter: f, c: make(chan LogLine, logBuffer), done: make(chan struct{})}
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.retained()
	var after int64
	if cursor != "" {
		epoch, n, _ := strings.Cut(cursor, ".")
		seq, err := strconv.ParseInt(n, 10, 64)
		switch {
		case epoch != r.epoch || err != nil || seq > r.seq: // another process's id, or not one of ours
			s.Gap = true
		case len(kept) > 0 && seq < kept[0].Seq-1: // lines after the cursor fell off the ring
			s.Gap, after = true, seq
		default:
			after = seq
		}
	}
	for _, l := range kept {
		if l.Seq > after && f.matches(l) {
			s.Backlog = append(s.Backlog, l)
		}
	}
	r.subs[s] = struct{}{}
	return s
}

// Handler returns a slog.Handler passing every record on to next, and adding to the ring the records of a
// delivery: those carrying a deliveryId, as the node's logger of a delivery does (spec section 10.3). Records reach
// it after the node's redaction, so the ring holds no secret.
func (r *LogRing) Handler(next slog.Handler) slog.Handler { return &logHandler{next: next, ring: r} }

type logHandler struct {
	next  slog.Handler
	ring  *LogRing
	attrs []slog.Attr // the top-level attributes added by With
	group bool        // a group was opened: later attributes are not top-level
}

func (h *logHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *logHandler) Handle(ctx context.Context, rec slog.Record) error {
	err := h.next.Handle(ctx, rec)
	l := LogLine{TS: rec.Time, Level: rec.Level.String(), Message: rec.Message}
	set := func(a slog.Attr) bool {
		v := a.Value.Resolve()
		switch a.Key {
		case "pipeline":
			l.Pipeline = v.String()
		case "runId":
			l.RunID = v.String()
		case "step":
			l.Step = v.String()
		case "itemId":
			l.ItemID = v.String()
		case "deliveryId":
			l.DeliveryID = v.String()
		case "attempt":
			if v.Kind() == slog.KindInt64 {
				l.Attempt = int(v.Int64())
			}
		case "node":
			l.Node = v.String()
		}
		return true
	}
	for _, a := range h.attrs {
		set(a)
	}
	if !h.group {
		rec.Attrs(set)
	}
	if l.DeliveryID != "" {
		l.Message = capLine(l.Message)
		h.ring.Add(l)
	}
	return err
}

// MaxLogLine is the most bytes of a line's message the ring keeps: an action printing a megabyte on one line costs
// the ring, and each client, no more than this.
const MaxLogLine = 8 << 10

// truncatedMark ends a message cut at MaxLogLine.
const truncatedMark = " …[truncated]"

func capLine(msg string) string {
	if len(msg) <= MaxLogLine {
		return msg
	}
	return strings.ToValidUTF8(msg[:MaxLogLine-len(truncatedMark)], "") + truncatedMark
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := &logHandler{next: h.next.WithAttrs(attrs), ring: h.ring, attrs: h.attrs, group: h.group}
	if !h.group {
		out.attrs = append(slices.Clone(h.attrs), attrs...)
	}
	return out
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	return &logHandler{next: h.next.WithGroup(name), ring: h.ring, attrs: h.attrs, group: true}
}
