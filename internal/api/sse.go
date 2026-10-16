package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// DefaultHeartbeat is how often a quiet stream sends a comment, so proxies and clients see it alive.
const DefaultHeartbeat = 15 * time.Second

// writeTimeout bounds each write of a stream: a client that stops reading is cut off rather than holding the
// handler (the server's read deadline does not apply to streams, which read nothing once open).
const writeTimeout = 10 * time.Second

// The server-sent events of the live streams (spec section 12.1). A stream answers 200 with text/event-stream once
// its scope is checked; a scope that does not exist answers a JSON error before.
const (
	// sseState is a state event of a run's or an item's stream: its id is the event's sequence number.
	sseState = "state"
	// sseSnapshot is a pipeline's step snapshot: no id, a snapshot is not resumable.
	sseSnapshot = "snapshot"
	// sseSnapshotRequired ends a stream whose cursor cannot be resumed: the client reloads through the REST API.
	sseSnapshotRequired = "snapshotRequired"
	// sseLog is a log line: its id is the log ring's.
	sseLog = "log"
	// sseGap says log lines were lost since the client's cursor.
	sseGap = "gap"
	// sseHeartbeat is a state stream's heartbeat: data {}, with the stream's position as its id when it has one.
	sseHeartbeat = "heartbeat"
)

// Event is an event of the log as a state event carries it.
type Event struct {
	Seq      int64           `json:"seq"`
	TS       time.Time       `json:"ts"`
	Pipeline string          `json:"pipeline"`
	RunID    string          `json:"runId,omitempty"`
	Kind     string          `json:"kind"`
	Data     json.RawMessage `json:"data"`
}

// sse writes a stream's events and its heartbeats, each write under its own deadline.
type sse struct {
	w  http.ResponseWriter
	f  http.Flusher
	rc *http.ResponseController
}

// startSSE answers the stream's headers.
func startSSE(w http.ResponseWriter) (*sse, error) {
	f, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("live: the response cannot stream")
	}
	rc := http.NewResponseController(w)
	// A stream outlives the server's read timeout, meant for reading requests.
	if err := rc.SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return nil, err
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no") // a buffering proxy would hold the events back
	s := &sse{w: w, f: f, rc: rc}
	if err := s.deadline(); err != nil {
		return nil, err
	}
	w.WriteHeader(http.StatusOK)
	f.Flush()
	return s, nil
}

// deadline sets the deadline of the next write; a writer without deadlines (a test's recorder) has none.
func (s *sse) deadline() error {
	if err := s.rc.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// write writes raw stream text under a fresh deadline and flushes it.
func (s *sse) write(text string) error {
	if err := s.deadline(); err != nil {
		return err
	}
	if _, err := s.w.Write([]byte(text)); err != nil {
		return err
	}
	s.f.Flush()
	return nil
}

// event writes one event; data is JSON-encoded. An id of "" is left out.
func (s *sse) event(name, id string, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	var b strings.Builder
	if id != "" {
		b.WriteString("id: " + id + "\n")
	}
	b.WriteString("event: " + name + "\ndata: ")
	b.Write(body) // JSON has no newline: one data line
	b.WriteString("\n\n")
	return s.write(b.String())
}

func (s *sse) heartbeat() error { return s.write(": heartbeat\n\n") }

// cursor sends a heartbeat event carrying the stream's position as its id, so a quiet stream resumes from where the
// log is rather than from its last event in scope, which may fall out of retention. While events are queued (they
// carry their own ids) the heartbeat has no id; with always unset, nothing is sent then.
func (s *sse) cursor(sub *Subscription, always bool) error {
	seq, ok := sub.Cursor()
	switch {
	case ok:
		return s.write("event: " + sseHeartbeat + "\nid: " + strconv.FormatInt(seq, 10) + "\ndata: {}\n\n")
	case always:
		return s.write("event: " + sseHeartbeat + "\ndata: {}\n\n")
	}
	return nil
}

// cursor is the client's last event id: the Last-Event-ID header, or the lastEventId query parameter, which a
// client opening a new EventSource to resume sends instead.
func cursor(r *http.Request) string {
	if id := r.Header.Get("Last-Event-ID"); id != "" {
		return id
	}
	return r.URL.Query().Get("lastEventId")
}

// stream sends what c delivers with send, and beat every Heartbeat, until the request ends, done closes or a write
// fails (the client is gone).
func stream[T any](s *Server, r *http.Request, done <-chan struct{}, c <-chan T, send func(T) error,
	beat func() error,
) {
	timer := s.opts.Clock.NewTimer(s.opts.Heartbeat)
	defer func() { timer.Stop() }()
	for {
		var err error
		select {
		case <-r.Context().Done():
			return
		case <-done:
			return
		case v := <-c:
			err = send(v)
		case <-timer.C():
			err = beat()
			timer = s.opts.Clock.NewTimer(s.opts.Heartbeat)
		}
		if err != nil {
			return
		}
	}
}

func (s *Server) livePipeline(w http.ResponseWriter, r *http.Request) error {
	if s.opts.Broker == nil {
		return fmt.Errorf("live: no broker: %w", errUnavailable)
	}
	sub, first, err := s.opts.Broker.SubscribePipeline(r.Context(), r.PathValue("pipeline"))
	if err != nil {
		return err
	}
	defer sub.Close()
	out, err := startSSE(w)
	if err != nil {
		return err
	}
	// A snapshot the broker took before the first one may still arrive: the stream never goes back in time.
	var last PipelineSnapshot
	send := func(snap PipelineSnapshot) error {
		if snap.Seq < last.Seq || (snap.Seq == last.Seq && snap.At.Before(last.At)) {
			return nil
		}
		last = snap
		return out.event(sseSnapshot, "", snap)
	}
	if send(first) == nil {
		stream(s, r, sub.Done(), sub.C(), send, out.heartbeat)
	}
	return nil
}

// scopePipeline is the pipeline query parameter of a run's or an item's stream: run and item ids are unique
// within a pipeline only.
func scopePipeline(r *http.Request) (string, error) {
	p := r.URL.Query().Get("pipeline")
	if p == "" {
		return "", invalidf("pipeline: required, run and item ids are unique within a pipeline")
	}
	return p, nil
}

func (s *Server) liveRun(w http.ResponseWriter, r *http.Request) error {
	p, err := scopePipeline(r)
	if err != nil {
		return err
	}
	run, err := s.opts.Store.Run(r.Context(), p, r.PathValue("run"))
	if err != nil {
		return err
	}
	return s.streamEvents(w, r, Filter{Pipeline: p, RunID: run.ID})
}

func (s *Server) liveItem(w http.ResponseWriter, r *http.Request) error {
	p, err := scopePipeline(r)
	if err != nil {
		return err
	}
	it, err := s.item(r.Context(), p, r.PathValue("item"))
	if err != nil {
		return err
	}
	return s.streamEvents(w, r, Filter{Pipeline: p, ItemID: it.ID})
}

// streamEvents streams the state events of a scope, resuming after the client's cursor.
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request, f Filter) error {
	if s.opts.Broker == nil {
		return fmt.Errorf("live: no broker: %w", errUnavailable)
	}
	var seq int64
	id := cursor(r)
	resume := id != ""
	if resume {
		var err error
		if seq, err = strconv.ParseInt(id, 10, 64); err != nil {
			seq = -1 // not one of ours: Subscribe answers snapshot required
		}
	}
	sub, err := s.opts.Broker.Subscribe(r.Context(), f, seq, resume)
	if errors.Is(err, ErrSnapshotRequired) {
		out, err := startSSE(w)
		if err != nil {
			return err
		}
		_ = out.event(sseSnapshotRequired, "", map[string]string{"lastEventId": id}) // the stream ends either way
		return nil
	}
	if err != nil {
		return err
	}
	defer sub.Close()
	out, err := startSSE(w)
	if err != nil {
		return err
	}
	send := func(ev store.Event) error {
		return out.event(sseState, strconv.FormatInt(ev.Seq, 10), Event(ev))
	}
	for _, ev := range sub.Backlog {
		if send(ev) != nil {
			return nil
		}
	}
	if out.cursor(sub, false) != nil {
		return nil
	}
	beat := func() error { return out.cursor(sub, true) }
	// Done closes when the subscriber is dropped for falling behind or the broker stops: the client resumes.
	stream(s, r, sub.Done(), sub.C(), send, beat)
	return nil
}

func (s *Server) liveLogs(w http.ResponseWriter, r *http.Request) error {
	if s.opts.Logs == nil {
		return fmt.Errorf("live logs: no log relay: %w", errUnavailable)
	}
	q := r.URL.Query()
	sub := s.opts.Logs.Subscribe(LogFilter{Pipeline: q.Get("pipeline"), RunID: q.Get("run"), Step: q.Get("step"),
		ItemID: q.Get("item"), DeliveryID: q.Get("delivery")}, cursor(r))
	defer sub.Close()
	out, err := startSSE(w)
	if err != nil {
		return err
	}
	if sub.Gap {
		if out.event(sseGap, "", map[string]string{"lastEventId": cursor(r)}) != nil {
			return nil
		}
	}
	send := func(l LogLine) error { return out.event(sseLog, s.opts.Logs.ID(l), l) }
	for _, l := range sub.Backlog {
		if send(l) != nil {
			return nil
		}
	}
	stream(s, r, sub.Done(), sub.C(), send, out.heartbeat)
	return nil
}
