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

// sse writes a stream's events and its heartbeats.
type sse struct {
	w http.ResponseWriter
	f http.Flusher
}

// startSSE answers the stream's headers.
func startSSE(w http.ResponseWriter) (*sse, error) {
	f, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("live: the response cannot stream")
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no") // a buffering proxy would hold the events back
	w.WriteHeader(http.StatusOK)
	f.Flush()
	return &sse{w: w, f: f}, nil
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
	if _, err := s.w.Write([]byte(b.String())); err != nil {
		return err
	}
	s.f.Flush()
	return nil
}

func (s *sse) heartbeat() error {
	if _, err := s.w.Write([]byte(": heartbeat\n\n")); err != nil {
		return err
	}
	s.f.Flush()
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

// stream sends what c delivers with send, and a heartbeat every Heartbeat, until the request ends, done closes or
// a write fails (the client is gone).
func stream[T any](s *Server, r *http.Request, out *sse, done <-chan struct{}, c <-chan T, send func(T) error) {
	beat := s.opts.Clock.NewTimer(s.opts.Heartbeat)
	defer func() { beat.Stop() }()
	for {
		var err error
		select {
		case <-r.Context().Done():
			return
		case <-done:
			return
		case v := <-c:
			err = send(v)
		case <-beat.C():
			err = out.heartbeat()
			beat = s.opts.Clock.NewTimer(s.opts.Heartbeat)
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
	send := func(snap PipelineSnapshot) error { return out.event(sseSnapshot, "", snap) }
	if send(first) == nil {
		stream(s, r, out, sub.Done(), sub.C(), send)
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
	run, err := s.st.Run(r.Context(), p, r.PathValue("run"))
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
	// Done closes when the subscriber is dropped for falling behind or the broker stops: the client resumes.
	stream(s, r, out, sub.Done(), sub.C(), send)
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
	stream(s, r, out, sub.Done(), sub.C(), send)
	return nil
}
