package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// streamWriter is a ResponseWriter a test reads while the handler writes: every flush is signalled.
type streamWriter struct {
	mu      sync.Mutex
	header  http.Header
	code    int
	buf     bytes.Buffer
	flushed chan struct{}
	// gate, when set, holds every Write until it is closed: a client that does not read.
	gate chan struct{}
}

func newStreamWriter() *streamWriter {
	return &streamWriter{header: http.Header{}, flushed: make(chan struct{}, 1)}
}

func (w *streamWriter) Header() http.Header { return w.header }

func (w *streamWriter) WriteHeader(code int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.code = code
}

func (w *streamWriter) Write(b []byte) (int, error) {
	if w.gate != nil {
		<-w.gate
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return w.buf.Write(b)
}

func (w *streamWriter) Flush() {
	select {
	case w.flushed <- struct{}{}:
	default:
	}
}

func (w *streamWriter) status() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.code
}

func (w *streamWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// waitFor waits until the stream holds n events (blocks ending with a blank line) or a comment matching want.
func (w *streamWriter) waitFor(t *testing.T, cond func(string) bool) string {
	t.Helper()
	guard := time.NewTimer(5 * time.Second) // a guard, never reached when the code works
	defer guard.Stop()
	for {
		if s := w.String(); cond(s) {
			return s
		}
		select {
		case <-w.flushed:
		case <-guard.C:
			t.Fatalf("the stream never got there: %q", w.String())
		}
	}
}

// sseEvent is one event of a stream.
type sseEvent struct{ id, name, data string }

func parseSSE(s string) []sseEvent {
	var out []sseEvent
	for _, block := range strings.Split(s, "\n\n") {
		var ev sseEvent
		for _, line := range strings.Split(block, "\n") {
			switch k, v, _ := strings.Cut(line, ": "); k {
			case "id":
				ev.id = v
			case "event":
				ev.name = v
			case "data":
				ev.data = v
			}
		}
		if ev.name != "" {
			out = append(out, ev)
		}
	}
	return out
}

func events(n int) func(string) bool {
	return func(s string) bool { return len(parseSSE(s)) >= n }
}

// liveEnv is a fixture with a broker and a server streaming from it, the broker pumped by hand.
type liveEnv struct {
	*fixture
	b    *Broker
	logs *LogRing
	srv  *Server
}

// Fixtures of live envs.
const (
	ownFixture = iota // a clone of the test's own
	readOnly          // the shared fixture: the test changes nothing
	scratch           // the scratch clone: the test changes only what the others ignore
)

// newLiveEnv returns a live env over a fixture of the kind given.
func newLiveEnv(t *testing.T, opts BrokerOptions, kind int) *liveEnv {
	var f *fixture
	switch kind {
	case ownFixture:
		f = newFixture(t)
	case readOnly:
		f = readFixture(t)
	default:
		f = scratchFixture(t)
	}
	opts.Store = cmpStore(opts.Store, f.st)
	opts.Clock = f.clock
	b, err := NewBroker(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	logs := NewLogRing(4, t0)
	srv, err := New(Options{Store: f.st, Broker: b, Logs: logs, Clock: f.clock})
	if err != nil {
		t.Fatal(err)
	}
	return &liveEnv{fixture: f, b: b, logs: logs, srv: srv}
}

func cmpStore(a, b store.Store) store.Store {
	if a != nil {
		return a
	}
	return b
}

// liveStream is a live request being served.
type liveStream struct {
	w      *streamWriter
	cancel context.CancelFunc
	done   chan struct{}
}

// open starts serving a live request; close ends it and checks the answer against the OpenAPI document.
func (e *liveEnv) open(t *testing.T, target string, header map[string]string) *liveStream {
	t.Helper()
	return e.openWith(t, target, header, newStreamWriter())
}

func (e *liveEnv) openWith(t *testing.T, target string, header map[string]string, w *streamWriter) *liveStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
	for k, v := range header {
		r.Header.Set(k, v)
	}
	s := &liveStream{w: w, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		e.srv.ServeHTTP(s.w, r)
	}()
	t.Cleanup(func() {
		s.cancel()
		<-s.done
		rec := httptest.NewRecorder()
		copyHeader(rec.Header(), s.w.header)
		rec.WriteHeader(s.w.status())
		if strings.HasPrefix(s.w.header.Get("Content-Type"), "application/json") {
			rec.Body.WriteString(s.w.String())
		}
		_, p, _ := strings.Cut(r.Pattern, " ")
		theContract().validate(t, http.MethodGet, strings.TrimPrefix(p, Prefix), rec)
	})
	return s
}

func copyHeader(dst, src http.Header) {
	for k, v := range src {
		dst[k] = v
	}
}

// ids returns the ids of a stream's state events.
func ids(evs []sseEvent) []int64 {
	var out []int64
	for _, ev := range evs {
		if ev.name == sseState {
			n, _ := strconv.ParseInt(ev.id, 10, 64)
			out = append(out, n)
		}
	}
	return out
}

// runEvents returns the sequence numbers of a run's events after seq, from the store.
func (e *liveEnv) runEvents(t *testing.T, runID string, after int64) []int64 {
	t.Helper()
	evs := must(e.st.PipelineEvents(e.ctx, store.EventFilter{Pipeline: "listings", RunID: runID}))
	var out []int64
	for _, ev := range evs {
		if ev.Seq > after {
			out = append(out, ev.Seq)
		}
	}
	return out
}

// Review focus 2: a client that reconnects with its last event id after missing events receives exactly the
// events of its scope it missed, in order, then the live ones, with no duplicate.
func TestLiveResumption(t *testing.T) {
	e := newLiveEnv(t, BrokerOptions{}, ownFixture)
	ctx := context.Background()
	first := e.open(t, "/api/v1/live/runs/r1?pipeline=listings", nil)
	first.w.waitFor(t, func(string) bool { return first.w.status() != 0 })
	// r1's review is approved: events of r1, among events of another run (r5).
	e.fire("r5")
	must(e.st.DecideReview(ctx, store.ReviewDecisionRequest{Pipeline: "listings", DeliveryID: e.reviewDelivery,
		Version: 1, Decision: store.DecisionApprove, Reviewer: "ana"}))
	e.b.pump(ctx)
	got := ids(parseSSE(first.w.waitFor(t, events(2))))
	last := got[0] // the client received the first event only, then lost its connection
	first.cancel()
	<-first.done

	// While it is away: r1's dead letter is replayed and fails again, interleaved with r5's work.
	must(e.st.ReplayDeadLetters(ctx, "listings", "r1", nil, false))
	e.done(e.lease("crawl"), out("x", `{"price":1}`))
	l := e.lease("score")
	if l.RunID != "r1" {
		l2 := e.lease("score")
		e.done(l, out("", `{"score":1}`))
		l = l2
	}
	must(e.st.Fail(ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Class: "nonRetryable",
		Message: "again"}))
	e.b.pump(ctx)
	missed := e.runEvents(t, "r1", last)

	again := e.open(t, "/api/v1/live/runs/r1?pipeline=listings&lastEventId="+strconv.FormatInt(last, 10), nil)
	resumed := ids(parseSSE(again.w.waitFor(t, events(len(missed)))))
	if !slices.Equal(resumed, missed) {
		t.Fatalf("resumed with %v, want the missed %v", resumed, missed)
	}
	// Then the live events: r1's new dead letter discarded.
	open := must(e.st.DeadLetters(ctx, store.DeadLetterFilter{Pipeline: "listings", RunID: "r1",
		State: store.DeadLetterOpen}))
	must(e.st.DiscardDeadLetters(ctx, "listings", []string{open[0].ID}))
	e.b.pump(ctx)
	all := e.runEvents(t, "r1", last)
	resumed = ids(parseSSE(again.w.waitFor(t, events(len(all)))))
	if !slices.Equal(resumed, all) {
		t.Fatalf("stream %v, want %v", resumed, all)
	}

	// The header resumes as the query parameter does, and an item's stream carries its deliveries' events.
	item := e.open(t, "/api/v1/live/items/"+e.dead.ID+"?pipeline=listings", map[string]string{"Last-Event-ID": "0"})
	evs := parseSSE(item.w.waitFor(t, func(s string) bool { return strings.Contains(s, `"kind":"deliveryDead"`) }))
	for _, ev := range evs {
		var data Event
		if err := json.Unmarshal([]byte(ev.data), &data); err != nil {
			t.Fatal(err)
		}
		if eventItem(store.Event{Data: data.Data}) != e.dead.ID {
			t.Errorf("item stream event %s names another item: %s", ev.id, ev.data)
		}
	}
}

// A cursor the broker cannot resume answers one snapshotRequired event, then the stream ends.
func TestLiveSnapshotRequired(t *testing.T) {
	e := newLiveEnv(t, BrokerOptions{ResumeLimit: 3}, readOnly)
	head := must(e.st.LastEventSeq(e.ctx))
	for _, cursor := range []string{"1", strconv.FormatInt(head+5, 10), "x"} {
		s := e.open(t, "/api/v1/live/runs/r1?pipeline=listings&lastEventId="+cursor, nil)
		<-s.done // the stream ended by itself
		evs := parseSSE(s.w.String())
		if len(evs) != 1 || evs[0].name != sseSnapshotRequired || evs[0].data != `{"lastEventId":"`+cursor+`"}` {
			t.Errorf("cursor %s: %+v", cursor, evs)
		}
	}
	// Within the limit, it resumes.
	s := e.open(t, "/api/v1/live/runs/r2?pipeline=listings&lastEventId="+strconv.FormatInt(head-2, 10), nil)
	s.w.waitFor(t, func(string) bool { return s.w.status() == http.StatusOK })
	if evs := parseSSE(s.w.String()); slices.ContainsFunc(evs, func(ev sseEvent) bool { return ev.name != sseState }) {
		t.Errorf("resumed within the limit: %+v", evs)
	}
}

// gappedStore is a store whose events before from are gone, as retention will make them.
type gappedStore struct {
	store.Store
	from int64
}

func (g gappedStore) Events(ctx context.Context, after int64, limit int) ([]store.Event, error) {
	return g.Store.Events(ctx, max(after, g.from-1), limit)
}

func TestLiveRetentionGap(t *testing.T) {
	f := readFixture(t)
	b := must(NewBroker(BrokerOptions{Store: gappedStore{f.st, 10}, Clock: f.clock}))
	must(0, b.Start(f.ctx))
	if _, err := b.Subscribe(f.ctx, Filter{Pipeline: "listings"}, 2, true); err == nil || !strings.Contains(err.Error(),
		"gone") {
		t.Errorf("a cursor before the retained events: %v", err)
	}
	sub := must(b.Subscribe(f.ctx, Filter{Pipeline: "listings"}, 10, true))
	if len(sub.Backlog) == 0 || sub.Backlog[0].Seq != 11 {
		t.Errorf("backlog from 10 = %d events", len(sub.Backlog))
	}
	sub.Close()
	sub.Close() // twice is harmless
}

// A subscriber that falls behind is dropped: neither the writer nor the broker waits for it.
func TestLiveSlowClientDropped(t *testing.T) {
	e := newLiveEnv(t, BrokerOptions{Buffer: 1}, scratch)
	slow := must(e.b.Subscribe(e.ctx, Filter{Pipeline: "listings"}, 0, false))
	fast := must(e.b.Subscribe(e.ctx, Filter{Pipeline: "listings", RunID: "nope"}, 0, false))
	e.fire("r5") // several events, one buffered
	e.b.pump(e.ctx)
	select {
	case <-slow.Done():
	default:
		t.Fatal("the slow subscriber was not dropped")
	}
	select {
	case <-fast.Done():
		t.Fatal("a subscriber with nothing to receive was dropped")
	default:
	}
	// Its stream ends, so the client resumes from its last event: a client that does not read is dropped.
	w := newStreamWriter()
	w.gate = make(chan struct{})
	s := e.openWith(t, "/api/v1/live/runs/r5?pipeline=listings", nil, w)
	s.w.waitFor(t, func(string) bool { return w.status() == http.StatusOK })
	for _, step := range []string{"crawl", "score"} { // the handler blocks on the first event, the next fill its buffer
		e.done(e.lease(step), out("k", `{"score":1}`))
		e.b.pump(e.ctx)
	}
	close(w.gate)
	<-s.done
}

// The broker stopping ends the streams; one that is not started answers unavailable.
func TestLiveBrokerRun(t *testing.T) {
	e := newLiveEnv(t, BrokerOptions{}, scratch)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.b.Run(ctx) }()
	s := e.open(t, "/api/v1/live/runs/r1?pipeline=listings", nil)
	s.w.waitFor(t, func(string) bool { return s.w.status() == http.StatusOK })
	snaps := e.open(t, "/api/v1/live/pipelines/listings", nil)
	snaps.w.waitFor(t, events(1))
	// A commit wakes the broker: the event reaches the streams without a pump by hand, the snapshot at once.
	e.st.OnCommit(e.b.Notify)
	t.Cleanup(func() { e.st.OnCommit(nil) })
	must(0, e.st.Cancel(e.ctx, "listings", "r1"))
	s.w.waitFor(t, func(s string) bool { return strings.Contains(s, "runCancelled") })
	snaps.w.waitFor(t, events(2))
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-s.done
	wantError(t, do(t, e.srv, request{target: "/api/v1/live/runs/r1?pipeline=listings"}), 503, codeUnavailable)
	wantError(t, do(t, e.srv, request{target: "/api/v1/live/pipelines/listings"}), 503, codeUnavailable)
}

func TestLiveErrors(t *testing.T) {
	e := newLiveEnv(t, BrokerOptions{}, readOnly)
	for _, tt := range []struct {
		target, code string
		status       int
	}{
		{"/api/v1/live/runs/r1", codeInvalid, 400},
		{"/api/v1/live/runs/nope?pipeline=listings", codeNotFound, 404},
		{"/api/v1/live/items/r1", codeInvalid, 400},
		{"/api/v1/live/items/nope?pipeline=listings", codeNotFound, 404},
		{"/api/v1/live/pipelines/nope", codeNotFound, 404},
	} {
		wantError(t, do(t, e.srv, request{target: tt.target}), tt.status, tt.code)
	}
	bare := must(New(Options{Store: e.st}))
	for _, target := range []string{"/api/v1/live/runs/r1?pipeline=listings", "/api/v1/live/pipelines/listings",
		"/api/v1/live/logs"} {
		wantError(t, do(t, bare, request{target: target}), 503, codeUnavailable)
	}
	if _, err := NewBroker(BrokerOptions{}); err == nil {
		t.Error("NewBroker without a store succeeded")
	}
}

// A quiet stream sends a heartbeat comment every Heartbeat, by the injected clock.
func TestLiveHeartbeat(t *testing.T) {
	e := newLiveEnv(t, BrokerOptions{}, scratch)
	s := e.open(t, "/api/v1/live/pipelines/listings", nil)
	s.w.waitFor(t, events(1))
	for i := 1; i <= 2; i++ {
		for e.clock.pending() == 0 { // the handler arms its timer after the first snapshot
			s.w.waitFor(t, events(1))
		}
		e.clock.Advance(DefaultHeartbeat)
		s.w.waitFor(t, func(s string) bool { return strings.Count(s, ": heartbeat\n\n") == i })
	}
}

// Snapshots coalesce: the first event after a quiet spell is sent at once, the next ones at most once per
// SnapshotInterval, whatever the number of events.
func TestLiveSnapshotCoalescing(t *testing.T) {
	e := newLiveEnv(t, BrokerOptions{}, ownFixture)
	sub, first := must2(e.b.SubscribePipeline(e.ctx, "listings"))
	defer sub.Close()
	if first.Pipeline != "listings" || len(first.Steps) != 3 {
		t.Fatalf("first snapshot = %+v", first)
	}
	byStep := map[string]StepSnapshot{}
	for _, st := range first.Steps {
		byStep[st.Step] = st
	}
	if c := byStep["check"]; c.AwaitingReview != 3 {
		t.Errorf("check = %+v", c)
	}
	if s := byStep["score"]; s.ThroughputPerMin == 0 || s.ErrorRate != 1.0/6 || s.P95LatencyMs == 0 {
		t.Errorf("score = %+v", s)
	}
	if _, ok := e.b.nextDue(); ok {
		t.Fatal("a snapshot is due with no event")
	}
	sent := 0
	for i := range 8 { // an event every 125 ms over a second
		e.fire("burst-" + strconv.Itoa(i))
		e.b.pump(e.ctx)
		if due, ok := e.b.nextDue(); ok && !due.After(e.clock.Now()) {
			e.b.flush(e.ctx)
		}
		select {
		case <-sub.C():
			sent++
		default:
		}
		e.clock.Advance(125 * time.Millisecond)
	}
	if sent != 4 { // at 0, 250, 500 and 750 ms
		t.Errorf("%d snapshots in a second of events, want 4", sent)
	}
	// The pipeline stream sends them.
	s := e.open(t, "/api/v1/live/pipelines/listings", nil)
	evs := parseSSE(s.w.waitFor(t, events(1)))
	if evs[0].name != sseSnapshot || evs[0].id != "" {
		t.Errorf("snapshot event = %+v", evs[0])
	}
	e.fire("after")
	e.b.pump(e.ctx)
	e.clock.Advance(time.Second)
	e.b.flush(e.ctx)
	s.w.waitFor(t, events(2))
}

func must2[A, B any](a A, b B, err error) (A, B) {
	if err != nil {
		panic(err)
	}
	return a, b
}

// The live log tail: a delivery's records reach the ring through the handler, a reconnect within the ring gets
// what it missed, one beyond it a gap marker first.
func TestLiveLogs(t *testing.T) {
	e := newLiveEnv(t, BrokerOptions{}, readOnly)
	var out bytes.Buffer
	log := slog.New(e.logs.Handler(slog.NewJSONHandler(&out, nil))).With("node", "embedded-1")
	log.Info("node started") // no delivery: not relayed
	dlog := log.With("pipeline", "listings", "runId", "r1", "step", "score", "itemId", "it_1", "deliveryId", "dl_1",
		"attempt", 1)
	dlog.Info("scoring")
	dlog.WithGroup("g").Info("grouped", "deliveryId", "dl_x")
	if !strings.Contains(out.String(), "node started") {
		t.Error("records were not passed on")
	}
	s := e.open(t, "/api/v1/live/logs?run=r1", nil)
	evs := parseSSE(s.w.waitFor(t, events(2)))
	var line LogLine
	if err := json.Unmarshal([]byte(evs[0].data), &line); err != nil {
		t.Fatal(err)
	}
	if evs[0].name != sseLog || line.Message != "scoring" || line.DeliveryID != "dl_1" || line.Attempt != 1 ||
		line.Node != "embedded-1" {
		t.Errorf("first line = %+v %+v", evs[0], line)
	}
	lastID := evs[1].id
	dlog.Info("live line")
	s.w.waitFor(t, events(3))
	s.cancel()
	<-s.done

	// Within the ring: no gap, the missed line is resent.
	dlog.Info("missed")
	r := e.open(t, "/api/v1/live/logs?delivery=dl_1", map[string]string{"Last-Event-ID": lastID})
	evs = parseSSE(r.w.waitFor(t, events(2)))
	if evs[0].name != sseLog || !strings.Contains(evs[1].data, "missed") {
		t.Errorf("resumed within the ring: %+v", evs)
	}
	// Beyond it, or from another process: a gap, then what the ring holds.
	for range 5 {
		dlog.Info("overflow")
	}
	for _, cursor := range []string{lastID, "other.3", "x"} {
		g := e.open(t, "/api/v1/live/logs?lastEventId="+cursor, nil)
		evs = parseSSE(g.w.waitFor(t, events(5)))
		if evs[0].name != sseGap || len(evs) != 5 {
			t.Errorf("cursor %s: %+v", cursor, evs)
		}
	}
	// A subscriber that falls behind is dropped.
	slow := e.logs.Subscribe(LogFilter{}, "")
	for range logBuffer + 1 {
		dlog.Info("flood")
	}
	select {
	case <-slow.Done():
	default:
		t.Error("the slow log subscriber was not dropped")
	}
}

func TestSystemClock(t *testing.T) {
	c := SystemClock{}
	if c.Now().IsZero() {
		t.Error("Now is zero")
	}
	<-c.NewTimer(0).C()
	if c.NewTimer(time.Hour).Stop() != true {
		t.Error("Stop of a pending timer = false")
	}
}

// failingStore fails every read of pipelines: an error the API answers as internal, without its message.
type failingStore struct{ store.Store }

func (failingStore) Pipelines(context.Context) ([]store.PipelineState, error) {
	return nil, errors.New("disk on fire at /var/db")
}

func TestInternalError(t *testing.T) {
	f := readFixture(t)
	var logged bytes.Buffer
	srv := must(New(Options{Store: failingStore{f.st}, Logger: slog.New(slog.NewTextHandler(&logged, nil))}))
	rec := do(t, srv, request{target: "/api/v1/pipelines"})
	wantError(t, rec, http.StatusInternalServerError, codeInternal)
	if strings.Contains(rec.Body.String(), "disk") || !strings.Contains(logged.String(), "disk on fire") {
		t.Errorf("answer %q, log %q", rec.Body.String(), logged.String())
	}
}

func TestCheckLoopback(t *testing.T) {
	for addr, ok := range map[string]bool{
		"127.0.0.1:7070": true, "[::1]:7070": true, "localhost:7070": true, "127.0.0.2:80": true,
		"0.0.0.0:7070": false, ":7070": false, "192.168.1.2:7070": false, "example.com:7070": false, "7070": false,
	} {
		if err := CheckLoopback(addr); (err == nil) != ok {
			t.Errorf("CheckLoopback(%q) = %v", addr, err)
		}
	}
}
