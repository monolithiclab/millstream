package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/monolithiclab/millstream/internal/insights"
	"github.com/monolithiclab/millstream/internal/store"
)

// ErrSnapshotRequired means a resumption cannot replay what the client missed: its cursor is further behind than the
// broker replays (BrokerOptions.ResumeLimit), older than the events the store keeps, or not a position of this store
// at all (ahead of its last event). The client reloads state through the REST API and subscribes afresh.
var ErrSnapshotRequired = errors.New("snapshot required")

// Defaults for zero BrokerOptions.
const (
	DefaultBuffer           = 256
	DefaultResumeLimit      = 10000
	DefaultSnapshotInterval = 250 * time.Millisecond
	DefaultPollInterval     = 15 * time.Second
	DefaultWindow           = 5 * time.Minute
)

// eventPage is how many events the broker reads at once.
const eventPage = 1000

// BrokerOptions configure a Broker. Store is required.
type BrokerOptions struct {
	Store store.Store
	// Clock paces the poll and the snapshots; nil means SystemClock.
	Clock Clock
	// Buffer is how many events a subscriber may fall behind before it is dropped; zero means DefaultBuffer.
	Buffer int
	// ResumeLimit is the most events behind the last one a resumption replays; zero means DefaultResumeLimit.
	ResumeLimit int64
	// SnapshotInterval is the least time between two snapshots of a pipeline; zero means DefaultSnapshotInterval
	// (at most 4 a second).
	SnapshotInterval time.Duration
	// PollInterval paces a read of the event log without a notification, for the writes of other processes (CLI
	// commands on the same SQLite file); zero means DefaultPollInterval.
	PollInterval time.Duration
	// Window is how far back a snapshot's throughput, error rate and latency look; zero means DefaultWindow.
	Window time.Duration
	// Logger receives read errors; nil discards them.
	Logger *slog.Logger
}

// Broker fans the store's event log out to the live streams (spec section 12.1). One goroutine (Run) reads the
// events after the last one it fanned out whenever the writer reports a commit (Notify), and hands each to the
// subscribers whose scope it matches; per pipeline it coalesces the events into step snapshots, at most one per
// SnapshotInterval. A subscriber that falls Buffer events behind is dropped, never waited for: neither the writer nor
// the other subscribers depend on a client's pace, and the client resumes from its last event id.
//
// Resumption replays what a client missed from the store, which keeps every event: Subscribe registers the
// subscriber at the last event fanned out, h, under the lock that fanning out holds, then reads the events after the
// client's cursor up to h; every later event reaches the subscriber's channel. The two never overlap or leave a
// gap, so a client receives exactly the events it missed, in order.
type Broker struct {
	st   store.Store
	opts BrokerOptions
	log  *slog.Logger
	wake chan struct{}

	mu      sync.Mutex
	started bool
	head    int64 // the last event fanned out
	subs    map[*Subscription]struct{}
	feeds   map[string]*feed // pipeline snapshots, by pipeline
}

// Filter is the scope of a subscription: a pipeline's events, narrowed to a run's or an item's. An item's events are
// those naming it (itemId): its publication and the lifecycle of its deliveries.
type Filter struct {
	Pipeline string
	RunID    string
	ItemID   string
}

// Subscription receives the events of its scope after its backlog.
type Subscription struct {
	b      *Broker
	filter Filter
	c      chan store.Event
	done   chan struct{}
	// Backlog is the events a resumption replayed, in order: the client's missed events up to the subscription.
	Backlog []store.Event
}

// C delivers the events after the backlog, in order.
func (s *Subscription) C() <-chan store.Event { return s.c }

// Done is closed when the subscription ends: closed by Close, dropped for falling behind, or the broker stopped.
func (s *Subscription) Done() <-chan struct{} { return s.done }

// Close ends the subscription.
func (s *Subscription) Close() {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	s.b.dropLocked(s)
}

// feed is a pipeline's snapshot subscribers and the coalescing of its events: due is when the next snapshot is to be
// sent (zero when none is pending), last when the previous one was.
type feed struct {
	subs      map[*PipelineSubscription]struct{}
	last, due time.Time
}

// PipelineSubscription receives a pipeline's step snapshots. Snapshots replace one another: a client that reads
// slowly skips to the latest, it is never dropped for it.
type PipelineSubscription struct {
	b        *Broker
	pipeline string
	c        chan PipelineSnapshot
	done     chan struct{}
}

// C delivers the snapshots after the first.
func (s *PipelineSubscription) C() <-chan PipelineSnapshot { return s.c }

// Done is closed when the subscription ends.
func (s *PipelineSubscription) Done() <-chan struct{} { return s.done }

// Close ends the subscription.
func (s *PipelineSubscription) Close() {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	if f := s.b.feeds[s.pipeline]; f != nil {
		if _, ok := f.subs[s]; ok {
			delete(f.subs, s)
			close(s.done)
		}
	}
}

// PipelineSnapshot is a pipeline's live state per step (spec section 12.1): Seq is the last event it reflects.
type PipelineSnapshot struct {
	Pipeline string         `json:"pipeline"`
	At       time.Time      `json:"at"`
	Seq      int64          `json:"seq"`
	Paused   bool           `json:"paused"`
	Steps    []StepSnapshot `json:"steps"`
}

// StepSnapshot is a step's live state: deliveries queued, in flight and awaiting review now, and over the window,
// the deliveries finished per minute, the share of them that ended dead and their 95th percentile latency.
type StepSnapshot struct {
	Step             string  `json:"step"`
	Paused           bool    `json:"paused"`
	Queued           int64   `json:"queued"`
	InFlight         int64   `json:"inFlight"`
	AwaitingReview   int64   `json:"awaitingReview"`
	ThroughputPerMin float64 `json:"throughputPerMin"`
	ErrorRate        float64 `json:"errorRate"`
	P95LatencyMs     int64   `json:"p95LatencyMs"`
}

// NewBroker returns a broker over opts.Store; Start or Run makes it serve.
func NewBroker(opts BrokerOptions) (*Broker, error) {
	if opts.Store == nil {
		return nil, errors.New("broker: no store")
	}
	if opts.Clock == nil {
		opts.Clock = SystemClock{}
	}
	if opts.Buffer <= 0 {
		opts.Buffer = DefaultBuffer
	}
	if opts.ResumeLimit <= 0 {
		opts.ResumeLimit = DefaultResumeLimit
	}
	if opts.SnapshotInterval <= 0 {
		opts.SnapshotInterval = DefaultSnapshotInterval
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = DefaultPollInterval
	}
	if opts.Window <= 0 {
		opts.Window = DefaultWindow
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	return &Broker{st: opts.Store, opts: opts, log: opts.Logger, wake: make(chan struct{}, 1),
		subs: map[*Subscription]struct{}{}, feeds: map[string]*feed{}}, nil
}

// Notify wakes the broker after a commit; it never blocks, so the store's writer may call it (sqlite OnCommit).
func (b *Broker) Notify(int64) {
	select {
	case b.wake <- struct{}{}:
	default: // a wake-up is already pending: it reads this commit's events too
	}
}

// Start reads where the event log ends: subscribers receive the events committed after it. Run starts the broker
// when Start was not called.
func (b *Broker) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return nil
	}
	head, err := b.st.LastEventSeq(ctx)
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	b.head, b.started = head, true
	return nil
}

// Run fans events out until ctx ends, then ends every subscription.
func (b *Broker) Run(ctx context.Context) error {
	if err := b.Start(ctx); err != nil {
		return err
	}
	defer b.stop()
	poll := b.opts.Clock.NewTimer(b.opts.PollInterval)
	defer func() { poll.Stop() }()
	var (
		flush    Timer
		flushC   <-chan time.Time
		flushDue time.Time
	)
	for {
		b.pump(ctx)
		if due, ok := b.nextDue(); ok && (flush == nil || !due.Equal(flushDue)) {
			if flush != nil {
				flush.Stop()
			}
			flush, flushDue = b.opts.Clock.NewTimer(max(due.Sub(b.opts.Clock.Now()), 0)), due
			flushC = flush.C()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-b.wake:
		case <-poll.C():
			poll = b.opts.Clock.NewTimer(b.opts.PollInterval)
		case <-flushC:
			flush, flushC = nil, nil
			b.flush(ctx)
		}
	}
}

// stop ends every subscription: their streams end, and clients reconnect to whatever serves next.
func (b *Broker) stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs {
		b.dropLocked(s)
	}
	for _, f := range b.feeds {
		for s := range f.subs {
			delete(f.subs, s)
			close(s.done)
		}
	}
	b.started = false
}

// pump reads the events after the last one fanned out and fans them out, until it reaches the end of the log.
func (b *Broker) pump(ctx context.Context) {
	for {
		b.mu.Lock()
		after := b.head
		b.mu.Unlock()
		evs, err := b.st.Events(ctx, after, eventPage)
		if err != nil {
			if ctx.Err() == nil {
				b.log.Error("live events", "error", err.Error())
			}
			return
		}
		b.mu.Lock()
		for _, ev := range evs {
			if ev.Seq > b.head {
				b.head = ev.Seq
				b.fanoutLocked(ev)
			}
		}
		b.mu.Unlock()
		if len(evs) < eventPage {
			return
		}
	}
}

// eventItem reads the item an event names, empty for none.
func eventItem(ev store.Event) string {
	var d struct {
		ItemID string `json:"itemId"`
	}
	_ = json.Unmarshal(ev.Data, &d) // the store writes objects; another shape names no item
	return d.ItemID
}

func (f Filter) matches(ev store.Event, item func() string) bool {
	return ev.Pipeline == f.Pipeline && (f.RunID == "" || ev.RunID == f.RunID) && (f.ItemID == "" || item() == f.ItemID)
}

// fanoutLocked hands ev to the subscribers it matches, dropping those whose buffer is full, and schedules a
// snapshot of its pipeline.
func (b *Broker) fanoutLocked(ev store.Event) {
	var item *string
	itemOf := func() string {
		if item == nil {
			id := eventItem(ev)
			item = &id
		}
		return *item
	}
	for s := range b.subs {
		if !s.filter.matches(ev, itemOf) {
			continue
		}
		select {
		case s.c <- ev:
		default:
			b.log.Warn("live subscriber dropped: too slow", "pipeline", s.filter.Pipeline, "seq", ev.Seq)
			b.dropLocked(s)
		}
	}
	if f := b.feeds[ev.Pipeline]; f != nil && len(f.subs) > 0 && f.due.IsZero() {
		now := b.opts.Clock.Now()
		f.due = f.last.Add(b.opts.SnapshotInterval)
		if f.due.Before(now) {
			f.due = now
		}
	}
}

func (b *Broker) dropLocked(s *Subscription) {
	if _, ok := b.subs[s]; ok {
		delete(b.subs, s)
		close(s.done)
	}
}

// Subscribe subscribes to the events of a scope. With resume, cursor is the last event the client received: the
// subscription's Backlog holds the events of the scope it missed, or Subscribe returns ErrSnapshotRequired.
func (b *Broker) Subscribe(ctx context.Context, f Filter, cursor int64, resume bool) (*Subscription, error) {
	s := &Subscription{b: b, filter: f, c: make(chan store.Event, b.opts.Buffer), done: make(chan struct{})}
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return nil, fmt.Errorf("live events: the broker is not running: %w", errUnavailable)
	}
	head := b.head
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	if !resume {
		return s, nil
	}
	backlog, err := b.backlog(ctx, f, cursor, head)
	if err != nil {
		s.Close()
		return nil, err
	}
	s.Backlog = backlog
	return s, nil
}

// backlog reads the events of the scope after cursor up to head.
func (b *Broker) backlog(ctx context.Context, f Filter, cursor, head int64) ([]store.Event, error) {
	if cursor < 0 || cursor > head || head-cursor > b.opts.ResumeLimit {
		return nil, fmt.Errorf("cursor %d, last event %d: %w", cursor, head, ErrSnapshotRequired)
	}
	var out []store.Event
	for after := cursor; after < head; {
		evs, err := b.st.Events(ctx, after, eventPage)
		if err != nil {
			return nil, err
		}
		if len(evs) == 0 || evs[0].Seq != after+1 { // the store no longer has the events after the cursor
			return nil, fmt.Errorf("cursor %d: the events after it are gone: %w", cursor, ErrSnapshotRequired)
		}
		for _, ev := range evs {
			if ev.Seq > head {
				break
			}
			after = ev.Seq
			if f.matches(ev, func() string { return eventItem(ev) }) {
				out = append(out, ev)
			}
		}
	}
	return out, nil
}

// SubscribePipeline subscribes to a pipeline's step snapshots and returns the first, taken now.
func (b *Broker) SubscribePipeline(ctx context.Context, pipelineName string) (*PipelineSubscription, PipelineSnapshot,
	error,
) {
	s := &PipelineSubscription{b: b, pipeline: pipelineName, c: make(chan PipelineSnapshot, 1),
		done: make(chan struct{})}
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return nil, PipelineSnapshot{}, fmt.Errorf("live snapshots: the broker is not running: %w", errUnavailable)
	}
	f := b.feeds[pipelineName]
	if f == nil {
		f = &feed{subs: map[*PipelineSubscription]struct{}{}}
		b.feeds[pipelineName] = f
	}
	f.subs[s] = struct{}{}
	b.mu.Unlock()
	snap, err := b.snapshot(ctx, pipelineName)
	if err != nil {
		s.Close()
		return nil, snap, err
	}
	return s, snap, nil
}

// nextDue returns when the next snapshot is due, false when none is pending.
func (b *Broker) nextDue() (time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var next time.Time
	for _, f := range b.feeds {
		if !f.due.IsZero() && (next.IsZero() || f.due.Before(next)) {
			next = f.due
		}
	}
	return next, !next.IsZero()
}

// flush sends the snapshots due now.
func (b *Broker) flush(ctx context.Context) {
	now := b.opts.Clock.Now()
	b.mu.Lock()
	var due []string
	for _, name := range slices.Sorted(maps.Keys(b.feeds)) {
		f := b.feeds[name]
		if !f.due.IsZero() && !f.due.After(now) {
			f.due, f.last = time.Time{}, now
			due = append(due, name)
		}
	}
	b.mu.Unlock()
	for _, name := range due {
		snap, err := b.snapshot(ctx, name)
		if err != nil {
			if ctx.Err() == nil {
				b.log.Error("live snapshot", "pipeline", name, "error", err.Error())
			}
			continue
		}
		b.mu.Lock()
		for s := range b.feeds[name].subs {
			select { // the latest replaces a snapshot not read yet
			case <-s.c:
			default:
			}
			s.c <- snap
		}
		b.mu.Unlock()
	}
}

// snapshot reads a pipeline's step state: its active revision's steps, and any other step with deliveries.
func (b *Broker) snapshot(ctx context.Context, pipelineName string) (PipelineSnapshot, error) {
	now := b.opts.Clock.Now()
	b.mu.Lock()
	seq := b.head // read before the state: the snapshot reflects at least the events up to seq
	b.mu.Unlock()
	ps, _, p, err := activePipeline(ctx, b.st, pipelineName)
	if err != nil {
		return PipelineSnapshot{}, err
	}
	stats, err := b.st.StepStats(ctx, pipelineName, now.Add(-b.opts.Window))
	if err != nil {
		return PipelineSnapshot{}, err
	}
	byStep := map[string]store.StepStats{}
	for name := range p.Steps {
		byStep[name] = store.StepStats{Step: name}
	}
	for _, st := range stats {
		byStep[st.Step] = st
	}
	snap := PipelineSnapshot{Pipeline: pipelineName, At: now, Seq: seq, Paused: ps.Paused, Steps: []StepSnapshot{}}
	for _, name := range slices.Sorted(maps.Keys(byStep)) {
		st := byStep[name]
		step := StepSnapshot{Step: name, Paused: ps.Paused || slices.Contains(ps.PausedSteps, name),
			Queued: st.Pending, InFlight: st.Leased, AwaitingReview: st.AwaitingReview,
			ThroughputPerMin: float64(st.Finished) / b.opts.Window.Minutes()}
		if st.Finished > 0 {
			step.ErrorRate = float64(st.Dead) / float64(st.Finished)
			step.P95LatencyMs = insights.Percentile(st.Latencies, 0.95).Milliseconds()
		}
		snap.Steps = append(snap.Steps, step)
	}
	return snap, nil
}
