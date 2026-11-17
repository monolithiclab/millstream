package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest/fakeexec"
	"github.com/monolithiclab/millstream/internal/store"
)

func TestNewValidates(t *testing.T) {
	t.Parallel()
	pool := exec.NewPool(exec.PoolOptions{})
	tr := newFakeTransport()
	tests := []struct {
		name string
		t    Transport
		opts Options
	}{
		{"no transport", nil, Options{Owner: "n", Pool: pool}},
		{"no owner", tr, Options{Pool: pool}},
		{"no pool", tr, Options{Owner: "n"}},
	}
	for _, tt := range tests {
		if _, err := New(tt.t, tt.opts); err == nil {
			t.Errorf("%s: New succeeded", tt.name)
		}
	}
	n, err := New(tr, Options{Owner: "n", Pool: pool})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if n.opts.Slots != DefaultSlots || n.opts.HeartbeatInterval != DefaultHeartbeatInterval ||
		n.opts.PullInterval != DefaultPullInterval || n.opts.DrainTimeout != DefaultDrainTimeout {
		t.Errorf("defaults not applied: %+v", n.opts)
	}
	if n, _ := New(tr, Options{Owner: "n", Pool: pool, Heartbeat: func() time.Duration { return time.Second }}); n.opts.HeartbeatInterval != time.Second {
		t.Errorf("first interval %s, want the one Heartbeat gives", n.opts.HeartbeatInterval)
	}
	if HeartbeatFor(0) != DefaultHeartbeatInterval || HeartbeatFor(3*time.Second) != time.Second {
		t.Error("HeartbeatFor: want a third of the lease, at most the default")
	}
}

func TestRunCompletesAssignments(t *testing.T) {
	t.Parallel()
	tr := newFakeTransport(assignment("dl_1", "echo", nil), assignment("dl_2", "echo", nil),
		assignment("dl_3", "echo", nil))
	stop := startNode(t, tr, Options{Slots: 2})
	got := await(t, tr.reported, 3)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	slices.Sort(got)
	if !slices.Equal(got, []string{"dl_1", "dl_2", "dl_3"}) {
		t.Errorf("reported %v", got)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.dones) != 3 || len(tr.fails) != 0 {
		t.Fatalf("dones %d, fails %d", len(tr.dones), len(tr.fails))
	}
	for _, d := range tr.dones {
		want := store.LeaseRef{Pipeline: "listings", DeliveryID: d.DeliveryID, Step: "score", FencingToken: 7}
		if d.LeaseRef != want || d.Attempt != 2 || d.CostUSD != 0 {
			t.Errorf("done = %+v", d)
		}
		if len(d.Outputs) != 1 || d.Outputs[0].Key != "key-"+d.DeliveryID {
			t.Errorf("outputs = %+v", d.Outputs)
		}
	}
}

func TestStaleLeaseDroppedAndNodeContinues(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	tr := newFakeTransport(assignment("dl_1", "echo", nil), assignment("dl_2", "error", nil))
	tr.reportErr = fmt.Errorf("done: %w", store.ErrStaleLease)
	stop := startNode(t, tr, Options{Logger: newJSONLogger(&logs)})
	await(t, tr.reported, 2)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, id := range []string{"dl_1", "dl_2"} {
		if !hasLog(t, &logs, "stale lease, result dropped", id) {
			t.Errorf("no stale-lease log for %s", id)
		}
	}
}

func TestReportErrorLogged(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	tr := newFakeTransport(assignment("dl_1", "echo", nil))
	tr.reportErr = errors.New("engine unreachable")
	stop := startNode(t, tr, Options{Logger: newJSONLogger(&logs)})
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !hasLog(t, &logs, "report delivery", "dl_1") {
		t.Error("no report error log")
	}
}

func TestNodeLogsCarryKeys(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	a := assignment("dl_1", "stderr", nil)
	tr := newFakeTransport(a)
	pool := fakeexec.NewPool(t, exec.Options{})
	stop := startNode(t, tr, Options{Logger: newJSONLogger(&logs), Pool: pool})
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_ = pool.Close() // drains the action's stderr
	want := map[string]any{"pipeline": "listings", "runId": "run_1", "step": "score", "itemId": "it_dl_1",
		"deliveryId": "dl_1", "attempt": float64(2), "node": "node-a"}
	var found int
	for _, l := range logs.logs(t) {
		if l["msg"] != "fetching page" && l["msg"] != "parsed listing" {
			continue
		}
		found++
		for k, v := range want {
			if l[k] != v {
				t.Errorf("%q: %s = %v, want %v", l["msg"], k, l[k], v)
			}
		}
	}
	if found != 2 {
		t.Errorf("found %d action lines, want 2", found)
	}
}

// hasLog reports whether a log line has msg and, when id is set, that deliveryId.
func hasLog(t *testing.T, b *syncBuffer, msg, id string) bool {
	t.Helper()
	for _, l := range b.logs(t) {
		if l["msg"] == msg && (id == "" || l["deliveryId"] == id) {
			return true
		}
	}
	return false
}

// The node reports an attempt's cost as the sum of its cost_usd metrics, on done and on an action's error; a
// failure in the runtime reports none, so its reservation is charged in full.
func TestReportsCostMetrics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		mode       string
		config     map[string]any
		cost       float64
		unreported bool
	}{
		{"done", "cost", map[string]any{"costs": []any{0.25, 0.5}}, 0.75, false},
		{"error", "cost", map[string]any{"costs": []any{0.125}, "fail": true}, 0.125, false},
		{"no cost metric", "cost", nil, 0, false},
		{"crash", "crash", nil, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := newFakeTransport(assignment("dl_1", tt.mode, tt.config))
			stop := startNode(t, tr, Options{})
			await(t, tr.reported, 1)
			if err := stop(); err != nil {
				t.Fatalf("Run: %v", err)
			}
			tr.mu.Lock()
			defer tr.mu.Unlock()
			var cost float64
			var unreported bool
			var metrics []store.Metric
			switch {
			case len(tr.dones) == 1:
				cost, metrics = tr.dones[0].CostUSD, tr.dones[0].Metrics
			case len(tr.fails) == 1:
				cost, unreported, metrics = tr.fails[0].CostUSD, tr.fails[0].Unreported, tr.fails[0].Metrics
			default:
				t.Fatalf("dones = %+v, fails = %+v", tr.dones, tr.fails)
			}
			if cost != tt.cost || unreported != tt.unreported {
				t.Fatalf("cost = %v unreported %v, want %v %v", cost, unreported, tt.cost, tt.unreported)
			}
			// The action's metrics travel with the report: the cost_usd ones and the other one.
			if tt.mode == "cost" && len(metrics) != len(sliceOf(tt.config["costs"]))+1 {
				t.Fatalf("metrics = %+v", metrics)
			}
		})
	}
}

// An action's abstain message is reported as a Fail of class store.ClassAbstain, its reason the message: the engine
// applies the Abstain unit to it.
func TestReportsAbstain(t *testing.T) {
	t.Parallel()
	a := assignment("dl_1", "rules", nil)
	a.ItemMsg.Payload = json.RawMessage(`{"abstain":true}`)
	tr := newFakeTransport(a)
	stop := startNode(t, tr, Options{})
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.fails) != 1 || tr.fails[0].Class != store.ClassAbstain || tr.fails[0].Retryable ||
		tr.fails[0].Message != "no rule for this item" {
		t.Fatalf("fails = %+v", tr.fails)
	}
}

func sliceOf(v any) []any {
	s, _ := v.([]any)
	return s
}

// Metrics without a name or a finite value are dropped, and at most store.MaxAttemptMetrics are kept. Names, label
// keys and label values are redacted of the delivery's secrets, as error messages are.
func TestStoreMetrics(t *testing.T) {
	t.Parallel()
	red := redact.New(map[string]string{"apiToken": "s3cr3t-token"})
	in := []protocol.Metric{{Name: "a", Value: 1, Labels: map[string]string{"k": "v"}}, {Name: "", Value: 1},
		{Name: "nan", Value: math.NaN()}, {Name: "inf", Value: math.Inf(-1)},
		{Name: "calls_s3cr3t-token", Value: 1, Labels: map[string]string{"s3cr3t-token": "url?key=s3cr3t-token"}}}
	for range store.MaxAttemptMetrics + 5 {
		in = append(in, protocol.Metric{Name: "many", Value: 2})
	}
	out := storeMetrics(slog.New(slog.DiscardHandler), red, in)
	if len(out) != store.MaxAttemptMetrics || out[0].Name != "a" || out[0].Labels["k"] != "v" {
		t.Fatalf("metrics = %d, first %+v", len(out), out[0])
	}
	if m := out[1]; m.Name != "calls_[redacted:apiToken]" || len(m.Labels) != 1 ||
		m.Labels["[redacted:apiToken]"] != "url?key=[redacted:apiToken]" {
		t.Fatalf("metric with a secret = %+v", m)
	}
	if in[4].Labels["s3cr3t-token"] == "" {
		t.Fatal("the action's labels were changed in place")
	}
}

// The node never runs more deliveries than its slots: it pulls only for free slots, two at once here.
func TestRunRespectsSlots(t *testing.T) {
	t.Parallel()
	var as []Assignment
	for i := range 5 {
		as = append(as, assignment(fmt.Sprintf("dl_%d", i), "echo", nil))
	}
	tr := newFakeTransport(as...)
	stop := startNode(t, tr, Options{Slots: 2})
	await(t, tr.reported, 5)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.maxOut != 2 {
		t.Errorf("max in flight = %d, want 2", tr.maxOut)
	}
	for _, m := range tr.pullMax {
		if m < 1 || m > 2 {
			t.Errorf("pulled with max %d", m)
		}
	}
}

// wakingTransport is a fake transport that is a Waker: the test closes its changes channel to say work is due.
type wakingTransport struct {
	*fakeTransport
	pulls   chan struct{} // one per Pull, after it
	changes chan struct{}
}

func (w *wakingTransport) Pull(ctx context.Context, owner string, maxN int) ([]Assignment, error) {
	defer func() { w.pulls <- struct{}{} }()
	return w.fakeTransport.Pull(ctx, owner, maxN)
}

func (w *wakingTransport) Changes() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.changes
}

// TestWakerWakesThePull: work that comes due between pulls (a builtin step the engine completed) is pulled when the
// transport says so, not at the next pull tick, which this test never fires.
func TestWakerWakesThePull(t *testing.T) {
	t.Parallel()
	tr := &wakingTransport{fakeTransport: newFakeTransport(), pulls: make(chan struct{}, 8), changes: make(chan struct{})}
	stop, _ := startNodeClock(t, tr, Options{})
	await(t, tr.pulls, 1) // nothing to pull yet
	tr.mu.Lock()
	tr.queue = append(tr.queue, assignment("dl_1", "echo", nil))
	close(tr.changes)
	tr.changes = make(chan struct{})
	tr.mu.Unlock()
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestHeartbeatRenewsHeldLeases(t *testing.T) {
	t.Parallel()
	pool, release, started := heldPool(t)
	tr := newFakeTransport(assignment("dl_1", "echo", nil))
	stop, clk := startNodeClock(t, tr, Options{Pool: pool})
	await(t, tr.pulled, 1)
	await(t, started, 1)
	clk.fire(DefaultPullInterval) // the one slot is busy: no pull
	clk.fire(DefaultHeartbeatInterval)
	clk.fire(DefaultHeartbeatInterval)
	await(t, tr.beats, 2)
	tr.mu.Lock()
	pulls := len(tr.pullMax)
	tr.mu.Unlock()
	if pulls != 1 {
		t.Errorf("pulls = %d with the slot busy, want 1", pulls)
	}
	release()
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	want := store.LeaseRef{Pipeline: "listings", DeliveryID: "dl_1", Step: "score", FencingToken: 7}
	for _, refs := range tr.heartbeats {
		if len(refs) != 1 || refs[0] != want {
			t.Errorf("heartbeat refs = %+v", refs)
		}
	}
}

func TestLostLeaseDropsResult(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	pool, _, started := heldPool(t) // never released: only the lost lease ends the delivery
	a := assignment("dl_1", "echo", nil)
	tr := newFakeTransport(a)
	tr.lose[a.Lease.Ref()] = true
	stop, clk := startNodeClock(t, tr, Options{Pool: pool, Logger: newJSONLogger(&logs)})
	await(t, tr.pulled, 1)
	await(t, started, 1)
	clk.fire(DefaultHeartbeatInterval)
	await(t, tr.lost, 1)
	if err := stop(); err != nil { // drains: returns once the cancelled delivery is gone
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.dones)+len(tr.fails) != 0 {
		t.Errorf("a lost lease was reported: dones %+v, fails %+v", tr.dones, tr.fails)
	}
	if !hasLog(t, &logs, "delivery abandoned, result dropped", "dl_1") {
		t.Error("no abandoned log for dl_1")
	}
}

func TestPullAndHeartbeatErrorsAreRetried(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	pool, release, started := heldPool(t)
	tr := newFakeTransport(assignment("dl_1", "echo", nil))
	tr.pullErr = errors.New("engine busy")
	tr.hbErr = errors.New("engine busy")
	beaten := make(chan struct{}, 8) // each heartbeat's end, before the node takes its next interval
	stop, clk := startNodeClock(t, tr, Options{Pool: pool, Logger: newJSONLogger(&logs),
		HeartbeatInterval: DefaultHeartbeatInterval,
		Heartbeat:         func() time.Duration { beaten <- struct{}{}; return DefaultHeartbeatInterval }})
	clk.fire(DefaultHeartbeatInterval) // nothing held: no heartbeat call
	await(t, beaten, 1)
	clk.fire(DefaultPullInterval) // the first pull failed: the next tick pulls again
	await(t, tr.pulled, 1)
	await(t, started, 1)
	clk.fire(DefaultHeartbeatInterval) // fails
	clk.fire(DefaultHeartbeatInterval) // renews
	await(t, tr.beats, 2)
	release()
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.dones) != 1 {
		t.Errorf("dones = %d after a pull error", len(tr.dones))
	}
	if len(tr.heartbeats) != 2 {
		t.Errorf("heartbeat calls = %d, want 2: none while nothing was held", len(tr.heartbeats))
	}
	for _, msg := range []string{"pull deliveries", "heartbeat"} {
		if !hasLog(t, &logs, msg, "") {
			t.Errorf("no %q warning", msg)
		}
	}
}

// stopAsync calls stop on another goroutine, for a test that acts on the node while it drains.
func stopAsync(stop func() error) <-chan error {
	errc := make(chan error, 1)
	go func() { errc <- stop() }()
	return errc
}

func TestDrainWaitsForInFlight(t *testing.T) {
	t.Parallel()
	pool, release, _ := heldPool(t)
	tr := newFakeTransport(assignment("dl_1", "echo", nil), assignment("dl_2", "echo", nil))
	stop, clk := startNodeClock(t, tr, Options{Slots: 1, Pool: pool})
	await(t, tr.pulled, 1)
	errc := stopAsync(stop)
	clk.draining()
	clk.fire(DefaultHeartbeatInterval) // the drain keeps renewing the leases
	await(t, tr.beats, 1)
	release()
	if err := <-errc; err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.dones) != 1 || tr.dones[0].DeliveryID != "dl_1" {
		t.Errorf("dones = %+v, want dl_1 finished during the drain", tr.dones)
	}
	if len(tr.queue) != 1 {
		t.Errorf("pulled after cancel: queue = %d", len(tr.queue))
	}
}

func TestDrainTimeoutAbandons(t *testing.T) {
	t.Parallel()
	pool, _, _ := heldPool(t) // never released
	tr := newFakeTransport(assignment("dl_1", "echo", nil))
	stop, clk := startNodeClock(t, tr, Options{Pool: pool})
	await(t, tr.pulled, 1)
	errc := stopAsync(stop)
	clk.draining()
	clk.fire(DefaultDrainTimeout)
	if err := <-errc; !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Run = %v, want ErrDrainTimeout", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.dones)+len(tr.fails) != 0 {
		t.Errorf("an abandoned delivery was reported")
	}
}

// Cancelled with ErrAbandon, the node does not drain: it cancels its deliveries at once and reports none.
func TestAbandonCancelsWithoutReporting(t *testing.T) {
	t.Parallel()
	pool, _, _ := heldPool(t) // never released: only the cancellation ends the delivery
	tr := newFakeTransport(assignment("dl_1", "echo", nil))
	n, err := New(tr, Options{Owner: "node-a", Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	clk := newFakeClock(t)
	n.newTimer = clk.newTimer
	ctx, cancel := context.WithCancelCause(t.Context())
	errc := make(chan error, 1)
	go func() { errc <- n.Run(ctx) }()
	await(t, tr.pulled, 1)
	cancel(fmt.Errorf("stepping down: %w", ErrAbandon))
	if err := <-errc; err != nil {
		t.Fatalf("Run = %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.dones)+len(tr.fails) != 0 || len(tr.heartbeats) != 0 {
		t.Errorf("an abandoned delivery was reported or renewed")
	}
}

// A permanent runtime failure fails non-retryably: here an action of a kind the pool has no runtime for (the
// oversized item of TestOversizedItemFailsNonRetryable, in the e2e build, is another).
func TestPermanentRuntimeFailureIsNotRetryable(t *testing.T) {
	t.Parallel()
	a := assignment("dl_1", "echo", nil)
	a.Spec.Kind, a.Spec.Image, a.Spec.Command = pipeline.KindContainer, "sha256:img", nil
	tr := newFakeTransport(a)
	stop := startNode(t, tr, Options{})
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.fails) != 1 || tr.fails[0].Class != exec.ClassCrash || tr.fails[0].Retryable {
		t.Fatalf("fails = %+v; want one non-retryable crash failure", tr.fails)
	}
}

func TestRealTimer(t *testing.T) {
	t.Parallel()
	for _, repeat := range []bool{false, true} {
		c, stop := realTimer(time.Hour, repeat)
		stop()
		select {
		case <-c:
			t.Errorf("repeat %v: a stopped timer fired", repeat)
		default:
		}
	}
}

// Delivery ids are unique within a pipeline only: two held leases of different pipelines with the same delivery id
// are both renewed, and a heartbeat that reports one lost cancels that one alone.
func TestSameDeliveryIDInTwoPipelines(t *testing.T) {
	t.Parallel()
	pool, release, started := heldPool(t)
	kept, lost := assignment("dl_1", "echo", nil), assignment("dl_1", "echo", nil)
	lost.Lease.Pipeline = "other"
	tr := newFakeTransport(kept, lost)
	tr.lose[lost.Lease.Ref()] = true
	stop, clk := startNodeClock(t, tr, Options{Slots: 2, Pool: pool})
	await(t, tr.pulled, 2)
	await(t, started, 2)
	clk.fire(DefaultHeartbeatInterval)
	if n := await(t, tr.beats, 1)[0]; n != 2 {
		t.Fatalf("heartbeat renewed %d leases, want both", n)
	}
	await(t, tr.lost, 1)
	release()
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.dones) != 1 || tr.dones[0].Pipeline != "listings" || len(tr.fails) != 0 {
		t.Fatalf("dones %+v, fails %+v; want the kept lease done, the lost one dropped", tr.dones, tr.fails)
	}
}

// blockingPull is a transport whose pulls after the first block until released: a long poll.
type blockingPull struct {
	*fakeTransport
	pulls   atomic.Int32
	waiting chan struct{} // closed once a pull blocks
	release chan struct{}
}

func (b *blockingPull) Pull(ctx context.Context, owner string, maxN int) ([]Assignment, error) {
	if b.pulls.Add(1) == 2 {
		close(b.waiting)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	}
	return b.fakeTransport.Pull(ctx, owner, maxN)
}

// The heartbeat runs on its own goroutine: a pull waiting on the engine never delays it. After each heartbeat the
// node takes its interval anew, as a remote node adopts the one of the engine it registered with last.
func TestHeartbeatBesidePull(t *testing.T) {
	t.Parallel()
	pool, release, started := heldPool(t)
	tr := &blockingPull{fakeTransport: newFakeTransport(assignment("dl_1", "echo", nil)), waiting: make(chan struct{}),
		release: make(chan struct{})}
	stop, clk := startNodeClock(t, tr, Options{Pool: pool, Slots: 2, HeartbeatInterval: DefaultHeartbeatInterval,
		Heartbeat: func() time.Duration { return 2 * time.Second }})
	await(t, tr.pulled, 1)
	await(t, started, 1)
	clk.fire(DefaultPullInterval)
	<-tr.waiting                       // the loop now waits in a pull that does not return
	clk.fire(DefaultHeartbeatInterval) // renews, then takes the engine's interval
	clk.fire(2 * time.Second)
	clk.fire(2 * time.Second)
	if got := await(t, tr.beats, 3); got[2] != 1 {
		t.Errorf("leases per heartbeat = %v", got)
	}
	close(tr.release)
	release()
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}
