package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
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

func TestRunRespectsSlots(t *testing.T) {
	t.Parallel()
	var as []Assignment
	for i := range 5 {
		as = append(as, assignment(fmt.Sprintf("dl_%d", i), "slow", map[string]any{"sleepMs": 20}))
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

// TestDoneFailMapping pins how each outcome is reported: done with outputs, the action's own error as class
// action, runtime failures under their class and always retryable.
func TestDoneFailMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		mode      string
		config    map[string]any
		wantDone  bool
		class     string
		retryable bool
	}{
		{"echo", nil, true, "", false},
		{"error", map[string]any{"retryable": true}, false, ClassAction, true},
		{"error", map[string]any{"retryable": false}, false, ClassAction, false},
		{"crash", nil, false, exec.ClassCrash, true},
		{"garbage", nil, false, exec.ClassProtocol, true},
		{"hang", nil, false, exec.ClassTimeout, true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.mode, tt.config), func(t *testing.T) {
			t.Parallel()
			a := assignment("dl_1", tt.mode, tt.config)
			a.Timeout = 50 * time.Millisecond // only hang reaches it
			tr := newFakeTransport(a)
			stop := startNode(t, tr, Options{})
			await(t, tr.reported, 1)
			if err := stop(); err != nil {
				t.Fatalf("Run: %v", err)
			}
			tr.mu.Lock()
			defer tr.mu.Unlock()
			if tt.wantDone {
				if len(tr.dones) != 1 {
					t.Fatalf("dones = %+v, fails = %+v", tr.dones, tr.fails)
				}
				return
			}
			if len(tr.fails) != 1 {
				t.Fatalf("dones = %+v, fails = %+v", tr.dones, tr.fails)
			}
			f := tr.fails[0]
			if f.Class != tt.class || f.Retryable != tt.retryable || f.Attempt != 2 || f.FencingToken != 7 ||
				f.Message == "" {
				t.Errorf("fail = %+v", f)
			}
		})
	}
}

func TestHeartbeatRenewsHeldLeases(t *testing.T) {
	t.Parallel()
	tr := newFakeTransport(assignment("dl_1", "slow", map[string]any{"sleepMs": 60}))
	stop := startNode(t, tr, Options{HeartbeatInterval: 5 * time.Millisecond})
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.heartbeats) < 2 {
		t.Fatalf("heartbeats = %d, want several during a 60 ms delivery", len(tr.heartbeats))
	}
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
	tr := newFakeTransport(assignment("dl_1", "slow", map[string]any{"sleepMs": 3000}))
	tr.lose["dl_1"] = true
	start := time.Now()
	stop := startNode(t, tr, Options{HeartbeatInterval: 5 * time.Millisecond, Logger: newJSONLogger(&logs)})
	await(t, tr.lost, 1)
	if err := stop(); err != nil { // drains: returns once the cancelled delivery is gone
		t.Fatalf("Run: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Run took %s: the delivery was not cancelled", elapsed)
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

func TestPullAndHeartbeatErrorsAreRetried(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	tr := newFakeTransport(assignment("dl_1", "slow", map[string]any{"sleepMs": 40}))
	tr.pullErr = errors.New("engine busy")
	tr.hbErr = errors.New("engine busy")
	stop := startNode(t, tr, Options{HeartbeatInterval: 5 * time.Millisecond, Logger: newJSONLogger(&logs)})
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.dones) != 1 {
		t.Errorf("dones = %d after a pull error", len(tr.dones))
	}
	for _, msg := range []string{"pull deliveries", "heartbeat"} {
		if !hasLog(t, &logs, msg, "") {
			t.Errorf("no %q warning", msg)
		}
	}
}

func TestDrainWaitsForInFlight(t *testing.T) {
	t.Parallel()
	tr := newFakeTransport(assignment("dl_1", "slow", map[string]any{"sleepMs": 100}),
		assignment("dl_2", "echo", nil))
	stop := startNode(t, tr, Options{Slots: 1})
	await(t, tr.pulled, 1)
	if err := stop(); err != nil {
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
	tr := newFakeTransport(assignment("dl_1", "hang", nil))
	stop := startNode(t, tr, Options{DrainTimeout: 20 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond})
	await(t, tr.pulled, 1)
	err := stop()
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Run = %v, want ErrDrainTimeout", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.dones)+len(tr.fails) != 0 {
		t.Errorf("an abandoned delivery was reported")
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

// An item too large for a protocol line fails non-retryably, class protocol: retrying cannot make it fit.
func TestOversizedItemFailsNonRetryable(t *testing.T) {
	t.Parallel()
	a := assignment("dl_1", "echo", nil)
	a.ItemMsg.Payload = json.RawMessage(`"` + strings.Repeat("x", protocol.MaxLine) + `"`)
	tr := newFakeTransport(a)
	stop := startNode(t, tr, Options{})
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.fails) != 1 || tr.fails[0].Class != exec.ClassProtocol || tr.fails[0].Retryable {
		t.Fatalf("fails = %+v; want one non-retryable protocol failure", tr.fails)
	}
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

func sliceOf(v any) []any {
	s, _ := v.([]any)
	return s
}

// Metrics without a name or a finite value are dropped, and at most store.MaxAttemptMetrics are kept.
func TestStoreMetrics(t *testing.T) {
	t.Parallel()
	in := []protocol.Metric{{Name: "a", Value: 1, Labels: map[string]string{"k": "v"}}, {Name: "", Value: 1},
		{Name: "nan", Value: math.NaN()}, {Name: "inf", Value: math.Inf(-1)}}
	for range store.MaxAttemptMetrics + 5 {
		in = append(in, protocol.Metric{Name: "many", Value: 2})
	}
	out := storeMetrics(slog.New(slog.DiscardHandler), in)
	if len(out) != store.MaxAttemptMetrics || out[0].Name != "a" || out[0].Labels["k"] != "v" {
		t.Fatalf("metrics = %d, first %+v", len(out), out[0])
	}
}
