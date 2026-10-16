//go:build e2e

package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

// The real-time twins of the fake-clock tests in node_test.go: the built fake action, real tickers and sleeps.

func TestRunRespectsSlotsRealTime(t *testing.T) {
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

func TestHeartbeatRenewsHeldLeasesRealTime(t *testing.T) {
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

func TestLostLeaseDropsResultRealTime(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	a := assignment("dl_1", "slow", map[string]any{"sleepMs": 3000})
	tr := newFakeTransport(a)
	tr.lose[a.Lease.Ref()] = true
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

func TestPullAndHeartbeatErrorsAreRetriedRealTime(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	tr := newFakeTransport(assignment("dl_1", "slow", map[string]any{"sleepMs": 40}))
	tr.pullErr = errors.New("engine busy")
	tr.hbErr = errors.New("engine busy")
	stop := startNode(t, tr, Options{PullInterval: 5 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond,
		Logger: newJSONLogger(&logs)})
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

func TestDrainWaitsForInFlightRealTime(t *testing.T) {
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

func TestDrainTimeoutAbandonsRealTime(t *testing.T) {
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
			if tt.mode == "hang" {
				a.Timeout = time.Nanosecond // expires at once: the fake never answers
			}
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
