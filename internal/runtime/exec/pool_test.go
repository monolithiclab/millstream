package exec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
)

func newTestPool(t *testing.T, maxIdle int) *Pool {
	t.Helper()
	pl := NewPool(PoolOptions{Options: testOpts, MaxIdle: maxIdle})
	t.Cleanup(func() { _ = pl.Close() })
	return pl
}

func poolPID(t *testing.T, pl *Pool, spec Spec, id string) int {
	t.Helper()
	res, err := pl.Deliver(t.Context(), spec, delivery(id, `{}`))
	if err != nil {
		t.Fatalf("Deliver %s: %v", id, err)
	}
	return decodeEcho(t, res).PID
}

func TestPoolLifecycles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		lifecycle string
		reused    bool
	}{
		{"", true},
		{pipeline.LifecycleLong, true},
		{pipeline.LifecyclePerItem, false},
	}
	for _, tt := range tests {
		t.Run(tt.lifecycle, func(t *testing.T) {
			t.Parallel()
			pl := newTestPool(t, 0)
			spec := fakeSpec("echo", nil)
			spec.Lifecycle = tt.lifecycle
			first, second := poolPID(t, pl, spec, "dl_1"), poolPID(t, pl, spec, "dl_2")
			if (first == second) != tt.reused {
				t.Errorf("pids %d, %d: reused = %v, want %v", first, second, first == second, tt.reused)
			}
			pl.mu.Lock()
			idle := len(pl.idle)
			pl.mu.Unlock()
			if want := map[bool]int{true: 1, false: 0}[tt.reused]; idle != want {
				t.Errorf("idle = %d, want %d", idle, want)
			}
		})
	}
}

func TestPoolKeysOnDigestConfigVarsAndLessons(t *testing.T) {
	t.Parallel()
	pl := newTestPool(t, 0)
	base := fakeSpec("echo", nil)
	pid := poolPID(t, pl, base, "dl_0")
	otherDigest, otherConfig, otherVars, otherLessons := base, fakeSpec("echo", map[string]any{"extra": 1}), base, base
	otherDigest.Digest = "sha256:other"
	otherVars.Vars = map[string]any{"city": "Paris"}
	otherLessons.Lessons = json.RawMessage(`{"rules":[{"text":"Be strict."}]}`)
	for name, spec := range map[string]Spec{"digest": otherDigest, "config": otherConfig, "vars": otherVars,
		"lessons": otherLessons} {
		if got := poolPID(t, pl, spec, "dl_"+name); got == pid {
			t.Errorf("another %s reused process %d", name, pid)
		}
	}
	if got := poolPID(t, pl, base, "dl_again"); got != pid {
		t.Errorf("same key: pid %d, want %d", got, pid)
	}
}

func TestPoolReplacesCrashedProcess(t *testing.T) {
	t.Parallel()
	pl := newTestPool(t, 0)
	spec := fakeSpec("echo", nil)
	pid := poolPID(t, pl, spec, "dl_1")
	_, err := pl.Deliver(t.Context(), spec, delivery("dl_2", `{"mode":"crash"}`))
	requireClass(t, err, ClassCrash)
	if got := poolPID(t, pl, spec, "dl_3"); got == pid {
		t.Errorf("crashed process %d reused", pid)
	}
}

func TestPoolReplacesProcessThatExitedWhileIdle(t *testing.T) {
	t.Parallel()
	pl := newTestPool(t, 0)
	spec := fakeSpec("echo", nil)
	pid := poolPID(t, pl, spec, "dl_1")
	pl.mu.Lock()
	p := pl.idle[0].p
	pl.mu.Unlock()
	p.terminate()
	waitExited(t, p)
	if got := poolPID(t, pl, spec, "dl_2"); got == pid {
		t.Errorf("dead process %d reused", pid)
	}
}

// TestPoolConcurrentDeliveriesUseSeparateProcesses: deliveries in flight together run on as many processes. Each
// in-process action holds its item until all three have one, so the three deliveries overlap.
func TestPoolConcurrentDeliveriesUseSeparateProcesses(t *testing.T) {
	t.Parallel()
	const n = 3
	var pids atomic.Int64
	var holding sync.WaitGroup
	holding.Add(n)
	opts := funcOpts(func(_ context.Context, _, _ []string, stdin io.Reader, stdout, _ io.Writer) int {
		r, w := protocol.NewReader(stdin), protocol.NewWriter(stdout)
		if _, err := r.Read(); err != nil { // hello
			return 1
		}
		_ = w.Write(protocol.Ready{})
		pid := pids.Add(1)
		for {
			m, err := r.Read()
			if err != nil {
				return 0
			}
			it, ok := m.(protocol.Item)
			if !ok {
				return 0 // shutdown
			}
			holding.Done()
			holding.Wait()
			_ = w.Write(protocol.Emit{Delivery: it.Delivery, Payload: json.RawMessage(fmt.Sprintf(`{"pid":%d}`, pid))})
			_ = w.Write(protocol.Done{Delivery: it.Delivery})
		}
	})
	pl := NewPool(PoolOptions{Options: opts})
	t.Cleanup(func() { _ = pl.Close() })
	var wg sync.WaitGroup
	got := make([]int, n)
	for i := range got {
		wg.Go(func() {
			res, err := pl.Deliver(t.Context(), fakeSpec("echo", nil), delivery("dl_c", `{}`))
			if err != nil {
				t.Errorf("Deliver: %v", err)
				holding.Done() // the others must not wait for it
				return
			}
			got[i] = decodeEcho(t, res).PID
		})
	}
	wg.Wait()
	if got[0] == got[1] || got[1] == got[2] || got[0] == got[2] {
		t.Errorf("pids %v: one process served concurrent deliveries", got)
	}
}

func TestPoolMaxIdleEvictsOldest(t *testing.T) {
	t.Parallel()
	pl := newTestPool(t, 1)
	a, b := fakeSpec("echo", nil), fakeSpec("echo", nil)
	b.Digest = "sha256:b"
	pidA := poolPID(t, pl, a, "dl_a")
	poolPID(t, pl, b, "dl_b")
	pl.mu.Lock()
	n := len(pl.idle)
	pl.mu.Unlock()
	if n != 1 {
		t.Fatalf("idle = %d, want 1", n)
	}
	if got := poolPID(t, pl, a, "dl_a2"); got == pidA {
		t.Error("evicted process reused")
	}
}

func TestPoolClose(t *testing.T) {
	t.Parallel()
	pl := NewPool(PoolOptions{Options: testOpts})
	spec := fakeSpec("echo", nil)
	poolPID(t, pl, spec, "dl_1")
	pl.mu.Lock()
	p := pl.idle[0].p
	pl.mu.Unlock()
	if err := pl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitExited(t, p)
	if _, err := pl.Deliver(t.Context(), spec, delivery("dl_2", `{}`)); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Deliver after Close = %v, want ErrPoolClosed", err)
	}
}

func TestPoolReleaseAfterCloseClosesProcess(t *testing.T) {
	t.Parallel()
	pl := NewPool(PoolOptions{Options: testOpts})
	p, err := Start(t.Context(), fakeSpec("echo", nil), testOpts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := pl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A delivery that was in flight during Close comes back: its process is closed, not kept idle.
	pl.release("key", p)
	waitExited(t, p)
	if len(pl.idle) != 0 {
		t.Errorf("idle = %d after Close", len(pl.idle))
	}
}

func TestPoolKeyEncodeError(t *testing.T) {
	t.Parallel()
	pl := newTestPool(t, 0)
	spec := fakeSpec("echo", map[string]any{"bad": make(chan int)})
	if _, err := pl.Deliver(t.Context(), spec, delivery("dl_1", `{}`)); err == nil {
		t.Error("Deliver with an unencodable config succeeded")
	}
}

func TestPoolPerItemStartFailure(t *testing.T) {
	t.Parallel()
	pl := NewPool(PoolOptions{}) // the exec launcher, which refuses a spec without a command
	t.Cleanup(func() { _ = pl.Close() })
	spec := fakeSpec("echo", nil)
	spec.Lifecycle = pipeline.LifecyclePerItem
	spec.Command = nil
	_, err := pl.Deliver(t.Context(), spec, delivery("dl_1", `{}`))
	requireClass(t, err, ClassCrash)
}
