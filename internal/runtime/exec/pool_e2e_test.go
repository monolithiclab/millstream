//go:build e2e

package exec

import (
	"sync"
	"testing"
)

// TestPoolConcurrentDeliveriesUseSeparateProcessesRealTime: TestPoolConcurrentDeliveriesUseSeparateProcesses with
// the built fake action, its deliveries overlapping for the time they sleep.
func TestPoolConcurrentDeliveriesUseSeparateProcessesRealTime(t *testing.T) {
	t.Parallel()
	pl := newTestPool(t, 0)
	spec := fakeSpec("slow", map[string]any{"sleepMs": slowMs})
	var wg sync.WaitGroup
	pids := make([]int, 3)
	for i := range pids {
		wg.Go(func() {
			res, err := pl.Deliver(t.Context(), spec, delivery("dl_c", `{}`))
			if err != nil {
				t.Errorf("Deliver: %v", err)
				return
			}
			pids[i] = decodeEcho(t, res).PID
		})
	}
	wg.Wait()
	if pids[0] == pids[1] || pids[1] == pids[2] || pids[0] == pids[2] {
		t.Errorf("pids %v: one process served concurrent deliveries", pids)
	}
}
