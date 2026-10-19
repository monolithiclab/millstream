//go:build e2e

package exec

import (
	"encoding/json"
	"slices"
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

// Any difference in what a process starts with keys another process: an action shared by two pipelines with other
// secrets (env) or packages (mounts), a container image run with another network or command.
func TestPoolKeysOnTheLaunchSpec(t *testing.T) {
	t.Parallel()
	pl := newTestPool(t, 16) // room for every variant: the base process must stay idle
	base := fakeSpec("echo", nil)
	pid := poolPID(t, pl, base, "dl_0")
	with := func(edit func(*Spec)) Spec {
		s := base
		edit(&s)
		return s
	}
	for name, spec := range map[string]Spec{
		"digest":  with(func(s *Spec) { s.Digest = "sha256:other" }),
		"config":  fakeSpec("echo", map[string]any{"extra": 1}),
		"vars":    with(func(s *Spec) { s.Vars = map[string]any{"city": "Paris"} }),
		"lessons": with(func(s *Spec) { s.Lessons = json.RawMessage(`{"rules":[{"text":"Be strict."}]}`) }),
		"env":     with(func(s *Spec) { s.Env = append(slices.Clone(s.Env), "API_KEY=rotated") }),
		"network": with(func(s *Spec) { s.Network = true }),
		"mounts":  with(func(s *Spec) { s.Mounts = []Mount{{Source: "/pkg/b", Target: "/pkg", ReadOnly: true}} }),
		"command": with(func(s *Spec) { s.Command = append(slices.Clone(s.Command), "--other") }),
		"image":   with(func(s *Spec) { s.Image = "sha256:image" }),
	} {
		if got := poolPID(t, pl, spec, "dl_"+name); got == pid {
			t.Errorf("another %s reused process %d", name, pid)
		}
	}
	if got := poolPID(t, pl, base, "dl_again"); got != pid {
		t.Errorf("same key: pid %d, want %d", got, pid)
	}
}
