//go:build e2e

package exec

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

// These regressions run against real processes: a command that never reads stdin, and a late metric racing the
// next delivery. runtime_regress_test.go has in-process versions.

// The hello write honours the ready timeout: a process that never reads stdin does not block Start.
func TestHelloWriteHonoursReadyTimeout(t *testing.T) {
	t.Parallel()
	spec := Spec{Action: "lab/sleep", Command: []string{"sleep", "60"}, Dir: t.TempDir(),
		Config: map[string]any{"big": strings.Repeat("x", 4<<20)}}
	done := make(chan error, 1)
	go func() {
		p, err := Start(t.Context(), spec, Options{ReadyTimeout: 300 * time.Millisecond})
		if err == nil {
			_ = p.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		requireClass(t, err, ClassTimeout)
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return")
	}
}

// A metric sent after done is ignored, whether it arrives while idle or ahead of the next delivery's answer.
func TestLateMetricIsIgnored(t *testing.T) {
	t.Parallel()
	logs := &syncBuffer{}
	opts := testOpts
	opts.Logger = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p, err := Start(t.Context(), fakeSpec("lateMetric", nil), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Deliver(t.Context(), delivery("dl_1", `{}`)); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let the late metric arrive while idle, most of the time
	res, err := p.Deliver(t.Context(), delivery("dl_2", `{"mode":"echo"}`))
	if err != nil {
		t.Fatalf("delivery after a late metric: %v", err)
	}
	decodeEcho(t, res)
	if !strings.Contains(logs.String(), "late metric ignored") {
		t.Errorf("late metric not logged:\n%s", logs.String())
	}
}
