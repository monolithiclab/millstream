package exec

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
)

// deliverWithin runs p.Deliver and fails the test if it does not return in time: the regressions below used to
// block forever.
func deliverWithin(t *testing.T, ctx context.Context, p *Process, d Delivery) (Result, error) {
	t.Helper()
	type out struct {
		res Result
		err error
	}
	ch := make(chan out, 1)
	go func() {
		res, err := p.Deliver(ctx, d)
		ch <- out{res, err}
	}()
	select {
	case o := <-ch:
		return o.res, o.err
	case <-time.After(10 * time.Second):
		t.Fatal("Deliver did not return")
		return Result{}, nil
	}
}

// bigPayload is a JSON string payload of n bytes: larger than a pipe buffer, so writing it blocks on a process
// that does not read.
func bigPayload(n int) string { return `"` + strings.Repeat("x", n) + `"` }

// The item write honours the delivery's timeout and context: a process that never reads stdin does not block it.
func TestItemWriteHonoursTimeoutAndContext(t *testing.T) {
	t.Parallel()
	t.Run("timeout", func(t *testing.T) {
		t.Parallel()
		p := startFake(t, "noRead", nil)
		d := delivery("dl_1", bigPayload(4<<20))
		d.Timeout = 300 * time.Millisecond
		_, err := deliverWithin(t, t.Context(), p, d)
		requireClass(t, err, ClassTimeout)
		waitExited(t, p)
	})
	t.Run("context", func(t *testing.T) {
		t.Parallel()
		p := startFake(t, "noRead", nil)
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		d := delivery("dl_1", bigPayload(4<<20))
		d.Timeout = 0
		_, err := deliverWithin(t, ctx, p, d)
		if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Fatalf("err = %v, want the context's", err)
		}
		waitExited(t, p)
	})
}

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

// An item larger than a protocol line fails the delivery as a permanent protocol error and leaves the process
// healthy for the next delivery.
func TestOversizedItemIsPermanentAndKeepsProcess(t *testing.T) {
	t.Parallel()
	p := startFake(t, "echo", nil)
	_, err := deliverWithin(t, t.Context(), p, delivery("dl_1", bigPayload(protocol.MaxLine)))
	requireClass(t, err, ClassProtocol)
	if !Permanent(err) {
		t.Fatalf("err = %v, want a permanent error", err)
	}
	if !p.Alive() {
		t.Fatal("process killed for an item it never received")
	}
	res, err := p.Deliver(t.Context(), delivery("dl_2", `{}`))
	if err != nil {
		t.Fatalf("next delivery: %v", err)
	}
	if out := decodeEcho(t, res); out.PID != p.cmd.Process.Pid {
		t.Fatalf("next delivery ran on pid %d, want %d", out.PID, p.cmd.Process.Pid)
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
