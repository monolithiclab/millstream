package exec

import (
	"context"
	"encoding/json"
	"io"
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
		d := delivery("dl_1", bigPayload(pipeFill))
		d.Timeout = answerTimeout // the fake never reads the item
		_, err := deliverWithin(t, t.Context(), p, d)
		requireClass(t, err, ClassTimeout)
		waitExited(t, p)
	})
	t.Run("context", func(t *testing.T) {
		t.Parallel()
		p := startFake(t, "noRead", nil)
		ctx, cancel := context.WithTimeout(t.Context(), answerTimeout)
		defer cancel()
		d := delivery("dl_1", bigPayload(pipeFill))
		d.Timeout = 0
		_, err := deliverWithin(t, ctx, p, d)
		if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Fatalf("err = %v, want the context's", err)
		}
		waitExited(t, p)
	})
}

// An item larger than a protocol line fails the delivery as a permanent protocol error and leaves the process
// healthy for the next delivery. The line limit is 1 KiB here: TestOversizedItemAtMaxLine (e2e) builds an item of
// MaxLine.
func TestOversizedItemIsPermanentAndKeepsProcess(t *testing.T) {
	t.Parallel()
	opts := testOpts
	opts.maxLine = 1 << 10
	testOversizedItem(t, opts)
}

// testOversizedItem delivers an item over the line limit of opts between two that fit.
func testOversizedItem(t *testing.T, opts Options) {
	t.Helper()
	p, err := Start(t.Context(), fakeSpec("echo", nil), opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	res, err := p.Deliver(t.Context(), delivery("dl_0", `{}`))
	if err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	pid := decodeEcho(t, res).PID
	_, err = deliverWithin(t, t.Context(), p, delivery("dl_1", bigPayload(p.opts.maxLine)))
	requireClass(t, err, ClassProtocol)
	if !Permanent(err) {
		t.Fatalf("err = %v, want a permanent error", err)
	}
	if !p.Alive() {
		t.Fatal("process killed for an item it never received")
	}
	res, err = p.Deliver(t.Context(), delivery("dl_2", `{}`))
	if err != nil {
		t.Fatalf("next delivery: %v", err)
	}
	if out := decodeEcho(t, res); out.PID != pid {
		t.Fatalf("next delivery ran on pid %d, want %d", out.PID, pid)
	}
}

// TestHelloWriteHonoursReadyTimeoutInProcess: TestHelloWriteHonoursReadyTimeout (runtime_e2e_test.go) with an
// in-process action that never reads stdin: on an in-memory pipe, the hello write blocks at once.
func TestHelloWriteHonoursReadyTimeoutInProcess(t *testing.T) {
	t.Parallel()
	opts := funcOpts(func(ctx context.Context, _, _ []string, _ io.Reader, _, _ io.Writer) int {
		<-ctx.Done()
		return 1
	})
	opts.ReadyTimeout = answerTimeout
	p, err := Start(t.Context(), fakeSpec("echo", nil), opts)
	if err == nil {
		_ = p.Close()
	}
	requireClass(t, err, ClassTimeout)
}

// TestLateMetricIsIgnoredInProcess: TestLateMetricIsIgnored (runtime_e2e_test.go) without its sleep. The action
// signals once its late metric is written: the runtime's reader has taken it, so the next delivery finds it
// waiting while idle.
func TestLateMetricIsIgnoredInProcess(t *testing.T) {
	t.Parallel()
	sent := make(chan struct{})
	opts := funcOpts(func(_ context.Context, _, _ []string, stdin io.Reader, stdout, _ io.Writer) int {
		r, w := protocol.NewReader(stdin), protocol.NewWriter(stdout)
		if _, err := r.Read(); err != nil { // hello
			return 1
		}
		_ = w.Write(protocol.Ready{})
		for n := 0; ; n++ {
			m, err := r.Read()
			if err != nil {
				return 0
			}
			it, ok := m.(protocol.Item)
			if !ok {
				return 0
			}
			_ = w.Write(protocol.Emit{Delivery: it.Delivery, Payload: json.RawMessage(`{}`)})
			_ = w.Write(protocol.Done{Delivery: it.Delivery})
			if n == 0 {
				_ = w.Write(protocol.Metric{Delivery: it.Delivery, Name: "late", Value: 1})
				close(sent)
			}
		}
	})
	logs := &syncBuffer{}
	opts.Logger = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p, err := Start(t.Context(), fakeSpec("echo", nil), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Deliver(t.Context(), delivery("dl_1", `{}`)); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	<-sent
	if _, err := p.Deliver(t.Context(), delivery("dl_2", `{}`)); err != nil {
		t.Fatalf("delivery after a late metric: %v", err)
	}
	if !strings.Contains(logs.String(), "late metric ignored") {
		t.Errorf("late metric not logged:\n%s", logs.String())
	}
}
