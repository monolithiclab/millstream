package exec

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
)

func TestDeliverEcho(t *testing.T) {
	t.Parallel()
	p := startFake(t, "echo", nil)
	res, err := p.Deliver(t.Context(), delivery("dl_1", `{"url":"u"}`))
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if res.Err != nil {
		t.Fatalf("action error: %+v", res.Err)
	}
	out := decodeEcho(t, res)
	if res.Outputs[0].Key != "key-dl_1" || string(out.In) != `{"url":"u"}` || out.PID == 0 {
		t.Errorf("output = %+v / %+v", res.Outputs[0], out)
	}
	if out.Workdir != p.workdir {
		t.Errorf("MILLSTREAM_WORKDIR = %q, want %q", out.Workdir, p.workdir)
	}
	if !p.Alive() {
		t.Error("process not alive after a good delivery")
	}
}

func TestDeliverKeyedOutputsAndMetrics(t *testing.T) {
	t.Parallel()
	p := startFake(t, "keyed", map[string]any{"n": 3})
	res, err := p.Deliver(t.Context(), delivery("dl_1", `{}`))
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(res.Outputs) != 3 {
		t.Fatalf("outputs = %+v", res.Outputs)
	}
	for i, o := range res.Outputs {
		if want := "k" + string(rune('0'+i)); o.Key != want || string(o.Payload) != string(rune('0'+i)) {
			t.Errorf("output %d = %s %s", i, o.Key, o.Payload)
		}
	}
	if len(res.Metrics) != 1 || res.Metrics[0].Name != "outputs" || res.Metrics[0].Value != 3 {
		t.Errorf("metrics = %+v", res.Metrics)
	}
}

func TestDeliverActionError(t *testing.T) {
	t.Parallel()
	for _, retryable := range []bool{true, false} {
		p := startFake(t, "error", map[string]any{"retryable": retryable})
		res, err := p.Deliver(t.Context(), delivery("dl_1", `{}`))
		if err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		if res.Err == nil || res.Err.Retryable != retryable || res.Err.Message != "portal answered 503" {
			t.Errorf("retryable=%v: Err = %+v", retryable, res.Err)
		}
		if res.Outputs != nil {
			t.Errorf("outputs on error: %+v", res.Outputs)
		}
		if !p.Alive() {
			t.Error("an action error must not end the process")
		}
	}
}

// TestDeliverRuntimeFailures pins review focus 4: garbage, an exit mid-delivery, no answer and out-of-protocol
// messages fail the delivery with a retryable runtime error, publish nothing, and leave the process dead.
func TestDeliverRuntimeFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		mode    string
		class   string
		wantMsg string
	}{
		{"crash", ClassCrash, "exit status 3"},
		{"garbage", ClassProtocol, "malformed line"},
		{"hang", ClassTimeout, "no answer within"},
		{"wrongId", ClassProtocol, `delivery "dl_other"`},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			t.Parallel()
			p := startFake(t, "echo", nil)
			d := delivery("dl_1", `{"mode":"`+tt.mode+`"}`)
			if tt.class == ClassTimeout {
				d.Timeout = answerTimeout
			}
			res, err := p.Deliver(t.Context(), d)
			requireClass(t, err, tt.class)
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error %q does not mention %q", err, tt.wantMsg)
			}
			if tt.class == ClassProtocol && !errors.Is(err, protocol.ErrProtocol) {
				t.Errorf("error %v does not wrap ErrProtocol", err)
			}
			if res.Outputs != nil {
				t.Errorf("outputs published: %+v", res.Outputs)
			}
			waitExited(t, p)
			if p.Alive() {
				t.Error("process alive after a runtime failure")
			}
		})
	}
}

func TestEmitAfterDoneIsAViolation(t *testing.T) {
	t.Parallel()
	p := startFake(t, "emitAfterDone", nil)
	if _, err := p.Deliver(t.Context(), delivery("dl_1", `{}`)); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	// The action writes the stray emit before reading the next item, so the next delivery sees it either while
	// idle or ahead of its own answer.
	_, err := p.Deliver(t.Context(), delivery("dl_2", `{"mode":"echo"}`))
	requireClass(t, err, ClassProtocol)
	if !strings.Contains(err.Error(), "emit") {
		t.Errorf("error %q does not name the stray emit", err)
	}
}

func TestDeliverAfterIdleExit(t *testing.T) {
	t.Parallel()
	p := startFake(t, "echo", nil)
	p.terminate()
	waitExited(t, p)
	_, err := p.Deliver(t.Context(), delivery("dl_1", `{}`))
	requireClass(t, err, ClassCrash)
}

func TestDeliverCancelledKills(t *testing.T) {
	t.Parallel()
	p := startFake(t, "hang", nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := p.Deliver(ctx, delivery("dl_1", `{}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	waitExited(t, p)
}

func TestStartCancelled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := Start(ctx, fakeSpec("noReady", nil), testOpts)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestStderrReemittedWithKeys(t *testing.T) {
	t.Parallel()
	var base, dl syncBuffer
	opts := testOpts
	opts.Logger = newJSONLogger(&base)
	p, err := Start(t.Context(), fakeSpec("stderr", nil), opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	keys := []any{"pipeline", "listings", "runId", "run_1", "step", "score", "itemId", "it_1", "deliveryId", "dl_1",
		"attempt", 2, "node", "node-a"}
	d := delivery("dl_1", `{"secretPayloadValue":"hunter2"}`)
	d.Log = newJSONLogger(&dl).With(keys...)
	if _, err := p.Deliver(t.Context(), d); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if err := p.Close(); err != nil { // waits for stderr to drain
		t.Fatalf("Close: %v", err)
	}
	lines := jsonLogs(t, dl.String())
	var msgs []string
	for _, l := range lines {
		msgs = append(msgs, l["msg"].(string))
		if l["level"] != "INFO" {
			t.Errorf("level = %v", l["level"])
		}
		for i := 0; i < len(keys); i += 2 {
			want, _ := json.Marshal(keys[i+1])
			got, _ := json.Marshal(l[keys[i].(string)])
			if string(got) != string(want) {
				t.Errorf("%s = %s, want %s", keys[i], got, want)
			}
		}
	}
	if strings.Join(msgs, "|") != "fetching page|parsed listing" {
		t.Errorf("messages = %q", msgs)
	}
	if all := dl.String() + base.String(); strings.Contains(all, "hunter2") || strings.Contains(all, "secretPayloadValue") {
		t.Error("a payload value reached the logs")
	}
}

func TestCloseGraceful(t *testing.T) {
	t.Parallel()
	p := startFake(t, "echo", nil)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if p.waitErr != nil {
		t.Errorf("exit = %v, want status 0", p.waitErr)
	}
	if _, err := os.Stat(p.workdir); !os.IsNotExist(err) {
		t.Errorf("workdir left behind: %v", err)
	}
	if err := p.Close(); err != nil { // idempotent
		t.Errorf("second Close: %v", err)
	}
}

func TestCloseKillsAfterTimeout(t *testing.T) {
	t.Parallel()
	opts := testOpts
	opts.ShutdownTimeout = answerTimeout
	p, err := Start(t.Context(), fakeSpec("hang", nil), opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	start := time.Now()
	err = p.Close()
	if err == nil || !strings.Contains(err.Error(), "killed") {
		t.Fatalf("Close = %v, want a killed error", err)
	}
	if elapsed := time.Since(start); elapsed < opts.ShutdownTimeout {
		t.Errorf("killed after %s, before the shutdown timeout", elapsed)
	}
}

func TestWorkdirClearedBetweenDeliveries(t *testing.T) {
	t.Parallel()
	p := startFake(t, "echo", nil)
	if err := os.WriteFile(p.workdir+"/leftover", []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Deliver(t.Context(), delivery("dl_1", `{}`)); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	entries, err := os.ReadDir(p.workdir)
	if err != nil || len(entries) != 0 {
		t.Errorf("workdir entries = %v, %v", entries, err)
	}
}

func TestClassOf(t *testing.T) {
	t.Parallel()
	if got := ClassOf(errors.New("other")); got != ClassCrash {
		t.Errorf("ClassOf(plain) = %s", got)
	}
	wrapped := errors.Join(runtimeErr(ClassTimeout, "late"))
	if got := ClassOf(wrapped); got != ClassTimeout {
		t.Errorf("ClassOf(wrapped) = %s", got)
	}
}

// TestStartFailuresInProcess: the start failures of TestStartFailures (process_e2e_test.go) that do not need a
// real command, with in-process actions.
func TestStartFailuresInProcess(t *testing.T) {
	t.Parallel()
	noCommand := fakeSpec("echo", nil)
	noCommand.Command = nil
	early := func(ctx context.Context, _, _ []string, stdin io.Reader, stdout, _ io.Writer) int {
		if _, err := protocol.NewReader(stdin).Read(); err != nil { // hello
			return 1
		}
		_, _ = io.WriteString(stdout, `{"type":"done","delivery":"x"}`+"\n")
		<-ctx.Done()
		return 1
	}
	exit4 := func(context.Context, []string, []string, io.Reader, io.Writer, io.Writer) int { return 4 }
	noReady := testOpts
	noReady.ReadyTimeout = answerTimeout // the fake never answers
	tests := []struct {
		name  string
		spec  Spec
		opts  Options
		class string
	}{
		{"no command", noCommand, Options{}, ClassCrash},
		{"no ready", fakeSpec("noReady", nil), noReady, ClassTimeout},
		{"exits before ready", fakeSpec("echo", nil), funcOpts(exit4), ClassCrash},
		{"message before ready", fakeSpec("echo", nil), funcOpts(early), ClassProtocol},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := Start(t.Context(), tt.spec, tt.opts)
			if p != nil {
				t.Fatal("Start returned a process on failure")
			}
			requireClass(t, err, tt.class)
		})
	}
}

// TestStderrOutsideDeliveryInProcess: TestStderrOutsideDeliveryAndLongLines (process_e2e_test.go) in-process.
func TestStderrOutsideDeliveryInProcess(t *testing.T) {
	t.Parallel()
	var base syncBuffer
	long := strings.Repeat("x", maxLogLine+100)
	opts := funcOpts(func(_ context.Context, _, _ []string, stdin io.Reader, stdout, stderr io.Writer) int {
		_, _ = io.WriteString(stderr, long+"\nstarting\n")
		_, _ = io.WriteString(stdout, `{"type":"ready"}`+"\n")
		_, _ = io.Copy(io.Discard, stdin) // hello, then shutdown until stdin closes
		return 0
	})
	opts.Logger = newJSONLogger(&base)
	p, err := Start(t.Context(), fakeSpec("echo", nil), opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	lines := jsonLogs(t, base.String())
	if len(lines) != 2 || len(lines[0]["msg"].(string)) != maxLogLine || lines[1]["msg"] != "starting" {
		t.Errorf("got %d lines; second = %v", len(lines), lines[len(lines)-1]["msg"])
	}
}

// A context cancelled while the handshake awaits ready ends Start with the context's error, the action killed.
func TestStartCancelledAwaitingReady(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	opts := funcOpts(func(actx context.Context, _, _ []string, stdin io.Reader, _, _ io.Writer) int {
		if _, err := protocol.NewReader(stdin).Read(); err != nil { // hello
			return 1
		}
		cancel() // never ready: the handshake is waiting for it
		<-actx.Done()
		return 1
	})
	p, err := Start(ctx, fakeSpec("echo", nil), opts)
	if p != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v, %v; want context.Canceled", p, err)
	}
}
