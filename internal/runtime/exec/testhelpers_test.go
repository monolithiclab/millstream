package exec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
)

// fakeBin is the fake action's command, and testOpts the options that run it: in-process in the default build, the
// built binary in the e2e build (fake_test.go, fake_e2e_test.go).
var fakeBin string

const millisecond = time.Millisecond

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	defer cleanupFake()
	var err error
	if fakeBin, err = fakeCommand(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	testOpts.Launchers = fakeLaunchers()
	return m.Run()
}

// testOpts keep the failure paths fast. The ready timeout is only a safety bound.
var testOpts = Options{ReadyTimeout: 2 * time.Second, ShutdownTimeout: shutdownTimeout}

// funcOpts are testOpts running every exec action as f, in-process.
func funcOpts(f func(ctx context.Context, args, env []string, stdin io.Reader, stdout, stderr io.Writer) int) Options {
	opts := testOpts
	opts.Launchers = map[string]Launcher{"exec": FuncLauncher{F: f}}
	return opts
}

// fakeSpec runs the fake action in mode, with extra config keys.
func fakeSpec(mode string, config map[string]any) Spec {
	c := map[string]any{"mode": mode}
	for k, v := range config {
		c[k] = v
	}
	return Spec{
		Action: "lab/fake", Version: "1.0.0", Digest: "sha256:fake-" + mode,
		Command: []string{fakeBin}, Dir: os.TempDir(), Config: c, Vars: map[string]any{"city": "Lyon"},
	}
}

func item(id, payload string) protocol.Item {
	return protocol.Item{Delivery: id, ID: "it_" + id, Key: "key-" + id, Payload: json.RawMessage(payload), Attempt: 1}
}

func delivery(id, payload string) Delivery {
	return Delivery{Item: item(id, payload), Timeout: 5 * time.Second}
}

func startFake(t *testing.T, mode string, config map[string]any) *Process {
	t.Helper()
	p, err := Start(t.Context(), fakeSpec(mode, config), testOpts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// echoOut decodes the fake action's echo output.
type echoOut struct {
	PID     int             `json:"pid"`
	Workdir string          `json:"workdir"`
	In      json.RawMessage `json:"in"`
}

func decodeEcho(t *testing.T, res Result) echoOut {
	t.Helper()
	if len(res.Outputs) != 1 {
		t.Fatalf("outputs = %d, want 1", len(res.Outputs))
	}
	var out echoOut
	if err := json.Unmarshal(res.Outputs[0].Payload, &out); err != nil {
		t.Fatalf("decode echo: %v", err)
	}
	return out
}

// syncBuffer is a bytes.Buffer safe for the concurrent writes of a slog handler.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func requireClass(t *testing.T, err error, class string) {
	t.Helper()
	if err == nil {
		t.Fatalf("err = nil, want a %s error", class)
	}
	if got := ClassOf(err); got != class {
		t.Fatalf("class = %s (%v), want %s", got, err, class)
	}
}

// waitExited waits for the process to be reaped.
func waitExited(t *testing.T, p *Process) {
	t.Helper()
	select {
	case <-p.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("process did not exit")
	}
}

func jsonLogs(t *testing.T, s string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range bytes.Split([]byte(s), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func newJSONLogger(w *syncBuffer) *slog.Logger { return slog.New(slog.NewJSONHandler(w, nil)) }
