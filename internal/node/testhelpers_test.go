package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
	"github.com/monolithiclab/millstream/internal/store"
)

// fakeBin is the fake action, built once by TestMain.
var fakeBin string

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	dir, err := os.MkdirTemp("", "node-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if fakeBin, err = exectest.Build(dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}

// assignment builds a leased delivery of the fake action in mode.
func assignment(id, mode string, config map[string]any) Assignment {
	c := map[string]any{"mode": mode}
	for k, v := range config {
		c[k] = v
	}
	lease := store.Lease{
		DeliveryID: id, Pipeline: "listings", RunID: "run_1", Step: "score", Attempt: 2, FencingToken: 7,
		Item: store.Item{ID: "it_" + id, Key: "key-" + id},
	}
	return Assignment{
		Lease: lease,
		Spec: exec.Spec{
			Action: "lab/fake", Version: "1.0.0", Digest: "sha256:fake", Command: []string{fakeBin},
			Dir: os.TempDir(), Config: c,
		},
		ItemMsg: protocol.Item{Delivery: id, ID: "it_" + id, Key: "key-" + id, Payload: json.RawMessage(`{}`), Attempt: 2},
		Timeout: 5 * time.Second,
	}
}

// fakeTransport serves a fixed queue of assignments and records what the node reports.
type fakeTransport struct {
	mu          sync.Mutex
	queue       []Assignment
	pullMax     []int
	outstanding int // pulled and not yet reported
	maxOut      int
	heartbeats  [][]store.LeaseRef
	lose        map[string]bool // delivery ids whose lease the next heartbeat reports lost
	dones       []store.DoneRequest
	fails       []store.FailRequest
	reportErr   error
	pullErr     error
	hbErr       error

	pulled   chan string // a delivery id per assignment handed out
	lost     chan string // a delivery id per lease reported lost
	reported chan string // a delivery id per Done or Fail
}

func newFakeTransport(as ...Assignment) *fakeTransport {
	return &fakeTransport{
		queue: as, lose: map[string]bool{},
		pulled: make(chan string, 64), lost: make(chan string, 64), reported: make(chan string, 64),
	}
}

func (f *fakeTransport) Pull(_ context.Context, _ string, maxN int) ([]Assignment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pullMax = append(f.pullMax, maxN)
	if f.pullErr != nil {
		err := f.pullErr
		f.pullErr = nil
		return nil, err
	}
	n := min(maxN, len(f.queue))
	out := f.queue[:n]
	f.queue = f.queue[n:]
	f.outstanding += n
	f.maxOut = max(f.maxOut, f.outstanding)
	for _, a := range out {
		f.pulled <- a.Lease.DeliveryID
	}
	return out, nil
}

func (f *fakeTransport) Heartbeat(_ context.Context, _ string, refs []store.LeaseRef) ([]store.LeaseRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats = append(f.heartbeats, refs)
	if f.hbErr != nil {
		err := f.hbErr
		f.hbErr = nil
		return nil, err
	}
	var lost []store.LeaseRef
	for _, r := range refs {
		if f.lose[r.DeliveryID] {
			delete(f.lose, r.DeliveryID)
			lost = append(lost, r)
			f.lost <- r.DeliveryID
		}
	}
	return lost, nil
}

func (f *fakeTransport) Done(_ context.Context, req store.DoneRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dones = append(f.dones, req)
	return f.report(req.DeliveryID)
}

func (f *fakeTransport) Fail(_ context.Context, req store.FailRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fails = append(f.fails, req)
	return f.report(req.DeliveryID)
}

func (f *fakeTransport) report(id string) error {
	f.outstanding--
	f.reported <- id
	return f.reportErr
}

// await receives n values from ch, failing the test after a timeout.
func await(t *testing.T, ch <-chan string, n int) []string {
	t.Helper()
	var got []string
	for range n {
		select {
		case id := <-ch:
			got = append(got, id)
		case <-time.After(10 * time.Second):
			t.Fatalf("waited for %d events, got %v", n, got)
		}
	}
	return got
}

// startNode runs a node over tr until the returned stop is called; stop returns Run's error.
func startNode(t *testing.T, tr Transport, opts Options) (stop func() error) {
	t.Helper()
	if opts.Owner == "" {
		opts.Owner = "node-a"
	}
	if opts.Pool == nil {
		pool := exec.NewPool(exec.PoolOptions{Options: exec.Options{ShutdownTimeout: 300 * time.Millisecond}})
		t.Cleanup(func() { _ = pool.Close() })
		opts.Pool = pool
	}
	if opts.PullInterval == 0 {
		opts.PullInterval = 20 * time.Millisecond
	}
	n, err := New(tr, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() { errc <- n.Run(ctx) }()
	var once sync.Once
	var runErr error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case runErr = <-errc:
			case <-time.After(10 * time.Second):
				t.Fatal("Run did not return")
			}
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return stop
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

func (b *syncBuffer) logs(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, line := range bytes.Split(b.buf.Bytes(), []byte("\n")) {
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
