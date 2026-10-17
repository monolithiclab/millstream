package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest/fakeexec"
	"github.com/monolithiclab/millstream/internal/store"
)

// fakeBin is the fake action's command: in-process in the default build, the built binary in the e2e build.
var fakeBin string

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	defer fakeexec.Cleanup()
	var err error
	if fakeBin, err = fakeexec.Command(); err != nil {
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
	lose        map[store.LeaseRef]bool // the leases the next heartbeat reports lost
	dones       []store.DoneRequest
	fails       []store.FailRequest
	reportErr   error
	pullErr     error
	hbErr       error
	putErr      error
	blobs       map[string][]byte // artifact contents by pipeline/sha256

	pulled   chan string // a delivery id per assignment handed out
	lost     chan string // a delivery id per lease reported lost
	reported chan string // a delivery id per Done or Fail
	beats    chan int    // the number of leases per Heartbeat call
}

func newFakeTransport(as ...Assignment) *fakeTransport {
	return &fakeTransport{
		queue: as, lose: map[store.LeaseRef]bool{},
		pulled: make(chan string, 64), lost: make(chan string, 64), reported: make(chan string, 64),
		beats: make(chan int, 64),
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
	select {
	case f.beats <- len(refs):
	default: // a real-time test heartbeating faster than it reads, under load: never block the transport
	}
	if f.hbErr != nil {
		err := f.hbErr
		f.hbErr = nil
		return nil, err
	}
	var lost []store.LeaseRef
	for _, r := range refs {
		if f.lose[r] {
			delete(f.lose, r)
			lost = append(lost, r)
			f.lost <- r.DeliveryID
		}
	}
	return lost, nil
}

// PutArtifact keeps the content in memory, by hash.
func (f *fakeTransport) PutArtifact(_ context.Context, lease store.LeaseRef, name string, r io.Reader) (store.ArtifactRef, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return store.ArtifactRef{}, err
	}
	sum := sha256.Sum256(data)
	ref := store.ArtifactRef{Name: name, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data)), Type: "text/plain"}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return store.ArtifactRef{}, f.putErr
	}
	if f.blobs == nil {
		f.blobs = map[string][]byte{}
	}
	f.blobs[lease.Pipeline+"/"+ref.SHA256] = data
	return ref, nil
}

// OpenArtifact reads a content PutArtifact kept.
func (f *fakeTransport) OpenArtifact(_ context.Context, lease store.LeaseRef, ref store.ArtifactRef) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.blobs[lease.Pipeline+"/"+ref.SHA256]
	if !ok {
		return nil, fmt.Errorf("artifact %s: %w", ref.SHA256, fs.ErrNotExist)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
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
func await[T any](t *testing.T, ch <-chan T, n int) []T {
	t.Helper()
	var got []T
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

// startNode runs a node over tr until the returned stop is called; stop returns Run's error. Its ticks and drain
// timer are real: the default intervals, which no test waits for, unless opts sets shorter ones (the e2e twins).
func startNode(t *testing.T, tr Transport, opts Options) (stop func() error) {
	t.Helper()
	return startNodeOn(t, tr, opts, nil)
}

// startNodeClock is startNode on a fake clock, whose ticks and drain timer the test fires.
func startNodeClock(t *testing.T, tr Transport, opts Options) (stop func() error, clk *fakeClock) {
	t.Helper()
	clk = newFakeClock(t)
	return startNodeOn(t, tr, opts, clk), clk
}

func startNodeOn(t *testing.T, tr Transport, opts Options, clk *fakeClock) (stop func() error) {
	t.Helper()
	if opts.Owner == "" {
		opts.Owner = "node-a"
	}
	if opts.Pool == nil {
		opts.Pool = fakeexec.NewPool(t, exec.Options{ShutdownTimeout: 300 * time.Millisecond})
	}
	n, err := New(tr, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if clk != nil {
		n.newTimer = clk.newTimer
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
				t.Error("Run did not return") // not Fatal: stop may run on another goroutine
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

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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

// fakeClock replaces a node's ticks and drain timer: a channel per duration, which fire sends on.
type fakeClock struct {
	t      *testing.T
	mu     sync.Mutex
	ticks  map[time.Duration]chan time.Time
	timers chan time.Duration // the duration of each one-shot timer made: the drain timeout, once draining
}

func newFakeClock(t *testing.T) *fakeClock {
	return &fakeClock{t: t, ticks: map[time.Duration]chan time.Time{}, timers: make(chan time.Duration, 8)}
}

func (c *fakeClock) ch(d time.Duration) chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ticks[d] == nil {
		c.ticks[d] = make(chan time.Time)
	}
	return c.ticks[d]
}

func (c *fakeClock) newTimer(d time.Duration, repeat bool) (<-chan time.Time, func()) {
	ch := c.ch(d)
	if !repeat {
		c.timers <- d
	}
	return ch, func() {}
}

// fire delivers a tick of the timer of duration d, returning once the node has taken it.
func (c *fakeClock) fire(d time.Duration) {
	c.t.Helper()
	select {
	case c.ch(d) <- time.Time{}:
	case <-time.After(10 * time.Second):
		c.t.Fatalf("no timer of %s took the tick", d)
	}
}

// draining waits until the node has started its drain timer.
func (c *fakeClock) draining() {
	c.t.Helper()
	if d := await(c.t, c.timers, 1)[0]; d != DefaultDrainTimeout {
		c.t.Fatalf("timer of %s made, want the drain timeout", d)
	}
}

// heldPool runs the fake action in-process, in both builds, and holds every delivery before the action starts
// until release is called (or the delivery is cancelled, which kills the action). started receives once per
// delivery held: the node holds its lease by then.
func heldPool(t *testing.T) (pool *exec.Pool, release func(), started <-chan struct{}) {
	t.Helper()
	gate := make(chan struct{})
	starts := make(chan struct{}, 64)
	held := func(ctx context.Context, args, env []string, stdin io.Reader, stdout, stderr io.Writer) int {
		starts <- struct{}{}
		select {
		case <-gate:
		case <-ctx.Done():
			return 1
		}
		return exectest.Serve(ctx, args, env, stdin, stdout, stderr)
	}
	pool = exec.NewPool(exec.PoolOptions{Options: exec.Options{
		Launchers: map[string]exec.Launcher{pipeline.KindExec: exec.FuncLauncher{F: held}},
	}})
	t.Cleanup(func() { _ = pool.Close() })
	var once sync.Once
	return pool, func() { once.Do(func() { close(gate) }) }, starts
}
