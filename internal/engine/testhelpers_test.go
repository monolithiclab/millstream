package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// fakeBin is the fake action, built once by TestMain.
var fakeBin string

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	dir, err := os.MkdirTemp("", "engine-test-")
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

var t0 = time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)

// fakeClock is a settable clock shared by the store and the engine.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// fakeManifest is an exec action whose command is a file of the package; most tests never run it.
const fakeManifest = `
name: fake
version: 1.0.0
kind: exec
runtime: { exec: { command: ["./run"] } }
`

// env is an engine under test over a temporary store with a fake clock.
type env struct {
	t     *testing.T
	ctx   context.Context
	clock *fakeClock
	st    *sqlite.Store
	e     *Engine
	logs  *syncBuffer
}

// newEnv opens a store and an engine on a fake clock starting at t0. opts.Store, Now and Logger are filled in.
func newEnv(t *testing.T, opts Options) *env {
	t.Helper()
	clock := &fakeClock{t: t0}
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: clock.Now, Rand: func() float64 { return 0.5 }})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	logs := &syncBuffer{}
	opts.Store, opts.Now, opts.Logger = st, clock.Now, newLogger(logs)
	e, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &env{t: t, ctx: context.Background(), clock: clock, st: st, e: e, logs: logs}
}

// writePackage writes a package (pipeline.yaml plus the fake manifest under actions/<name> for each name in
// actions) to a temporary directory and loads it.
func writePackage(t *testing.T, pipelineYAML string, actions ...string) *pipeline.Package {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{"pipeline.yaml": pipelineYAML}
	for _, a := range actions {
		files["actions/"+a+"/action.yaml"] = fakeManifest
		files["actions/"+a+"/run"] = "#!/bin/sh\n"
	}
	for name, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pkg, err := pipeline.Load(dir)
	if err != nil {
		t.Fatalf("load package: %v", err)
	}
	return pkg
}

// linkFakeAction places the fake action binary in the package directory dir at rel, hard-linked when possible.
func linkFakeAction(t *testing.T, dir, rel string) {
	t.Helper()
	dst := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(fakeBin, dst); err == nil {
		return
	}
	src, err := os.Open(fakeBin)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, 0o700) // #nosec G302 -- an executable test binary
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, src); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

func (v *env) apply(pkg *pipeline.Package) store.RevisionID {
	v.t.Helper()
	id, err := v.e.Apply(v.ctx, pkg)
	if err != nil {
		v.t.Fatalf("Apply: %v", err)
	}
	return id
}

// fire fires a trigger under key and returns the run id.
func (v *env) fire(pipelineName, trigger, key string) string {
	v.t.Helper()
	id, created, err := v.e.Fire(v.ctx, pipelineName, trigger, key, nil)
	if err != nil || !created {
		v.t.Fatalf("Fire %s.%s %s: created %v, %v", pipelineName, trigger, key, created, err)
	}
	return id
}

func (v *env) pull(n int) []string {
	v.t.Helper()
	as, err := v.e.Pull(v.ctx, "node-a", n)
	if err != nil {
		v.t.Fatalf("Pull: %v", err)
	}
	var steps []string
	for _, a := range as {
		steps = append(steps, a.Lease.Pipeline+"/"+a.Lease.Step)
	}
	return steps
}

func (v *env) run(pipelineName, id string) store.Run {
	v.t.Helper()
	r, err := v.st.Run(v.ctx, pipelineName, id)
	if err != nil {
		v.t.Fatalf("Run %s: %v", id, err)
	}
	return r
}

func (v *env) runs(pipelineName string) []store.Run {
	v.t.Helper()
	rs, err := v.st.Runs(v.ctx, store.RunFilter{Pipeline: pipelineName})
	if err != nil {
		v.t.Fatalf("Runs: %v", err)
	}
	return rs
}

func (v *env) events(kind string) []store.Event {
	v.t.Helper()
	evs, err := v.st.Events(v.ctx, 0, 100000)
	if err != nil {
		v.t.Fatalf("Events: %v", err)
	}
	var out []store.Event
	for _, ev := range evs {
		if kind == "" || ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

func (v *env) deliveries(runID string) []store.Delivery {
	v.t.Helper()
	ds, err := v.st.Deliveries(v.ctx, store.DeliveryFilter{RunID: runID})
	if err != nil {
		v.t.Fatalf("Deliveries: %v", err)
	}
	return ds
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

// lines decodes the JSON log lines.
func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range bytes.Split([]byte(b.String()), []byte("\n")) {
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

func newLogger(b *syncBuffer) *slog.Logger { return slog.New(slog.NewJSONHandler(b, nil)) }
