package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest/fakeexec"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

func TestMain(m *testing.M) {
	code := m.Run()
	fakeexec.Cleanup()
	os.Exit(code)
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

// pkgDir is the directory every test package claims. The engine only joins paths onto it (it reads a package from
// its Files), and the default build's in-process launcher never runs a command there, so nothing is on disk.
const pkgDir = "/millstream-test/pkg"

// newPackage builds a package in memory: pipeline.yaml plus the fake manifest under actions/<name> for each name
// in actions, with its content hash, as pipeline.Load would read it from pkgDir.
func newPackage(t *testing.T, pipelineYAML string, actions ...string) *pipeline.Package {
	t.Helper()
	files := map[string][]byte{pipeline.FileName: []byte(pipelineYAML)}
	for _, a := range actions {
		files["actions/"+a+"/action.yaml"] = []byte(fakeManifest)
		files["actions/"+a+"/run"] = []byte("#!/bin/sh\n")
	}
	p, err := pipeline.Parse(files[pipeline.FileName])
	if err != nil {
		t.Fatalf("load package: %v", err)
	}
	// The hash as pipeline.Load computes it: SHA-256 over path NUL content NUL for each file in path order.
	h := sha256.New()
	for _, name := range slices.Sorted(maps.Keys(files)) {
		h.Write([]byte(name))
		h.Write([]byte{0})
		h.Write(files[name])
		h.Write([]byte{0})
	}
	return &pipeline.Package{Dir: pkgDir, Pipeline: p, Hash: hex.EncodeToString(h.Sum(nil)), Files: files,
		PipelineFile: pipeline.FileName}
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

func newLogger(b *syncBuffer) *slog.Logger { return slog.New(slog.NewJSONHandler(b, nil)) }
