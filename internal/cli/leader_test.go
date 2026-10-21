package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
	"github.com/monolithiclab/millstream/internal/store/sqlite"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// Between terms the site answers unavailable; a term's handler serves until its context ends, and a later term's
// is not cleared by an earlier one ending.
func TestSiteTerms(t *testing.T) {
	s := &site{}
	get := func() int {
		rec := httptest.NewRecorder()
		s.serveHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		return rec.Code
	}
	if got := get(); got != http.StatusServiceUnavailable {
		t.Fatalf("no term: %d", got)
	}
	first, endFirst := context.WithCancel(context.Background())
	var seen context.Context
	s.set(first, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Context()
		w.WriteHeader(http.StatusTeapot)
	}))
	if got := get(); got != http.StatusTeapot {
		t.Fatalf("first term: %d", got)
	}
	second, endSecond := context.WithCancel(context.Background())
	defer endSecond()
	s.set(second, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	endFirst()
	if seen.Err() == nil {
		t.Error("a request outlived its term")
	}
	if got := get(); got != http.StatusAccepted {
		t.Fatalf("the first term's end cleared the second: %d", got)
	}
	endSecond()
	if got := get(); got != http.StatusServiceUnavailable {
		t.Fatalf("an ended term still serves: %d", got)
	}
}

// fakeEngines is a deployment's engines as a Postgres store reports them.
type fakeEngines struct {
	es  []postgres.EngineStatus
	err error
}

func (f fakeEngines) Engines(context.Context) ([]postgres.EngineStatus, error) { return f.es, f.err }

func TestPGDeployment(t *testing.T) {
	since := time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC)
	got, err := pgDeployment{fakeEngines{es: []postgres.EngineStatus{
		{Engine: postgres.Engine{ID: "a", Version: "v1", Address: "h:1"}, Active: true, Since: since, Epoch: 4},
		{Engine: postgres.Engine{ID: "b", Version: "v1"}},
	}}}.Engines(context.Background())
	if err != nil || len(got) != 2 || got[0].State != "active" || got[0].Epoch != 4 || got[0].Address != "h:1" ||
		!got[0].Since.Equal(since) || got[1].State != "standby" || got[1].Since != nil {
		t.Fatalf("engines %+v, %v", got, err)
	}
	if _, err := (pgDeployment{fakeEngines{err: errors.New("down")}}).Engines(context.Background()); err == nil {
		t.Fatal("an error was lost")
	}
}

func TestNewEngineID(t *testing.T) {
	a, b := newEngineID(), newEngineID()
	if a == b || !regexp.MustCompile(`^[A-Za-z0-9._-]{1,24}-[a-z2-7]{6}$`).MatchString(a) {
		t.Fatalf("engine ids %q, %q", a, b)
	}
}

// lostStore is a store whose write connection is lost when lose is closed.
type lostStore struct {
	store.Store
	lose chan struct{}
}

func (l lostStore) Lost() <-chan struct{} { return l.lose }
func (l lostStore) LostErr() error        { return fmt.Errorf("%w: test", store.ErrWriteLost) }

// testServer is a serve process on no listener: its site is set, never bound.
func testServer(t *testing.T) *server {
	t.Helper()
	env, err := pipeline.ParseEnvironment(nil)
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{Ctx: t.Context(), IO: IO{Out: io.Discard, Err: io.Discard}, Version: "v-test", DB: dsn(unusedDB()),
		Launchers: testLaunchers()}
	rt.Env = func(k string) (string, bool) { v, ok := map[string]string{"USER": "ops"}[k]; return v, ok }
	return &server{cmd: ServeCmd{Slots: 1, LeaseTTL: time.Second}, rt: rt, env: env,
		log: slog.New(slog.DiscardHandler), site: &site{}, id: "engine-1"}
}

// A term whose store loses its write connection steps down: it returns errSteppedDown, the loss wrapped, and its
// site answers unavailable once it ended.
func TestTermStepsDown(t *testing.T) {
	s := testServer(t)
	s.env.Node.Embedded = false // the node's abandon has its own test (internal/node)
	s.nodeToken = "node-token"  // the term serves remote nodes
	lose := make(chan struct{})
	close(lose)
	err := s.term(t.Context(), lostStore{Store: memoryStore(t), lose: lose}, leadership{identity: api.Identity{ID: s.id}})
	if !errors.Is(err, errSteppedDown) || !errors.Is(err, store.ErrWriteLost) {
		t.Fatalf("term = %v", err)
	}
	rec := httptest.NewRecorder()
	s.site.serveHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/nodes", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("the site after the term: %d", rec.Code)
	}
}

// readyStore is a lostStore that closes ready once the term waits on it: the engine is ready, its site set.
type readyStore struct {
	lostStore
	ready chan struct{}
	once  sync.Once
}

func (r *readyStore) Lost() <-chan struct{} {
	r.once.Do(func() { close(r.ready) })
	return r.lose
}

// The embedded node is named after the engine's id, not its process id: two engines in containers, both pid 1,
// would otherwise run two nodes named embedded-1.
func TestTermNamesEmbeddedNodeAfterEngine(t *testing.T) {
	s := testServer(t)
	st := &readyStore{lostStore: lostStore{Store: memoryStore(t), lose: make(chan struct{})}, ready: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- s.term(t.Context(), st, leadership{identity: api.Identity{ID: s.id}}) }()
	select {
	case <-st.ready:
	case err := <-done:
		t.Fatalf("term ended before it was ready: %v", err)
	}
	rec, req := httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/nodes", nil)
	req.Host = "localhost" // the API answers its own hosts only
	s.site.serveHTTP(rec, req)
	close(st.lose)
	if err := <-done; !errors.Is(err, errSteppedDown) {
		t.Fatalf("term = %v", err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"id":"embedded-engine-1"`) {
		t.Fatalf("nodes: %d %s", rec.Code, rec.Body)
	}
}

// The leadership loop: a first store that cannot be opened fails serve; a term that steps down stands by again and
// opens the store anew, retrying while it cannot; any other end of a term ends serve, and so does ctx.
func TestLeadLoop(t *testing.T) {
	s := testServer(t)
	s.env.Engine.LockCheck = pipeline.Duration(time.Millisecond)
	ctx := t.Context()
	s.open = func(context.Context) (store.Store, error) { return nil, errors.New("unreachable") }
	if err := s.lead(ctx); err == nil || err.Error() != "unreachable" {
		t.Fatalf("first open failing: %v", err)
	}
	var opens, terms int
	boom := errors.New("boom")
	s.open = func(context.Context) (store.Store, error) {
		opens++
		if opens == 2 {
			return nil, errors.New("database failing over")
		}
		return memoryStore(t), nil
	}
	s.run = func(context.Context, store.Store, leadership) error {
		terms++
		if terms < 2 {
			return fmt.Errorf("%w: lost", errSteppedDown)
		}
		return boom
	}
	if err := s.lead(ctx); !errors.Is(err, boom) || opens != 3 || terms != 2 {
		t.Fatalf("lead = %v after %d opens and %d terms", err, opens, terms)
	}
	done, cancel := context.WithCancel(ctx)
	cancel()
	s.open = func(context.Context) (store.Store, error) { return memoryStore(t), nil }
	if err := s.lead(done); err != nil {
		t.Fatalf("lead after ctx ended = %v", err)
	}
}

// Without a lock to wait for (SQLite here), acquire opens the store at once; the standby's API, set on the site
// for the standby's context, refuses actions and is gone once that context ends.
func TestAcquireAndStandby(t *testing.T) {
	s := testServer(t)
	s.rt.DB = dsn(sqlite.Memory)
	s.nodeToken = "node-token" // the standby refuses remote nodes, naming the active engine
	st, err := s.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s.standby(ctx, time.Now())
	rec := httptest.NewRecorder()
	s.site.serveHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/nodes", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("the site after the standby: %d", rec.Code)
	}
}

// serveOn runs one engine on a store the test hands in until ctx ends; on Postgres it leads, and fails when the
// first store cannot be opened (here, a DSN variable that is unset).
func TestServeOn(t *testing.T) {
	s := testServer(t)
	s.rt.Store = memoryStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.cmd.serveOn(ctx, s.rt, s.env, s.log, s.site, s.sec, s.arts, ""); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("serve on SQLite: %v", err)
	}
	s.rt.Store = nil
	env, err := pipeline.ParseEnvironment([]byte("store: { dsn: \"postgres://127.0.0.1:1/ms?connect_timeout=1\" }\n"))
	if err != nil {
		t.Fatal(err)
	}
	s.rt.DB = ""
	if err := s.cmd.serveOn(t.Context(), s.rt, env, s.log, s.site, s.sec, s.arts, ""); err == nil ||
		!strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("serve on an unreachable Postgres: %v", err)
	}
}

// Without test launchers, the embedded node runs exec actions, and container ones when the runtime is installed;
// the artifact store and package cache sit under artifacts.url's directory when it is set.
func TestHostDefaults(t *testing.T) {
	env, err := pipeline.ParseEnvironment([]byte("artifacts: { url: \"file:///srv/arts\" }\nruntime: { container: podman }\n"))
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{Env: func(string) (string, bool) { return "", false }}
	if l := launchers(rt, env); l[pipeline.KindExec] == nil {
		t.Fatalf("launchers %v", l)
	}
	if got := artifactsDir(env); got != "/srv/arts" {
		t.Fatalf("artifacts at %s", got)
	}
	if got := packageDir(env); got != "/srv/packages" {
		t.Fatalf("packages at %s", got)
	}
}

// withStoreMode reports an environment file it cannot read, and a --db that is no DSN, before it opens anything.
func TestWithStoreModeRefusals(t *testing.T) {
	rt := &Runtime{Ctx: t.Context(), EnvFile: filepath.Join(testdir.New(t), "missing.yaml"), DB: dsn(unusedDB()),
		Env: func(string) (string, bool) { return "", false }}
	if err := withStore(rt, func(store.Store) error { return nil }); err == nil {
		t.Fatal("a missing environment file was read")
	}
	dir := testdir.New(t)
	rt.EnvFile, rt.DB = filepath.Join(dir, "millstream.yaml"), "a.db"
	if err := os.WriteFile(rt.EnvFile, []byte("store: { dsn: sqlite://b.db }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := withStore(rt, func(store.Store) error { return nil }); err == nil ||
		err.Error() != "--db: want a DSN, such as sqlite://a.db" {
		t.Fatalf("a bare path: %v", err)
	}
}
