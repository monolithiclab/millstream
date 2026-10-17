package nodeapi_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/nodeapi"
	"github.com/monolithiclab/millstream/internal/nodewire"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/store/sqlite/sqlitetest"
)

// The node protocol end to end in memory: a remote.Transport whose HTTP client calls the engines' handlers
// directly (no listener), by host. The real listener, TLS and HTTP/2 are the e2e suite's (internal/e2e).

const pipelineYAML = `
pipeline: remote
triggers: { go: { manual: {} } }
steps:
  write:
    action: ./actions/fake
    in: trigger.go
    out: pages
  read:
    action: ./actions/fake
    in: pages
`

const manifest = `
name: fake
version: 1.0.0
kind: exec
runtime: { exec: { command: ["./run"] } }
`

const token = "node-s3cret"

// hosts routes a request to the handler of its host, as if each were an engine listening there; an unknown host
// cannot be reached.
type hosts struct {
	mu sync.Mutex
	h  map[string]http.Handler
}

func (hs *hosts) set(host string, h http.Handler) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if h == nil {
		delete(hs.h, host)
		return
	}
	hs.h[host] = h
}

func (hs *hosts) RoundTrip(req *http.Request) (*http.Response, error) {
	hs.mu.Lock()
	h := hs.h[req.URL.Host]
	hs.mu.Unlock()
	if h == nil {
		return nil, errors.New("connection refused")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result(), nil
}

// deployment is an engine over a store, serving the node protocol at a host.
type deployment struct {
	t     *testing.T
	ctx   context.Context
	st    *sqlite.Store
	arts  *memArtifacts
	e     *engine.Engine
	hosts *hosts
	pkg   *pipeline.Package
}

func newDeployment(t *testing.T) *deployment {
	t.Helper()
	return newDeploymentWith(t, sqlite.Options{})
}

// newDeploymentWith is newDeployment over a store opened with opts.
func newDeploymentWith(t *testing.T, opts sqlite.Options) *deployment {
	t.Helper()
	st, err := sqlite.Open(sqlite.Memory, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	d := &deployment{t: t, ctx: context.Background(), st: st, arts: newMemArtifacts(),
		hosts: &hosts{h: map[string]http.Handler{}}}
	d.e = d.engine("engine-a")
	files := map[string][]byte{"pipeline.yaml": []byte(pipelineYAML), "actions/fake/action.yaml": []byte(manifest),
		"actions/fake/run": []byte("#!/bin/sh\n")}
	if d.pkg, err = pipeline.Materialize(shared(t, "packages"), files); err != nil {
		t.Fatal(err)
	}
	if _, err := d.e.Apply(d.ctx, d.pkg); err != nil {
		t.Fatal(err)
	}
	d.serve("a:7070", d.e)
	return d
}

func (d *deployment) engine(id string) *engine.Engine {
	d.t.Helper()
	e, err := engine.New(engine.Options{Store: d.st, Artifacts: d.arts,
		Leadership: engine.Leadership{ID: id, Epoch: 1}})
	if err != nil {
		d.t.Fatal(err)
	}
	return e
}

func (d *deployment) serve(host string, e *engine.Engine) {
	d.t.Helper()
	h, err := nodeapi.New(nodeapi.Options{Engine: e, Token: token, Artifacts: d.arts, Version: "v1"})
	if err != nil {
		d.t.Fatal(err)
	}
	d.hosts.set(host, h)
}

func (d *deployment) transport(id string, join ...string) *nodeapi.Client {
	d.t.Helper()
	tr, err := nodeapi.NewClient(nodeapi.ClientOptions{Join: join, Token: token, HTTP: &http.Client{Transport: d.hosts},
		Node:     nodewire.Register{Node: id, Version: "v1", Runtimes: []string{"exec"}, Slots: 2},
		StateDir: shared(d.t, "state"), TmpDir: shared(d.t, "tmp"), PullWait: time.Millisecond})
	if err != nil {
		d.t.Fatal(err)
	}
	if _, err := tr.Register(d.ctx); err != nil {
		d.t.Fatal(err)
	}
	return tr
}

// The node token is compared on every request; a node speaking another protocol is refused.
func TestNodeTokenAndProtocol(t *testing.T) {
	d := newDeployment(t)
	h := d.hosts.h["a:7070"]
	for _, auth := range []string{"", "Bearer wrong", "Basic " + token} {
		req := httptest.NewRequest(http.MethodPost, nodewire.PathRegister, strings.NewReader(`{}`))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("authorization %q: %d", auth, rec.Code)
		}
	}
	for body, code := range map[string]string{
		`{"node":"n","protocol":2,"slots":1}`:           nodewire.CodeIncompatible,
		`{"node":"","protocol":1,"slots":1}`:            nodewire.CodeInvalid,
		`{"node":"embedded-1","protocol":1,"slots":1}`:  nodewire.CodeInvalid,
		`{"node":"n","protocol":1,"slots":1,"extra":1}`: nodewire.CodeInvalid,
	} {
		req := httptest.NewRequest(http.MethodPost, nodewire.PathRegister, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if !strings.Contains(rec.Body.String(), `"`+code+`"`) {
			t.Errorf("register %s: %d %s, want %s", body, rec.Code, rec.Body.String(), code)
		}
	}
	tr, err := nodeapi.NewClient(nodeapi.ClientOptions{Join: []string{"https://a:7070"}, Token: "wrong",
		HTTP: &http.Client{Transport: d.hosts}, Node: nodewire.Register{Node: "n", Slots: 1}, StateDir: shared(t, "state")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Register(d.ctx); !errors.Is(err, nodeapi.ErrUnauthorized) {
		t.Fatalf("register with a wrong token: %v", err)
	}
	if _, err := nodeapi.NewClient(nodeapi.ClientOptions{Join: []string{"http://10.0.0.1:7070"}, Token: token,
		HTTP: http.DefaultClient, Node: nodewire.Register{Node: "n"}, StateDir: shared(t, "state")}); err == nil {
		t.Fatal("plain HTTP to another host accepted")
	}
	// A standby without the token answers nothing about the active engine.
	standby := nodeapi.NewStandby(token, func(context.Context) (string, string) { return "e", "b:7070" })
	rec := httptest.NewRecorder()
	standby.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, nodewire.PathPull, nil))
	if rec.Code != http.StatusUnauthorized || rec.Header().Get(api.ActiveAddressHeader) != "" {
		t.Fatalf("standby without the token: %d %v", rec.Code, rec.Header())
	}
}

// request makes one authenticated request of h and returns its answer.
func request(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func wantCode(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status || !strings.Contains(rec.Body.String(), `"`+code+`"`) {
		t.Fatalf("answer %d %s, want %d %s", rec.Code, rec.Body.String(), status, code)
	}
}

// Leases are the capability: a package, or an artifact URL, only for a delivery the node holds; the proxy's URLs
// are signed and expire; relayed lines reach the relay; a node leaving ends its leases.
func TestLeaseScopedHandlers(t *testing.T) {
	d := newDeployment(t)
	var relayed strings.Builder
	now := time.Now()
	h, err := nodeapi.New(nodeapi.Options{Engine: d.e, Token: token, Artifacts: d.arts, Version: "v1",
		Relay: slog.New(slog.NewTextHandler(&relayed, nil)), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	d.hosts.set("a:7070", h)
	n1, _ := d.transport("n1", "https://a:7070"), d.transport("n2", "https://a:7070")
	if _, _, err := d.e.Fire(d.ctx, "remote", "go", "k1", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	as, err := n1.Pull(d.ctx, "n1", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("pull = %v, %v", as, err)
	}
	l := as[0].Lease
	q := func(nodeID, hash string) string {
		return nodewire.PathPackage + "?" + url.Values{"node": {nodeID}, "pipeline": {l.Pipeline},
			"delivery": {l.DeliveryID}, "step": {l.Step}, "token": {strconv.FormatInt(l.FencingToken, 10)},
			"hash": {hash}}.Encode()
	}
	if rec := request(h, http.MethodGet, q("n1", d.pkg.Hash), ""); rec.Code != http.StatusOK || rec.Body.Len() == 0 {
		t.Fatalf("package for the holder: %d", rec.Code)
	}
	wantCode(t, request(h, http.MethodGet, q("n2", d.pkg.Hash), ""), http.StatusForbidden, nodewire.CodeNotHeld)
	wantCode(t, request(h, http.MethodGet, q("n1", strings.Repeat("0", 64)), ""), http.StatusForbidden,
		nodewire.CodeNotHeld)
	wantCode(t, request(h, http.MethodGet, q("ghost", d.pkg.Hash), ""), http.StatusConflict, nodewire.CodeUnregistered)
	wantCode(t, request(h, http.MethodGet, nodewire.PathPackage+"?node=n1&token=x", ""), http.StatusBadRequest,
		nodewire.CodeInvalid)

	// An output's signed URL: tampered, expired or carrying other content, refused.
	lease := nodewire.FromRef(l.Ref())
	put := func(a store.ArtifactRef) nodewire.URL {
		body, _ := json.Marshal(nodewire.ArtifactURL{Node: "n1", Lease: lease, Op: nodewire.OpPut, Artifact: a})
		rec := request(h, http.MethodPost, nodewire.PathArtifactURL, string(body))
		var u nodewire.URL
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &u) != nil {
			t.Fatalf("artifact URL: %d %s", rec.Code, rec.Body.String())
		}
		return u
	}
	sum := sha256.Sum256([]byte("report"))
	ref := store.ArtifactRef{Name: "r.txt", SHA256: hex.EncodeToString(sum[:]), Size: 6, Type: "text/plain"}
	u := put(ref)
	if u.Direct || u.Exists || !strings.HasPrefix(u.URL, nodewire.PathBlob+"?") {
		t.Fatalf("proxy URL = %+v", u)
	}
	wantCode(t, request(h, http.MethodPut, strings.Replace(u.URL, "name=r.txt", "name=x.txt", 1), "report"),
		http.StatusForbidden, nodewire.CodeNotHeld)
	wantCode(t, request(h, http.MethodPut, u.URL, "other"), http.StatusBadRequest, nodewire.CodeInvalid)
	wantCode(t, request(h, http.MethodGet, u.URL, ""), http.StatusForbidden, nodewire.CodeNotHeld) // a put URL
	if rec := request(h, http.MethodPut, u.URL, "report"); rec.Code != http.StatusOK {
		t.Fatalf("put through the proxy: %d %s", rec.Code, rec.Body.String())
	}
	// Stored already, it is not sent again but touched, so the artifact sweep's grace counts from now.
	if again := put(ref); !again.Exists || d.arts.touched[l.Pipeline+"/"+ref.SHA256] != 1 {
		t.Fatalf("a stored content: %+v, touched %v", again, d.arts.touched)
	}
	d.arts.touchErr = errors.New("disk failure")
	body, _ := json.Marshal(nodewire.ArtifactURL{Node: "n1", Lease: lease, Op: nodewire.OpPut, Artifact: ref})
	wantCode(t, request(h, http.MethodPost, nodewire.PathArtifactURL, string(body)), http.StatusInternalServerError,
		nodewire.CodeInternal)
	d.arts.touchErr = nil
	for body, code := range map[string]string{
		`{"node":"n1","lease":{},"op":"delete","artifact":{}}`: nodewire.CodeInvalid,
		`{"node":"n1","lease":{},"op":"put","artifact":{}}`:    nodewire.CodeNotHeld,
	} {
		rec := request(h, http.MethodPost, nodewire.PathArtifactURL, body)
		if !strings.Contains(rec.Body.String(), code) {
			t.Errorf("artifact URL %s: %d %s", body, rec.Code, rec.Body.String())
		}
	}

	// The relay re-emits lines with the node's id; too many in one go are refused.
	if rec := request(h, http.MethodPost, nodewire.PathLogs, `{"node":"n1","lines":[{"time":"2026-10-16T06:00:00Z",`+
		`"level":"WARN","msg":"slow source","attrs":{"step":"write"}}]}`); rec.Code != http.StatusNoContent {
		t.Fatalf("logs: %d", rec.Code)
	}
	if got := relayed.String(); !strings.Contains(got, "slow source") || !strings.Contains(got, "nodeId=n1") ||
		!strings.Contains(got, "step=write") {
		t.Fatalf("relayed = %q", got)
	}
	lines := `{"node":"n1","lines":[` + strings.Repeat(`{},`, 1000) + `{}]}`
	wantCode(t, request(h, http.MethodPost, nodewire.PathLogs, lines), http.StatusBadRequest, nodewire.CodeInvalid)

	// Done through the handler, then the read step's input through the proxy; heartbeats renew, a stale report is
	// refused.
	done, _ := json.Marshal(nodewire.Done{Node: "n1", Lease: lease, Attempt: l.Attempt,
		Outputs: []nodewire.Output{{Key: "p", Payload: json.RawMessage(`{}`), Artifacts: []store.ArtifactRef{ref}}}})
	if rec := request(h, http.MethodPost, nodewire.PathDone, string(done)); rec.Code != http.StatusOK {
		t.Fatalf("done: %d %s", rec.Code, rec.Body.String())
	}
	as, err = n1.Pull(d.ctx, "n1", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("pull read = %v, %v", as, err)
	}
	read := nodewire.FromRef(as[0].Lease.Ref())
	hb, _ := json.Marshal(nodewire.Heartbeat{Node: "n1", Leases: []nodewire.LeaseRef{read, lease}})
	rec := request(h, http.MethodPost, nodewire.PathHeartbeat, string(hb))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"lost":[{"pipeline":"remote","deliveryId":"`+
		l.DeliveryID) {
		t.Fatalf("heartbeat: %d %s", rec.Code, rec.Body.String())
	}
	wantCode(t, request(h, http.MethodPost, nodewire.PathHeartbeat, `{"leases":[]}`), http.StatusBadRequest,
		nodewire.CodeInvalid)
	get, _ := json.Marshal(nodewire.ArtifactURL{Node: "n1", Lease: read, Op: nodewire.OpGet, Artifact: ref})
	rec = request(h, http.MethodPost, nodewire.PathArtifactURL, string(get))
	var gu nodewire.URL
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &gu) != nil {
		t.Fatalf("get URL: %d %s", rec.Code, rec.Body.String())
	}
	if rec := request(h, http.MethodGet, gu.URL, ""); rec.Code != http.StatusOK || rec.Body.String() != "report" {
		t.Fatalf("get through the proxy: %d %q", rec.Code, rec.Body.String())
	}
	stale := read
	stale.FencingToken++
	fail, _ := json.Marshal(nodewire.Fail{Node: "n1", Lease: stale, Attempt: 1, Class: "action", Message: "x"})
	wantCode(t, request(h, http.MethodPost, nodewire.PathFail, string(fail)), http.StatusConflict, nodewire.CodeStale)
	wantCode(t, request(h, http.MethodPost, nodewire.PathDone, `{"node":"n2","lease":`+mustJSON(read)+`}`),
		http.StatusConflict, nodewire.CodeStale)
	wantCode(t, request(h, http.MethodPost, nodewire.PathDone, `{`), http.StatusBadRequest, nodewire.CodeInvalid)

	// Leaving ends the lease the node holds; it is draining until it registers again.
	if err := n1.Leave(d.ctx); err != nil {
		t.Fatal(err)
	}
	ds, _ := d.st.Deliveries(d.ctx, store.DeliveryFilter{Pipeline: "remote", ID: read.DeliveryID})
	if len(ds) != 1 || ds[0].State != store.DeliveryPending || ds[0].ErrorClass != store.ClassLeaseExpired {
		t.Fatalf("delivery of a node that left = %+v", ds)
	}
	if as, err := n1.Pull(d.ctx, "n1", 1); err != nil || len(as) != 0 {
		t.Fatalf("a node that left pulled %d, %v", len(as), err)
	}

	now = now.Add(nodeapi.URLTTL + time.Second)
	wantCode(t, request(h, http.MethodPut, u.URL, "report"), http.StatusForbidden, nodewire.CodeNotHeld)

	// Without an artifact store, no artifact URL.
	bare, _ := nodeapi.New(nodeapi.Options{Engine: d.e, Token: token})
	wantCode(t, request(bare, http.MethodPost, nodewire.PathArtifactURL, `{"node":"n2","lease":{},"op":"get","artifact":{}}`),
		http.StatusServiceUnavailable, nodewire.CodeUnavailable)
	if _, err := nodeapi.New(nodeapi.Options{Engine: d.e}); err == nil {
		t.Fatal("a handler without a token")
	}
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// A failure is applied under the lease; an unknown node leaving must register; a request cut off, or a closed store,
// answers unavailable, and a node never takes an empty answer for an acceptance.
func TestFailLeaveAndStoreErrors(t *testing.T) {
	d := newDeployment(t)
	h := d.hosts.h["a:7070"]
	n1 := d.transport("n1", "https://a:7070")
	if _, _, err := d.e.Fire(d.ctx, "remote", "go", "k1", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	as, err := n1.Pull(d.ctx, "n1", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("pull = %v, %v", as, err)
	}
	fail, _ := json.Marshal(nodewire.Fail{Node: "n1", Lease: nodewire.FromRef(as[0].Lease.Ref()), Attempt: 1,
		Class: "action", Message: "down", Retryable: true})
	if rec := request(h, http.MethodPost, nodewire.PathFail, string(fail)); rec.Code != http.StatusOK {
		t.Fatalf("fail: %d %s", rec.Code, rec.Body.String())
	}
	wantCode(t, request(h, http.MethodPost, nodewire.PathFail, `{`), http.StatusBadRequest, nodewire.CodeInvalid)
	wantCode(t, request(h, http.MethodPost, nodewire.PathLeave, `{"node":"ghost"}`), http.StatusConflict,
		nodewire.CodeUnregistered)
	wantCode(t, request(h, http.MethodPost, nodewire.PathLeave, `{`), http.StatusBadRequest, nodewire.CodeInvalid)
	wantCode(t, request(h, http.MethodPost, nodewire.PathPull, `{"node":"n1","max":1,"waitMs":0,"x":1}`),
		http.StatusBadRequest, nodewire.CodeInvalid)
	wantCode(t, request(h, http.MethodPost, nodewire.PathLogs, `{"node":"ghost","lines":[]}`), http.StatusConflict,
		nodewire.CodeUnregistered)

	// A request cut off (its engine's term ended, its node went away) is never answered as a success, which a node
	// would take for an acceptance: 503, retried.
	if _, _, err := d.e.Fire(d.ctx, "remote", "go", "k2", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	as, err = n1.Pull(d.ctx, "n1", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("pull = %v, %v", as, err)
	}
	lease := mustJSON(nodewire.FromRef(as[0].Lease.Ref()))
	cut, cancel := context.WithCancel(d.ctx)
	cancel()
	for path, body := range map[string]string{
		nodewire.PathDone:      `{"node":"n1","attempt":1,"lease":` + lease + `}`,
		nodewire.PathFail:      `{"node":"n1","attempt":1,"class":"action","lease":` + lease + `}`,
		nodewire.PathHeartbeat: `{"node":"n1","leases":[` + lease + `]}`,
		nodewire.PathLeave:     `{"node":"n1"}`,
	} {
		req := httptest.NewRequestWithContext(cut, http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
			t.Errorf("%s cut off: %d %v %s", path, rec.Code, rec.Header(), rec.Body.String())
		}
	}
	// And a node takes an empty 2xx answer for no acceptance: it sends the report again.
	var empties int
	both := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == nodewire.PathDone && empties == 0 {
			empties++
			return // an implicit, empty 200
		}
		h.ServeHTTP(w, r)
	})
	d.hosts.set("a:7070", both)
	d.hosts.set("b:7070", both)
	n2 := d.transport("n1", "https://a:7070", "https://b:7070")
	if err := n2.Done(d.ctx, store.DoneRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 1}); err != nil || empties != 1 {
		t.Fatalf("done after an empty answer: %v, %d empty", err, empties)
	}
	ds, err := d.st.Deliveries(d.ctx, store.DeliveryFilter{Pipeline: "remote", ID: as[0].Lease.DeliveryID})
	if err != nil || len(ds) != 1 || ds[0].State != store.DeliveryDone {
		t.Fatalf("delivery after the resent done: %+v, %v", ds, err)
	}

	_ = d.st.Close()
	wantCode(t, request(h, http.MethodPost, nodewire.PathHeartbeat, `{"node":"n1","leases":[]}`),
		http.StatusServiceUnavailable, nodewire.CodeUnavailable)
}

// presigning is an artifact store nodes reach through presigned URLs, as the S3 store.
type presigning struct{ *memArtifacts }

func (presigning) PresignGet(p, sha string, _ time.Duration) (string, error) {
	return "https://s3.test/get/" + p + "/" + sha, nil
}

func (presigning) PresignPut(p, sha string, size int64, _ time.Duration) (string, error) {
	return "https://s3.test/put/" + p + "/" + sha + "?size=" + strconv.FormatInt(size, 10), nil
}

// With a presigning store, a node gets presigned URLs, direct, for an input it holds and an output.
func TestPresignedURLs(t *testing.T) {
	d := newDeployment(t)
	h, err := nodeapi.New(nodeapi.Options{Engine: d.e, Token: token, Artifacts: presigning{d.arts}})
	if err != nil {
		t.Fatal(err)
	}
	d.hosts.set("a:7070", h)
	n1 := d.transport("n1", "https://a:7070")
	if _, _, err := d.e.Fire(d.ctx, "remote", "go", "k1", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	as, err := n1.Pull(d.ctx, "n1", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("pull = %v, %v", as, err)
	}
	sha := strings.Repeat("ab", 32)
	body := mustJSON(nodewire.ArtifactURL{Node: "n1", Lease: nodewire.FromRef(as[0].Lease.Ref()), Op: nodewire.OpPut,
		Artifact: store.ArtifactRef{Name: "o.txt", SHA256: sha, Size: 1, Type: "text/plain"}})
	rec := request(h, http.MethodPost, nodewire.PathArtifactURL, body)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"url":"https://s3.test/put/remote/`+sha+`?size=1","direct":true`) {
		t.Fatalf("presigned put: %d %s", rec.Code, rec.Body.String())
	}
	bad := strings.Replace(body, `"size":1`, `"size":-1`, 1)
	wantCode(t, request(h, http.MethodPost, nodewire.PathArtifactURL, bad), http.StatusBadRequest, nodewire.CodeInvalid)

	// The node's direct PUT sends the length and the checksum the presigned URL signs, without the node token.
	var got *http.Request
	d.hosts.set("s3.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got = r }))
	ref, err := n1.PutArtifact(d.ctx, as[0].Lease.Ref(), "margin.txt", strings.NewReader("margin 18%"))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.URL.Query().Get("size") != "10" || got.ContentLength != 10 ||
		got.Header.Get(artifacts.ChecksumHeader) != artifacts.ChecksumSHA256(ref.SHA256) ||
		got.Header.Get("Authorization") != "" {
		t.Fatalf("direct PUT = %+v", got)
	}
}

// The tests write into one directory per process, removed when the tests end: a temporary directory per test costs
// more to remove than the test takes. Packages and node caches are content-addressed and shared; scratch gives a
// test a directory of its own in it, for an artifact store that starts empty.
var testRoot string

func TestMain(m *testing.M) {
	var err error
	if testRoot, err = os.MkdirTemp("", "millstream-node-test-"); err != nil {
		panic(err)
	}
	sqlitetest.Warm()
	code := m.Run()
	_ = os.RemoveAll(testRoot)
	os.Exit(code)
}

func shared(t *testing.T, name string) string {
	t.Helper()
	dir := filepath.Join(testRoot, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func scratch(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(testRoot, "scratch-")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// memArtifacts is an artifact store in memory: the tests move artifacts without touching a disk.
type memArtifacts struct {
	mu       sync.Mutex
	blobs    map[string][]byte // by pipeline/sha256
	touched  map[string]int    // Touch calls by pipeline/sha256
	touchErr error             // what Touch fails with when set
}

func newMemArtifacts() *memArtifacts {
	return &memArtifacts{blobs: map[string][]byte{}, touched: map[string]int{}}
}

func (m *memArtifacts) Touch(_ context.Context, p, sha string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.touchErr != nil {
		return m.touchErr
	}
	if _, ok := m.blobs[p+"/"+sha]; !ok {
		return fs.ErrNotExist
	}
	m.touched[p+"/"+sha]++
	return nil
}

func (m *memArtifacts) Put(_ context.Context, p, name string, r io.Reader) (store.ArtifactRef, error) {
	if !store.ValidArtifactName(name) {
		return store.ArtifactRef{}, errors.New("bad name")
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return store.ArtifactRef{}, err
	}
	sum := sha256.Sum256(b)
	ref := store.ArtifactRef{Name: name, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(b)),
		Type: http.DetectContentType(b)}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[p+"/"+ref.SHA256] = b
	return ref, nil
}

func (m *memArtifacts) Open(_ context.Context, p, sha string) (io.ReadSeekCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobs[p+"/"+sha]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return nopCloser{bytes.NewReader(b)}, nil
}

func (m *memArtifacts) Exists(_ context.Context, p, sha string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.blobs[p+"/"+sha]
	return ok, nil
}

func (m *memArtifacts) Copy(_ context.Context, from, to, sha string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobs[from+"/"+sha]
	if !ok {
		return fs.ErrNotExist
	}
	m.blobs[to+"/"+sha] = b
	return nil
}

type nopCloser struct{ *bytes.Reader }

func (nopCloser) Close() error { return nil }
