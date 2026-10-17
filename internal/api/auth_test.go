package api

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// authServer is a server over the shared fixture's store requiring the token "s3cret", reached as
// millstream.example behind a proxy.
func authServer(t *testing.T, public bool) (*Server, *fakeClock) {
	t.Helper()
	f := readFixture(t)
	clock := &fakeClock{t: t0}
	srv := must(New(Options{Store: f.st, Clock: clock, User: "tester", Auth: &Auth{Token: "s3cret",
		Hosts: []string{"millstream.example"}, Origins: []string{"https://millstream.example"}, SecureCookie: true,
		SessionTTL: time.Hour, MetricsPublic: public}}))
	return srv, clock
}

// With a token, every request carries it, as a bearer token or the session cookie signing in sets; signing out
// ends the session; a session expires.
func TestTokenAuth(t *testing.T) {
	srv, clock := authServer(t, false)
	bearer := func(token string) map[string]string { return map[string]string{"Authorization": "Bearer " + token} }

	rec := do(t, srv, request{target: "/api/v1/session"})
	wantError(t, rec, http.StatusUnauthorized, codeUnauthorized)
	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Error("401 without WWW-Authenticate")
	}
	wantError(t, do(t, srv, request{target: "/api/v1/session", header: bearer("wrong")}), http.StatusUnauthorized,
		codeUnauthorized)
	if rec := do(t, srv, request{target: "/api/v1/session", header: bearer("s3cret")}); rec.Code != http.StatusOK {
		t.Fatalf("bearer: %d %s", rec.Code, rec.Body)
	}
	// The reviewer is auth.user, whatever the request says.
	rec = do(t, srv, request{target: "/api/v1/session", header: map[string]string{"Authorization": "bearer s3cret",
		UserHeader: "mallory"}})
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `{"auth":true,"user":"operator"}`) {
		t.Fatalf("session: %d %s", rec.Code, rec.Body)
	}

	wantError(t, do(t, srv, request{method: http.MethodPost, target: "/api/v1/session", body: `{"token":"nope"}`}),
		http.StatusUnauthorized, codeUnauthorized)
	rec = do(t, srv, request{method: http.MethodPost, target: "/api/v1/session", body: `{"token":"s3cret"}`})
	cookies := rec.Result().Cookies()
	if rec.Code != http.StatusOK || len(cookies) != 1 {
		t.Fatalf("sign in: %d %s %v", rec.Code, rec.Body, cookies)
	}
	c := cookies[0]
	if c.Name != SessionCookie || len(c.Value) < 40 || !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteStrictMode ||
		c.Path != "/" || c.MaxAge != 3600 {
		t.Fatalf("session cookie %+v", c)
	}
	session := map[string]string{"Cookie": SessionCookie + "=" + c.Value}
	if rec := do(t, srv, request{target: "/api/v1/session", header: session}); rec.Code != http.StatusOK {
		t.Fatalf("cookie: %d %s", rec.Code, rec.Body)
	}
	// A wrong bearer token does not fall back to the cookie.
	both := map[string]string{"Cookie": session["Cookie"], "Authorization": "Bearer wrong"}
	wantError(t, do(t, srv, request{target: "/api/v1/session", header: both}), http.StatusUnauthorized,
		codeUnauthorized)

	// A state-changing request on the cookie names its origin: the server's own or ui.baseUrl's.
	pause := func(header map[string]string) *httptest.ResponseRecorder {
		return do(t, srv, request{method: http.MethodPost, target: "/api/v1/pipelines/nope/pause", header: header})
	}
	with := func(origin string) map[string]string {
		return map[string]string{"Cookie": session["Cookie"], "Origin": origin}
	}
	wantError(t, pause(session), http.StatusForbidden, codeForbidden)
	wantError(t, pause(with("https://evil.example")), http.StatusForbidden, codeForbidden)
	wantError(t, pause(with("null")), http.StatusForbidden, codeForbidden)
	for _, origin := range []string{"http://example.com", "https://millstream.example"} {
		wantError(t, pause(with(origin)), http.StatusNotFound, codeNotFound) // past the checks
	}
	wantError(t, pause(bearer("s3cret")), http.StatusNotFound, codeNotFound) // the CLI names no origin

	// Signing out ends the session and clears the cookie; a session expires.
	rec = do(t, srv, request{method: http.MethodDelete, target: "/api/v1/session", header: with("http://example.com")})
	if out := rec.Result().Cookies(); rec.Code != http.StatusNoContent || len(out) != 1 || out[0].MaxAge >= 0 {
		t.Fatalf("sign out: %d %v", rec.Code, out)
	}
	wantError(t, do(t, srv, request{target: "/api/v1/session", header: session}), http.StatusUnauthorized,
		codeUnauthorized)
	rec = do(t, srv, request{method: http.MethodPost, target: "/api/v1/session", body: `{"token":"s3cret"}`})
	session = map[string]string{"Cookie": SessionCookie + "=" + rec.Result().Cookies()[0].Value}
	clock.Advance(time.Hour)
	wantError(t, do(t, srv, request{target: "/api/v1/session", header: session}), http.StatusUnauthorized,
		codeUnauthorized)

	// /metrics takes the token unless it is public.
	wantError(t, do(t, srv, request{target: MetricsPath}), http.StatusUnauthorized, codeUnauthorized)
	public, _ := authServer(t, true)
	rec = httptest.NewRecorder()
	public.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, MetricsPath, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("public metrics: %d", rec.Code)
	}

	// The server answers ui.baseUrl's host beside loopback ones.
	ui := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ui")) })
	h := Handler(srv, ui)
	for host, want := range map[string]int{"millstream.example": 200, "MILLSTREAM.example:443": 200,
		"127.0.0.1:7070": 200, "evil.example": 403} {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/inbox", nil)
		r.Host = host
		h.ServeHTTP(rec, r)
		if rec.Code != want {
			t.Errorf("host %s: %d, want %d", host, rec.Code, want)
		}
	}
}

// Without a token, there is no session to start, a request decides as it says, and a state-changing request from
// another origin is still forbidden.
func TestNoTokenSession(t *testing.T) {
	f := readFixture(t)
	if got := get[Session](t, f.srv, "/api/v1/session"); got.Auth || got.User != "tester" {
		t.Fatalf("session %+v", got)
	}
	rec := do(t, f.srv, request{method: http.MethodPost, target: "/api/v1/session", body: `{"token":"x"}`})
	wantError(t, rec, http.StatusServiceUnavailable, codeUnavailable)
	if rec.Header().Get("Retry-After") == "" {
		t.Error("a 503 without Retry-After")
	}
	if rec := do(t, f.srv, request{method: http.MethodDelete, target: "/api/v1/session"}); rec.Code != http.StatusNoContent {
		t.Fatalf("sign out: %d", rec.Code)
	}
	wantError(t, do(t, f.srv, request{method: http.MethodPost, target: "/api/v1/pipelines/nope/pause",
		header: map[string]string{"Origin": "http://localhost:3000"}}), http.StatusForbidden, codeForbidden)
	if _, err := New(Options{Store: f.st, Auth: &Auth{}}); err == nil {
		t.Fatal("an empty token: no error")
	}
}

// Sessions are bounded: beyond maxSessions, expired ones go first, then the oldest.
func TestSessionsBounded(t *testing.T) {
	ss := NewSessions()
	first := ss.create(t0, t0.Add(time.Minute))
	for i := range maxSessions - 1 {
		ss.create(t0, t0.Add(time.Hour+time.Duration(i)))
	}
	ss.create(t0, t0.Add(2*time.Hour))
	if ss.valid(first, t0) || len(ss.byID) != maxSessions {
		t.Fatalf("after one more: first valid %v, %d sessions", ss.valid(first, t0), len(ss.byID))
	}
	ss.create(t0.Add(90*time.Minute), t0.Add(3*time.Hour)) // every session of the first hour has expired
	if len(ss.byID) != 2 {
		t.Fatalf("after expiry: %d sessions", len(ss.byID))
	}
}

// Ten failed sign-ins from one address within a minute refuse it, the right token included, until the minute is up;
// successful ones do not count, and another address is heard. The table is bounded, an IPv6 address counted by its
// /64.
func TestFailedSignInsLimited(t *testing.T) {
	srv, clock := authServer(t, false)
	signIn := func(token string) *httptest.ResponseRecorder {
		return do(t, srv, request{method: http.MethodPost, target: "/api/v1/session", body: `{"token":"` + token + `"}`})
	}
	for range maxFailures {
		if rec := signIn("s3cret"); rec.Code != http.StatusOK {
			t.Fatalf("sign in: %d", rec.Code)
		}
	}
	for range maxFailures {
		wantError(t, signIn("wrong"), http.StatusUnauthorized, codeUnauthorized)
	}
	clock.Advance(15 * time.Second)
	rec := signIn("s3cret")
	wantError(t, rec, http.StatusTooManyRequests, codeTooManyRequests)
	if got := rec.Header().Get("Retry-After"); got != "45" {
		t.Errorf("Retry-After = %q", got)
	}
	r := httptest.NewRequest(http.MethodPost, "/api/v1/session", strings.NewReader(`{"token":"s3cret"}`))
	r.Header.Set("Content-Type", "application/json")
	r.RemoteAddr = "198.51.100.7:4242"
	other := httptest.NewRecorder()
	srv.ServeHTTP(other, r)
	if other.Code != http.StatusOK {
		t.Errorf("another address: %d", other.Code)
	}
	clock.Advance(45 * time.Second)
	if rec := signIn("s3cret"); rec.Code != http.StatusOK {
		t.Fatalf("after the window: %d", rec.Code)
	}

	for addr, want := range map[string]string{"192.0.2.1:80": "192.0.2.1", "[2001:db8:0:1:aa::1]:443": "2001:db8:0:1::/64",
		"pipe": "pipe"} {
		if got := remoteAddr(&http.Request{RemoteAddr: addr}); got != want {
			t.Errorf("remoteAddr(%s) = %s, want %s", addr, got, want)
		}
	}
	f := newFailures()
	for i := range maxFailureAddrs {
		f.fail(strconv.Itoa(i), t0.Add(time.Duration(i)))
	}
	f.fail("late", t0.Add(time.Second))
	if _, ok := f.byAddr["0"]; ok || len(f.byAddr) != maxFailureAddrs {
		t.Fatalf("over the bound: oldest kept %v, %d addresses", ok, len(f.byAddr))
	}
	f.fail("later", t0.Add(time.Minute+time.Millisecond)) // every window but late's has ended
	if len(f.byAddr) != 2 {
		t.Fatalf("after expiry: %d addresses", len(f.byAddr))
	}
}

// A standby's read-only API enforces the token too, and signs the UI in although it refuses actions; a session
// outlives the term it started in when the process's servers share their sessions (serve's terms).
func TestStandbyAuth(t *testing.T) {
	f := readFixture(t)
	auth := &Auth{Token: "s3cret", Sessions: NewSessions()}
	standby := must(New(Options{Store: f.st, User: "tester", Auth: auth, Standby: true}))
	wantError(t, do(t, standby, request{target: "/api/v1/pipelines"}), http.StatusUnauthorized, codeUnauthorized)
	rec := do(t, standby, request{method: http.MethodPost, target: "/api/v1/session", body: `{"token":"s3cret"}`})
	cookies := rec.Result().Cookies()
	if rec.Code != http.StatusOK || len(cookies) != 1 {
		t.Fatalf("sign in on the standby: %d %s", rec.Code, rec.Body)
	}
	session := map[string]string{"Cookie": SessionCookie + "=" + cookies[0].Value}
	if rec := do(t, standby, request{target: "/api/v1/pipelines", header: session}); rec.Code != http.StatusOK {
		t.Fatalf("a read on the standby with the session: %d %s", rec.Code, rec.Body)
	}
	wantError(t, do(t, standby, request{method: http.MethodPost, target: "/api/v1/pipelines/listings/pause",
		header: map[string]string{"Authorization": "Bearer s3cret"}}), http.StatusServiceUnavailable, codeUnavailable)

	active := must(New(Options{Store: f.st, User: "tester", Auth: auth}))
	if rec := do(t, active, request{target: "/api/v1/session", header: session}); rec.Code != http.StatusOK {
		t.Fatalf("the session in the next term: %d %s", rec.Code, rec.Body)
	}
	other := must(New(Options{Store: f.st, User: "tester", Auth: &Auth{Token: "s3cret"}}))
	wantError(t, do(t, other, request{target: "/api/v1/session", header: session}), http.StatusUnauthorized,
		codeUnauthorized)
}

// Remote nodes' prefix bypasses the browser guards and auth.token: the node API, which checks the node token itself,
// answers a node naming any host; without one the prefix is not found, never the UI.
func TestNodePrefixSeam(t *testing.T) {
	f := readFixture(t)
	nodes := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	ui := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ui")) })
	with := must(New(Options{Store: f.st, Auth: &Auth{Token: "s3cret"}, NodeAPI: nodes}))
	without := must(New(Options{Store: f.st, Auth: &Auth{Token: "s3cret"}}))
	for _, tt := range []struct {
		srv  *Server
		path string
		want int
	}{
		{with, NodePrefix + "/pull", http.StatusTeapot},
		{with, NodePrefix, http.StatusTeapot},
		{with, "/node/v10", http.StatusForbidden}, // not the prefix: the Host guard applies
		{without, NodePrefix + "/pull", http.StatusNotFound},
	} {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, tt.path, nil)
		r.Host = "engine.internal:7443"
		Handler(tt.srv, ui).ServeHTTP(rec, r)
		if rec.Code != tt.want {
			t.Errorf("%s: %d, want %d", tt.path, rec.Code, tt.want)
		}
	}
}
