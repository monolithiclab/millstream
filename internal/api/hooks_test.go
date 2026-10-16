package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// hookSrc is a pipeline of builtins with two webhook triggers, slow taking one request a minute, and a manual one.
const hookSrc = `
pipeline: hooked
triggers:
  hook: { webhook: {}, inputSchema: { type: object, required: [city], properties: { city: { type: string } } } }
  slow: { webhook: { rate: 1/min } }
  adhoc: { manual: {} }
steps: { pass: { action: filter, in: [trigger.hook, trigger.slow, trigger.adhoc], config: { expr: "true" } } }
`

// Webhook triggers through the API on a store of their own: the tokens an apply shows once, the hooks endpoint's
// authentication, validation, idempotency, rate, size limit and lifecycle rules, rotation, and a standby's refusal.
func TestWebhooks(t *testing.T) {
	clock := &fakeClock{t: t0}
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng, err := engine.New(engine.Options{Store: st, Now: clock.Now, PackageDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	h, err := New(Options{Store: st, Engine: eng, User: "tester", Clock: clock, MaxBody: 256,
		Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}

	// The apply answers each webhook trigger's token once.
	var applied ApplyResponse
	rec := do(t, h, request{method: http.MethodPost, target: "/api/v1/revisions", body: applyBody("hooks", hookSrc)})
	if err := json.Unmarshal(rec.Body.Bytes(), &applied); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("apply: %d %s", rec.Code, rec.Body.String())
	}
	tokens := map[string]TriggerToken{}
	for _, tok := range applied.Revisions[0].Tokens {
		tokens[tok.Trigger] = tok
	}
	hook, slow := tokens["hook"], tokens["slow"]
	if len(tokens) != 2 || hook.Path != "/hooks/hooked/hook" || !strings.HasPrefix(hook.Token, "mswh_") ||
		hook.Token == slow.Token {
		t.Fatalf("tokens = %+v", applied.Revisions[0].Tokens)
	}
	// The graph names each webhook trigger's path and rate; applying again keeps the tokens (the engine's tests).
	p := get[Pipeline](t, h, "/api/v1/pipelines/hooked")
	if tr := p.Triggers[1]; tr.Name != "hook" || tr.Path != hook.Path || tr.Rate != "60/min" {
		t.Fatalf("triggers = %+v", p.Triggers)
	}

	hooks := Handler(h, http.NotFoundHandler()) // where webhooks are served: before the API's guards
	fire := func(target, token, body string, header map[string]string) *httptest.ResponseRecorder {
		t.Helper()
		if header == nil {
			header = map[string]string{}
		}
		if token != "" {
			header["Authorization"] = "Bearer " + token
		}
		return do(t, hooks, request{method: http.MethodPost, target: target, body: body, header: header})
	}
	wantRun := func(rec *httptest.ResponseRecorder, status int, created bool) WebhookRun {
		t.Helper()
		var run WebhookRun
		if err := json.Unmarshal(rec.Body.Bytes(), &run); err != nil || rec.Code != status || run.Created != created ||
			run.Pipeline != "hooked" {
			t.Fatalf("answer %d %s; want %d, created %v", rec.Code, rec.Body.String(), status, created)
		}
		return run
	}

	// Fired by the bearer token, keyed by the Idempotency-Key: sent again, the request fires nothing new.
	key := map[string]string{IdempotencyHeader: "delivery-42"}
	run := wantRun(fire(hook.Path, hook.Token, `{"city":"Lyon"}`, key), http.StatusAccepted, true)
	if run.RunID != "run_delivery-42_hook" {
		t.Fatalf("run %s", run.RunID)
	}
	if r, err := st.Run(t.Context(), "hooked", run.RunID); err != nil || r.TriggerSource != store.SourceWebhook {
		t.Fatalf("run %+v, %v", r, err)
	}
	wantRun(fire(hook.Path, hook.Token, `{"city":"Lyon"}`, key), http.StatusOK, false)
	// By the token query parameter, for callers that cannot set a header.
	wantRun(fire(hook.Path+"?token="+hook.Token, "", `{"city":"Paris"}`, nil), http.StatusAccepted, true)

	for _, c := range []struct {
		name, target, token, body string
		header                    map[string]string
		status                    int
		code                      string
	}{
		{"no token", hook.Path, "", `{"city":"Lyon"}`, nil, 401, codeUnauthorized},
		{"another trigger's token", hook.Path, slow.Token, `{"city":"Lyon"}`, nil, 401, codeUnauthorized},
		{"basic credentials", hook.Path, "", `{}`, map[string]string{"Authorization": "Basic eDp5"}, 401,
			codeUnauthorized},
		{"no such pipeline", "/hooks/nope/hook", hook.Token, `{}`, nil, 401, codeUnauthorized},
		{"outside the schema", hook.Path, hook.Token, `{"city":7}`, nil, 400, codeInvalid},
		{"not JSON", hook.Path, hook.Token, `{"city"`, nil, 400, codeInvalid},
		{"bad request id", hook.Path, hook.Token, `{"city":"Lyon"}`, map[string]string{IdempotencyHeader: "a b"},
			400, codeInvalid},
		{"too large", hook.Path, hook.Token, `{"city":"` + strings.Repeat("x", 300) + `"}`, nil, 413, codeTooLarge},
		{"form content", hook.Path, hook.Token, `city=Lyon`,
			map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, 415, codeUnsupportedMediaType},
	} {
		rec := fire(c.target, c.token, c.body, c.header)
		if c.status == 401 && rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s: no WWW-Authenticate", c.name)
		}
		wantError(t, rec, c.status, c.code)
	}
	wantError(t, do(t, hooks, request{target: hook.Path}), 405, codeMethodNotAllowed)
	wantError(t, do(t, hooks, request{method: http.MethodPost, target: "/hooks/hooked/", body: "{}"}), 404,
		codeNotFound)
	// A token stored for a trigger the active revision does not have as a webhook trigger admits nothing.
	if _, err := st.CreateTriggerTokens(t.Context(), "hooked",
		map[string]string{"adhoc": engine.TriggerTokenHash("x")}); err != nil {
		t.Fatal(err)
	}
	wantError(t, fire("/hooks/hooked/adhoc", "x", `{}`, nil), 404, codeNotFound)

	// The trigger's rate: one request a minute, refused with Retry-After until the minute is up.
	wantRun(fire(slow.Path, slow.Token, `{}`, nil), http.StatusAccepted, true)
	rec = fire(slow.Path, slow.Token, `{}`, nil)
	wantError(t, rec, 429, codeTooManyRequests)
	if got := rec.Header().Get("Retry-After"); got != "60" {
		t.Errorf("Retry-After = %q", got)
	}
	clock.Advance(time.Minute)
	wantRun(fire(slow.Path, slow.Token, `{}`, nil), http.StatusAccepted, true)

	// A paused pipeline's triggers fire, its deliveries wait; a disabled one's do not.
	for _, state := range []struct {
		action string
		status int
	}{{"pause", http.StatusAccepted}, {"disable", http.StatusConflict}} {
		do(t, h, request{method: http.MethodPost, target: "/api/v1/pipelines/hooked/" + state.action})
		if rec := fire(hook.Path, hook.Token, `{"city":"Nice"}`, nil); rec.Code != state.status {
			t.Errorf("%s: %d %s", state.action, rec.Code, rec.Body.String())
		}
		do(t, h, request{method: http.MethodPost, target: "/api/v1/pipelines/hooked/resume"})
	}

	// Rotation answers the new token once; the old one stops working.
	var rotated TriggerToken
	rec = do(t, h, request{method: http.MethodPost, target: "/api/v1/pipelines/hooked/triggers/hook/token"})
	if err := json.Unmarshal(rec.Body.Bytes(), &rotated); err != nil || rec.Code != http.StatusOK ||
		rotated.Token == hook.Token || rotated.Path != hook.Path {
		t.Fatalf("rotate: %d %s", rec.Code, rec.Body.String())
	}
	wantError(t, fire(hook.Path, hook.Token, `{"city":"Lyon"}`, nil), 401, codeUnauthorized)
	wantRun(fire(hook.Path, rotated.Token, `{"city":"Lyon"}`, nil), http.StatusAccepted, true)
	wantError(t, do(t, h, request{method: http.MethodPost, target: "/api/v1/pipelines/hooked/triggers/adhoc/token"}),
		400, codeInvalid)
	wantError(t, do(t, h, request{method: http.MethodPost, target: "/api/v1/pipelines/hooked/triggers/nope/token"}),
		404, codeNotFound)

	// Served before the Host and Origin guards: the caller is a service elsewhere.
	r := httptest.NewRequest(http.MethodPost, rotated.Path, strings.NewReader(`{"city":"Lyon"}`))
	r.Host = "hooks.example"
	r.Header.Set("Origin", "https://elsewhere.example")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+rotated.Token)
	rr := httptest.NewRecorder()
	Handler(h, http.NotFoundHandler()).ServeHTTP(rr, r)
	if rr.Code != http.StatusAccepted || rr.Header().Get("Content-Security-Policy") != apiPolicy {
		t.Fatalf("through the handler: %d %s", rr.Code, rr.Body.String())
	}

	// A standby answers unavailable, naming the active engine, once the request is authenticated.
	standby, err := New(Options{Store: st, Engine: eng, User: "tester", Clock: clock, Standby: true,
		Deployment: fakeDeployment{engines: []DeploymentEngine{{ID: "a", State: stateActive,
			Address: "https://a.example"}}}})
	if err != nil {
		t.Fatal(err)
	}
	rec = do(t, Handler(standby, http.NotFoundHandler()), request{method: http.MethodPost, target: rotated.Path, body: `{"city":"Lyon"}`,
		header: map[string]string{"Authorization": "Bearer " + rotated.Token}})
	wantError(t, rec, 503, codeUnavailable)
	if rec.Header().Get(ActiveAddressHeader) != "https://a.example" {
		t.Errorf("standby headers %v", rec.Header())
	}
	if strings.Contains(logs.String(), "mswh_") {
		t.Errorf("a token was logged:\n%s", logs.String())
	}
}
