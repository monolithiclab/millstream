//go:build e2e

package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/store/postgres/pgtest"
)

// A webhook trigger served by serve on Postgres with auth.token and tls: apply --server prints the trigger's token
// once; a request over HTTPS with the token fires a run that settles, sent again under its Idempotency-Key it fires
// nothing new, and neither the deployment's token nor no token is accepted in its place; trigger token --rotate
// replaces it, and the old one stops working.
func Test_webhookOnPostgres(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	dir := t.TempDir()
	pool := selfSigned(t, dir)
	envFile := filepath.Join(dir, "millstream.yaml")
	if err := os.WriteFile(envFile, []byte("store: { dsn: \""+dsn+"\" }\n"+
		"listen: 127.0.0.1:0\nauth: { token: { env: MILLSTREAM_TOKEN } }\ntls: { cert: cert.pem, key: key.pem }\n"),
		0o600); err != nil {
		t.Fatal(err)
	}
	environ := map[string]string{"MILLSTREAM_TOKEN": testToken}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	serving := regexp.MustCompile(`serving the UI and the API on https://(127\.0\.0\.1:\d+)/`)
	out := newWatchBuffer(func(s string) bool { return serving.MatchString(s) })
	errOut := newWatchBuffer(func(string) bool { return false })
	serveCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan int, 1)
	go func() {
		done <- Run(serveCtx, []string{"--env-file", envFile, "serve"}, IO{In: strings.NewReader(""), Out: out,
			Err: errOut}, "dev", func(rt *Runtime) {
			rt.Env = func(k string) (string, bool) { v, ok := environ[k]; return v, ok }
			rt.Launchers = testLaunchers()
		})
	}()
	select {
	case <-out.Done:
	case code := <-done:
		t.Fatalf("serve exited with %d before serving:\n%s", code, errOut.String())
	}
	base := "https://" + serving.FindStringSubmatch(out.String())[1]
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool,
		MinVersion: tls.VersionTLS12}}, Timeout: 10 * time.Second}
	cli := func(argv ...string) string {
		t.Helper()
		var cliOut, cliErr bytes.Buffer
		code := Run(ctx, argv, IO{In: strings.NewReader(""), Out: &cliOut, Err: &cliErr}, "dev", func(rt *Runtime) {
			env := map[string]string{ServerEnv: base, TokenEnv: testToken}
			rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
			rt.HTTPClient = client
		})
		if code != 0 {
			t.Fatalf("millstream %v: %d %q %q\nserve:\n%s", argv, code, cliOut.String(), cliErr.String(),
				lastLines(errOut.String(), 15))
		}
		return cliOut.String()
	}
	tokenLine := regexp.MustCompile(`extra webhook hook: POST (/hooks/extra/hook) with Authorization: Bearer (\S+) `)
	m := tokenLine.FindStringSubmatch(cli("apply", fixture(lifecycleV2)))
	if m == nil {
		t.Fatal("apply printed no token")
	}
	path, token := m[1], m[2]

	call := func(token, key, body string) (int, api.WebhookRun) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "https://sender.example") // a service elsewhere: no browser guard applies
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if key != "" {
			req.Header.Set(api.IdempotencyHeader, key)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var run api.WebhookRun
		_ = json.NewDecoder(resp.Body).Decode(&run)
		return resp.StatusCode, run
	}
	for _, tok := range []string{"", testToken} {
		if code, _ := call(tok, "", `{"n":2}`); code != http.StatusUnauthorized {
			t.Fatalf("with token %q: %d", tok, code)
		}
	}
	code, run := call(token, "evt-1", `{"n":2}`)
	if code != http.StatusAccepted || run.RunID != "run_evt-1_hook" {
		t.Fatalf("webhook: %d %+v\nserve:\n%s", code, run, lastLines(errOut.String(), 15))
	}
	if code, again := call(token, "evt-1", `{"n":2}`); code != http.StatusOK || again.Created {
		t.Fatalf("sent again: %d %+v", code, again)
	}
	waitFor(t, func() bool {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/v1/pipelines/extra/runs/"+run.RunID, nil)
		req.Header.Set("Authorization", "Bearer "+testToken)
		resp, err := client.Do(req)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		var r api.Run
		return json.NewDecoder(resp.Body).Decode(&r) == nil && r.State == "succeeded" && r.TriggerSource == "webhook"
	})

	m = tokenLine.FindStringSubmatch(cli("trigger", "token", "extra", "hook", "--rotate"))
	if m == nil || m[2] == token {
		t.Fatalf("rotation printed no new token: %v", m)
	}
	if code, _ := call(token, "", `{"n":3}`); code != http.StatusUnauthorized {
		t.Fatalf("the old token after the rotation: %d", code)
	}
	if code, _ := call(m[2], "", `{"n":3}`); code != http.StatusAccepted {
		t.Fatalf("the new token: %d", code)
	}
	stop()
	if code := <-done; code != 0 {
		t.Fatalf("serve exited with %d:\n%s", code, errOut.String())
	}
	if strings.Contains(errOut.String(), token) || strings.Contains(out.String(), token) {
		t.Error("serve logged a webhook token")
	}
}
