//go:build e2e

package cli

import (
	"context"
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

// serve on Postgres: packages applied through its API (apply --server) are loaded from the store, a trigger through
// the API runs end to end on the embedded node, read-only commands read beside it without the lock, and a second
// serve on the same store waits as the standby, serving the reads and refusing actions with the active engine's id
// and address, until it is stopped.
func Test_serveOnPostgres(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	dir := t.TempDir()
	envFile := filepath.Join(dir, "millstream.yaml")
	if err := os.WriteFile(envFile, []byte("store: { postgres: { dsn: { env: MILLSTREAM_DSN } } }\n"+
		"listen: 127.0.0.1:0\nartifacts: { local: { path: state/artifacts } }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	environ := map[string]string{"MILLSTREAM_DSN": dsn, "USER": "ops", "PATH": os.Getenv("PATH")}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	serve := func(ctx context.Context, out, errOut *watchBuffer) chan int {
		done := make(chan int, 1)
		go func() {
			done <- Run(ctx, []string{"--env-file", envFile, "serve", "--lease-ttl", "5s"},
				IO{In: strings.NewReader(""), Out: out, Err: errOut}, "dev", func(rt *Runtime) {
					rt.Env = func(k string) (string, bool) { v, ok := environ[k]; return v, ok }
					rt.Launchers = testLaunchers()
				})
		}()
		return done
	}
	serving := regexp.MustCompile(`serving the UI and the API on http://(127\.0\.0\.1:\d+)/`)
	activeOut := newWatchBuffer(func(s string) bool { return serving.MatchString(s) })
	activeErr := newWatchBuffer(func(string) bool { return false })
	activeCtx, stopActive := context.WithCancel(ctx)
	defer stopActive()
	activeDone := serve(activeCtx, activeOut, activeErr)
	select {
	case <-activeOut.Done:
	case code := <-activeDone:
		t.Fatalf("serve exited with %d before serving:\n%s", code, activeErr.String())
	}
	base := "http://" + serving.FindStringSubmatch(activeOut.String())[1]
	defer func() {
		// The package the engine loaded from the store, written beside the artifact store, read-only.
		if _, err := os.Stat(filepath.Join(dir, "state", "packages")); err != nil {
			t.Errorf("package cache: %v", err)
		}
	}()

	client := func(argv ...string) (int, string, string) {
		env := map[string]string{ServerEnv: base, "MILLSTREAM_DSN": dsn}
		return runCLICtx(t, ctx, env, append([]string{"--env-file", envFile}, argv...)...)
	}
	// An internal error names nothing to the client (the API logs the cause, not the store's internals): the serve
	// log beside it says what failed, such as a Postgres out of disk (SQLSTATE 53100).
	pkg := runPackage(t, "run")
	if code, out, errOut := client("apply", pkg, "--message", "e2e"); code != 0 ||
		!regexp.MustCompile(`^listings revision 1 `).MatchString(out) {
		t.Fatalf("apply --server: %d %q %q\nserve:\n%s", code, out, errOut, lastLines(activeErr.String(), 15))
	}
	code, out, errOut := client("trigger", "listings", "adhoc", "--request-id", "e2e")
	if code != 0 || out != "run_e2e_adhoc\n" {
		t.Fatalf("trigger --server: %d %q %q\nserve:\n%s", code, out, errOut, lastLines(activeErr.String(), 15))
	}
	events := readEvents(t, ctx, base+"/api/v1/live/runs/run_e2e_adhoc?pipeline=listings&lastEventId=0", "runSettled")
	if len(events) < 4 {
		t.Fatalf("run events: %v", events)
	}
	// status reads beside the active engine, without the lock, so it does not wait for it.
	if code, out, errOut := client("status"); code != 0 ||
		!regexp.MustCompile(`run_e2e_adhoc\s+listings\s+succeeded`).MatchString(out) {
		t.Fatalf("status beside serve: %d\n%s\n%s\nserve:\n%s", code, out, errOut, lastLines(activeErr.String(), 15))
	}
	// Without --server, a state change on Postgres is refused rather than waiting for the lock.
	if code, _, errOut := runCLICtx(t, ctx, environ, "--env-file", envFile, "pause", "listings"); code != exitUsage ||
		!strings.Contains(errOut, "only writer") {
		t.Fatalf("pause without --server: %d %q", code, errOut)
	}

	standbyOut := newWatchBuffer(func(s string) bool { return serving.MatchString(s) })
	standbyErr := newWatchBuffer(func(s string) bool { return strings.Contains(s, "waiting as the standby") })
	standbyCtx, stopStandby := context.WithCancel(ctx)
	defer stopStandby()
	standbyDone := serve(standbyCtx, standbyOut, standbyErr)
	select {
	case <-standbyErr.Done:
	case code := <-standbyDone:
		t.Fatalf("the second serve exited with %d instead of standing by:\n%s", code, standbyErr.String())
	}
	<-standbyOut.Done
	standby := "http://" + serving.FindStringSubmatch(standbyOut.String())[1]
	// The standby serves the reads from a read-only store, and refuses actions, naming the active engine.
	var runs api.Page[api.Run]
	waitFor(t, func() bool { return tryJSON(t, ctx, standby+"/api/v1/pipelines/listings/runs", &runs) })
	if len(runs.Items) != 1 {
		t.Fatalf("runs from the standby: %+v", runs)
	}
	var nodes api.Nodes
	waitFor(t, func() bool {
		return tryJSON(t, ctx, standby+"/api/v1/nodes", &nodes) && len(nodes.Engines) == 2
	})
	if nodes.Engine.State != "standby" || nodes.Engines[0].State != "active" || nodes.Engines[0].Epoch != 1 ||
		nodes.Engines[1].ID != nodes.Engine.ID {
		t.Fatalf("nodes from the standby: %+v", nodes)
	}
	resp := postJSON(t, ctx, standby+"/api/v1/pipelines/listings/pause")
	if resp.StatusCode != http.StatusServiceUnavailable ||
		resp.Header.Get(api.ActiveEngineHeader) != nodes.Engines[0].ID ||
		resp.Header.Get(api.ActiveAddressHeader) != strings.TrimPrefix(base, "http://") {
		t.Fatalf("an action on the standby: %d %v", resp.StatusCode, resp.Header)
	}
	stopStandby()
	if code := <-standbyDone; code != 0 {
		t.Fatalf("the stopped standby exited with %d:\n%s", code, standbyErr.String())
	}
	stopActive()
	if code := <-activeDone; code != 0 {
		t.Fatalf("serve exited with %d:\n%s", code, activeErr.String())
	}
}

// lastLines returns the last n lines of s.
func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], "\n")
}

// tryJSON decodes the JSON answer of a GET, reporting false while the server answers 503 (it serves no API between
// terms), and failing the test on any other status.
func tryJSON(t *testing.T, ctx context.Context, url string, v any) bool {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusServiceUnavailable:
		return false
	case http.StatusOK:
	default:
		t.Fatalf("GET %s: %s", url, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return true
}

// postJSON posts an empty JSON object and returns the answer, its body closed.
func postJSON(t *testing.T, ctx context.Context, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp
}

// waitFor polls cond until it holds, failing the test after 30 seconds.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 30 s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
