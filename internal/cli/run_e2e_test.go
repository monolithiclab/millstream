//go:build e2e

package cli

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// More run flows through the engine, beyond Test_runOnce: a generated request id and a failing run, a cron
// trigger, and a run until interrupted.

func Test_runOnceGeneratesRequestIDAndFailsOnErrors(t *testing.T) {
	t.Parallel()
	pkg, db := runPackage(t, "run"), migratedDB(t)
	out, _ := mustRun(t, 1, "--db", dsn(db), "run", "--once", "--var", "scoreMode=error", pkg)
	if !regexp.MustCompile(`^listings run_\d{8}T\d{6}Z-[0-9a-f]{8}_adhoc completedWithErrors\n`).MatchString(out) ||
		!strings.Contains(out, "  score.dlq 3\n") {
		t.Fatalf("summary:\n%s", out)
	}
}

func Test_runOnceCron(t *testing.T) {
	t.Parallel()
	pkg, db := runPackage(t, "cron"), migratedDB(t)
	out, _ := mustRun(t, 0, "--db", dsn(db), "run", "--once", pkg)
	if !regexp.MustCompile(`^nightly run_\d{4}-\d\d-\d\dT\d\d:\d\dZ_daily succeeded\n  trigger.daily 1\n$`).
		MatchString(out) {
		t.Fatalf("summary:\n%s", out)
	}
}

// run serves the API and the UI until interrupted: a trigger through the API, its run's events over a real SSE
// stream until it settles, then a resumption from the middle that replays exactly the rest.
func Test_runUntilInterrupted(t *testing.T) {
	t.Parallel()
	pkg, db := runPackage(t, "run"), migratedDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serving := regexp.MustCompile(`serving the UI and the API on (http://127\.0\.0\.1:\d+/)`)
	out := newWatchBuffer(func(s string) bool { return serving.MatchString(s) })
	errOut := &syncBuffer{}
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, []string{"--db", dsn(db), "run", "--lease-ttl", "3s", "--listen", "127.0.0.1:0", pkg},
			IO{In: strings.NewReader(""), Out: out, Err: errOut}, "dev", func(rt *Runtime) { rt.Launchers = testLaunchers() })
	}()
	select {
	case <-out.Done:
	case code := <-done:
		t.Fatalf("run exited with %d before serving:\n%s", code, errOut.String())
	}
	base := serving.FindStringSubmatch(out.String())[1]
	client := &http.Client{Timeout: 30 * time.Second}

	if page := httpGet(t, client, base); !strings.Contains(page, "<html") {
		t.Fatalf("GET /: %q", page)
	}
	if ps := httpGet(t, client, base+"api/v1/pipelines"); !strings.Contains(ps, `"name":"listings"`) {
		t.Fatalf("pipelines: %s", ps)
	}
	resp, err := client.Post(base+"api/v1/pipelines/listings/runs", "application/json",
		strings.NewReader(`{"requestId":"e2e"}`))
	if err != nil {
		t.Fatal(err)
	}
	var run struct{ ID string }
	if err := json.NewDecoder(resp.Body).Decode(&run); err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("trigger: %d %v", resp.StatusCode, err)
	}
	_ = resp.Body.Close()

	// The run's events from the start, live, until it settles.
	live := base + "api/v1/live/runs/" + run.ID + "?pipeline=listings"
	all := readEvents(t, ctx, live+"&lastEventId=0", "runSettled")
	if len(all) < 4 {
		t.Fatalf("run events: %v", all)
	}
	// Resuming from the middle, with the header, replays exactly the events after it.
	mid := all[len(all)/2]
	rest := readEvents(t, ctx, live, "runSettled", "Last-Event-ID", mid)
	want := all[len(all)/2+1:]
	if !slices.Equal(rest, want) {
		t.Fatalf("resumed after %s: %v, want %v", mid, rest, want)
	}
	// The pipeline's snapshots stream too.
	snap := readEvents(t, ctx, base+"api/v1/live/pipelines/listings", "")
	if len(snap) != 1 {
		t.Fatalf("snapshot: %v", snap)
	}

	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("code=%d err=%s", code, errOut.String())
	}
	status, _ := mustRun(t, 0, "--db", dsn(db), "status")
	if !regexp.MustCompile(`listings\s+active\s+1\s`).MatchString(status) {
		t.Fatalf("status:\n%s", status)
	}
}

func httpGet(t *testing.T, c *http.Client, url string) string {
	t.Helper()
	resp, err := c.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d %v", url, resp.StatusCode, err)
	}
	return string(body)
}

// readEvents reads a live stream's event ids (the event names for a stream without ids) until a state event of the
// kind until arrives, or the first event when until is empty. header is a header name and value to send.
func readEvents(t *testing.T, ctx context.Context, url, until string, header ...string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(header) == 2 {
		req.Header.Set(header[0], header[1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, ct)
	}
	var out []string
	var id, name string
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		k, v, _ := strings.Cut(sc.Text(), ": ")
		switch k {
		case "id":
			id = v
		case "event":
			name = v
		case "data":
			out = append(out, cmp.Or(id, name))
			if until == "" || strings.Contains(v, `"kind":"`+until+`"`) {
				return out
			}
			id, name = "", ""
		}
	}
	t.Fatalf("GET %s: the stream ended after %v: %v", url, out, sc.Err())
	return nil
}
