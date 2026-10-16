package cli

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/store"
)

// The lifecycle fixtures: a package of builtins in two revisions, and one whose check fails.
const lifecycleV1, lifecycleV2, lifecycleBad = "lifecycle-v1", "lifecycle-v2", "lifecycle-bad"

// apply, rollback, disable and resume from the command's own process, on SQLite: apply and rollback through the
// parser, the rest built directly (the parser costs more than their work).
func Test_lifecycleCommands(t *testing.T) {
	s := newEmptyState(t)
	if out, _ := s.must(0, "apply", fixture(lifecycleV1), "--message", "first"); !regexp.MustCompile(
		`^extra revision 1 [0-9a-f]{12}\n$`).MatchString(out) {
		t.Fatalf("apply: %q", out)
	}
	steps := []struct {
		cmd  runner
		fail string // the error, when the command fails
		out  string // a prefix of what it prints
	}{
		{ApplyCmd{Dirs: []string{fixture(lifecycleV1)}}, "", "extra revision 1 "},
		{ApplyCmd{Dirs: []string{fixture(lifecycleV2)}}, "", "extra revision 2 "},
		{RollbackCmd{Pipeline: "extra", Revision: 2}, "conflict", ""},
		{DisableCmd{Pipeline: "extra"}, "", "extra disabled\n"},
		{TriggerCmd{Pipeline: "extra", Trigger: "adhoc", Payload: "{}"}, "is disabled", ""},
		{PauseCmd{Pipeline: "extra"}, "resume it before pausing it", ""},
		{ResumeCmd{Pipeline: "extra"}, "", "extra resumed\n"},
		{ApplyCmd{Dirs: []string{fixture(lifecycleBad)}}, "failed", ""},
	}
	for _, step := range steps {
		out, errOut, err := s.call(nil, step.cmd)
		if step.fail == "" && err != nil || step.fail != "" && (err == nil || !strings.Contains(err.Error(), step.fail)) ||
			!strings.HasPrefix(out, step.out) {
			t.Fatalf("%+v: %q, %q, %v", step.cmd, out, errOut, err)
		}
		if _, ok := step.cmd.(DisableCmd); ok {
			if st := s.status(); !regexp.MustCompile(`extra\s+disabled\s+2`).MatchString(st) {
				t.Fatalf("status after disable:\n%s", st)
			}
		}
		if a, ok := step.cmd.(ApplyCmd); ok && a.Dirs[0] == fixture(lifecycleBad) && !strings.Contains(errOut, "extra: error steps.pass") {
			t.Fatalf("a failing check prints its findings: %q", errOut)
		}
	}
	if out, _ := s.must(0, "rollback", "extra"); out != "extra rolled back to revision 1\n" {
		t.Fatalf("rollback: %q", out)
	}
	if st := s.status(); !regexp.MustCompile(`extra\s+active\s+1`).MatchString(st) {
		t.Fatalf("status:\n%s", st)
	}
}

// On Postgres, a command that changes state refuses to write from its own process unless --direct says so; a
// --server that is not a URL is a usage error.
func TestStateChangeRefusals(t *testing.T) {
	pg := fixture("environments/postgres.yaml")
	for _, argv := range [][]string{
		{"--env-file", pg, "disable", "p"},
		{"--env-file", pg, "pause", "p"},
		{"--env-file", pg, "apply", sharedPackage(t, "valid")},
	} {
		code, _, errOut := runCLICtx(t, context.Background(), nil, argv...)
		if code != exitUsage || !strings.Contains(errOut, "on Postgres the active engine is the only writer") {
			t.Fatalf("%v: code %d, stderr %q", argv, code, errOut)
		}
	}
	// --direct goes on to open the store, which needs the DSN.
	code, _, errOut := runCLICtx(t, context.Background(), nil, "--env-file", pg, "--direct", "disable", "p")
	if code != exitFail || !strings.Contains(errOut, "MILLSTREAM_DSN is not set") {
		t.Fatalf("--direct: code %d, stderr %q", code, errOut)
	}
	code, _, errOut = runCLICtx(t, context.Background(), nil, "--server", "localhost:7070", "disable", "p")
	if code != exitUsage || !strings.Contains(errOut, "want the API's base URL") {
		t.Fatalf("--server without a scheme: code %d, stderr %q", code, errOut)
	}
}

// handlerTransport serves a client's requests with a handler, in process: the CLI's API client without a listener.
type handlerTransport struct{ h http.Handler }

func (t handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, r)
	return rec.Result(), nil
}

// The commands that change state go through a served engine's API with --server, sending the token and the
// reviewer.
func Test_commandsThroughTheServer(t *testing.T) {
	st := memoryStore(t)
	eng, err := engine.New(engine.Options{Store: st, PackageDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := api.New(api.Options{Store: st, Engine: eng, User: "server"})
	if err != nil {
		t.Fatal(err)
	}
	var auth []string
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = append(auth, r.Header.Get("Authorization"))
		srv.ServeHTTP(w, r)
	})
	env := map[string]string{ServerEnv: "http://127.0.0.1:7070", TokenEnv: "s3cret"}
	run := func(want int, argv ...string) (string, string) {
		t.Helper()
		var out, errb bytes.Buffer
		code := Run(context.Background(), append([]string{"--db", unusedDB()}, argv...),
			IO{In: strings.NewReader(""), Out: &out, Err: &errb}, "dev", func(rt *Runtime) {
				rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
				rt.HTTPClient = &http.Client{Transport: handlerTransport{h}}
			})
		if code != want {
			t.Fatalf("millstream %v: code %d, want %d\nstdout:\n%s\nstderr:\n%s", argv, code, want, out.String(),
				errb.String())
		}
		return out.String(), errb.String()
	}
	dirs := []string{fixture(lifecycleV1), fixture(lifecycleV2)}
	// Through the parser, the server named by the environment: apply, refused, then applied.
	if _, errOut := run(1, "apply", dirs[0], dirs[1]); !strings.Contains(errOut, "pipeline extra is given twice") {
		t.Fatalf("two packages of one pipeline: %q", errOut)
	}
	if out, _ := run(0, "apply", dirs[0], "--message", "first"); !strings.HasPrefix(out, "extra revision 1 ") {
		t.Fatalf("apply: %q", out)
	}
	// The other commands built directly, as the parser would build them.
	call := func(cmd runner) (string, error) {
		var out, errb bytes.Buffer
		rt := &Runtime{Ctx: context.Background(), IO: IO{In: strings.NewReader(""), Out: &out, Err: &errb},
			Env: func(k string) (string, bool) { v, ok := env[k]; return v, ok }, DB: unusedDB(),
			HTTPClient: &http.Client{Transport: handlerTransport{h}}, CurrentUser: noAccount}
		err := cmd.Run(rt)
		return out.String(), err
	}
	trigger := TriggerCmd{Pipeline: "extra", Trigger: "adhoc", Payload: "{}", RequestID: "r1"}
	replay := ReplayCmd{Pipeline: "extra", Step: "pass", RequestID: "rp"}
	for _, step := range []struct {
		cmd runner
		out string // a prefix of what it prints, or of the error
		err bool
	}{
		{ApplyCmd{Dirs: dirs[1:]}, "extra revision 2 ", false},
		{RollbackCmd{Pipeline: "extra"}, "extra rolled back to revision 1\n", false},
		{DisableCmd{Pipeline: "extra"}, "extra disabled\n", false},
		{ResumeCmd{Pipeline: "extra"}, "extra resumed\n", false},
		{PauseCmd{Pipeline: "extra", Step: "pass"}, "extra.pass paused\n", false},
		{ResumeCmd{Pipeline: "extra", Step: "pass"}, "extra.pass resumed\n", false},
		{trigger, "run_r1_adhoc\n", false},
		{trigger, "run_r1_adhoc (already fired)\n", false},
		{CancelCmd{Pipeline: "extra", RunID: "run_r1_adhoc"}, "run_r1_adhoc cancelled\n", false},
		{ReplayCmd{Pipeline: "extra", From: "dlq", RunID: "run_r1_adhoc"}, "0 dead letters replayed\n", false},
		{replay, "run_replay_rp\n", false},
		{replay, "run_replay_rp (already started)\n", false},
		{TriggerCmd{Pipeline: "extra", Trigger: "adhoc", Payload: "{}", Var: map[string]string{"x": "1"}}, "--var", true},
		{InboxApproveCmd{deliveryRef: deliveryRef{Delivery: "d1"}, reviewerFlag: reviewerFlag{As: "ana"}}, "--pipeline", true},
		{InboxSampleCmd{deliveryRef: deliveryRef{Delivery: "d1"}, Decision: "agree", reviewerFlag: reviewerFlag{As: "ana"}},
			"--pipeline", true},
		{InsightsAckCmd{insightRef{ID: "i1"}}, "--pipeline", true},
		{InboxSampleCmd{deliveryRef: deliveryRef{Delivery: "d1", Pipeline: "extra"}, Decision: "agree",
			reviewerFlag: reviewerFlag{As: "ana"}}, "d1", true},
		{InboxApproveCmd{deliveryRef: deliveryRef{Delivery: "d1", Pipeline: "extra"}, reviewerFlag: reviewerFlag{As: "ana"}},
			"d1", true},
		{InboxRejectCmd{deliveryRef: deliveryRef{Delivery: "d1", Pipeline: "extra"}, reviewerFlag: reviewerFlag{As: "ana"}},
			"delivery d1", true},
		{InsightsMuteCmd{insightRef{ID: "i1", Pipeline: "extra"}}, "insight", true},
		{DLQDiscardCmd{Pipeline: "extra", IDs: []string{"dl1"}}, "open dead letter dl1", true},
	} {
		out, err := call(step.cmd)
		got := out
		if err != nil {
			got = err.Error()
		}
		if (err != nil) != step.err || !strings.Contains(got, step.out) {
			t.Fatalf("%+v: %q, %v", step.cmd, out, err)
		}
	}
	for _, a := range auth {
		if a != "Bearer s3cret" {
			t.Fatalf("Authorization %q", a)
		}
	}
}

// serve refuses what it cannot serve before it opens anything: a non-loopback listener (with auth.token, until
// token auth checks it), what production forbids, and bad flags.
func TestServeRefusals(t *testing.T) {
	dir := t.TempDir()
	write := func(name, src string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	open := write("open.yaml", "listen: 0.0.0.0:7070\n")
	token := write("token.yaml", "listen: 0.0.0.0:7070\nauth: { token: { env: T } }\n")
	improve := write("improve.yaml", "improve: { enabled: true }\n")
	for _, tt := range []struct {
		argv []string
		want string
	}{
		{[]string{"--env-file", open, "serve"}, "requires auth.token"},
		{[]string{"--env-file", token, "serve"}, "M4 Task 7"},
		{[]string{"--env-file", open, "serve", "--listen", "127.0.0.1"}, "want host:port"},
		{[]string{"--env-file", improve, "serve"}, "improve.enabled"},
		{[]string{"serve", "--slots", "0"}, "--slots and --lease-ttl must be positive"},
	} {
		code, _, errOut := runCLICtx(t, context.Background(), map[string]string{"USER": "ops"}, tt.argv...)
		if code == 0 || !strings.Contains(errOut, tt.want) {
			t.Errorf("%v: code %d, stderr %q", tt.argv, code, errOut)
		}
	}
	code, _, errOut := direct(nil, ServeCmd{Slots: 4, LeaseTTL: time.Minute})
	if code != exitUsage || !strings.Contains(errOut, "no user") {
		t.Errorf("serve without a user: code %d, stderr %q", code, errOut)
	}
}

// The API client maps the server's error codes to the store's errors, and says what it got when the answer is not
// an API error.
func TestAPIClientErrors(t *testing.T) {
	for code, want := range map[string]error{"notFound": store.ErrNotFound, "conflict": store.ErrConflict,
		"invalid": store.ErrInvalid} {
		if err := error(&apiError{code: code, msg: "m"}); !errors.Is(err, want) || errors.Is(err, store.ErrClosed) {
			t.Errorf("%s: %v", code, err)
		}
	}
	if errors.Is(&apiError{code: "internal"}, store.ErrInvalid) {
		t.Error("internal matches invalid")
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "a=1" {
			http.Error(w, "<html>bad gateway</html>", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte("{"))
	})
	u, _ := url.Parse("http://127.0.0.1:7070")
	c := &apiClient{base: u, http: &http.Client{Transport: handlerTransport{h}}}
	if _, err := c.do(context.Background(), http.MethodGet, "/x", nil, nil); err == nil ||
		!strings.Contains(err.Error(), "502 Bad Gateway") {
		t.Errorf("a non-API error: %v", err)
	}
	var v map[string]any
	if _, err := c.do(context.Background(), http.MethodGet, "/x?a=1", nil, &v); err == nil ||
		!strings.Contains(err.Error(), "decode the server's answer") {
		t.Errorf("a broken answer: %v", err)
	}
	if _, err := c.do(context.Background(), http.MethodPost, "/x", func() {}, nil); err == nil {
		t.Error("a body that is not JSON: no error")
	}
}
