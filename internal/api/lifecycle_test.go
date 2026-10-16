package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// extraSrc is a pipeline of builtins, which check passes without any action.
const extraSrc = `
pipeline: extra
triggers: { adhoc: { manual: {} } }
steps: { pass: { action: filter, in: trigger.adhoc, config: { expr: "%s" } } }
`

// applyBody is an apply of the pipeline files given, one package each.
func applyBody(message string, pipelines ...string) string {
	var pkgs []string
	for _, src := range pipelines {
		pkgs = append(pkgs, fmt.Sprintf(`{"files":{"pipeline.yaml":%q}}`,
			base64.StdEncoding.EncodeToString([]byte(src))))
	}
	return fmt.Sprintf(`{"packages":[%s],"message":%q}`, strings.Join(pkgs, ","), message)
}

// Apply, disable, resume and rollback, through the API on a store of their own, served by an engine that keeps
// the packages it is sent in its package directory.
func TestLifecycleActions(t *testing.T) {
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng, err := engine.New(engine.Options{Store: st, PackageDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	h, err := New(Options{Store: st, Engine: eng, User: "tester", MaxPackagesBody: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	post := func(target, body string) *http.Response {
		t.Helper()
		return do(t, h, request{method: http.MethodPost, target: "/api/v1" + target, body: body}).Result()
	}
	decode := func(res *http.Response, v any) {
		t.Helper()
		if err := json.NewDecoder(res.Body).Decode(v); err != nil {
			t.Fatal(err)
		}
	}

	// Apply two revisions of a pipeline, the second twice: the repeat creates nothing.
	var applied ApplyResponse
	for i, want := range []struct {
		expr    string
		created bool
	}{{"true", true}, {"item.n > 1", true}, {"item.n > 1", false}} {
		res := post("/revisions", applyBody(fmt.Sprintf("apply %d", i), fmt.Sprintf(extraSrc, want.expr)))
		if res.StatusCode != http.StatusOK {
			t.Fatalf("apply %d: %d", i, res.StatusCode)
		}
		decode(res, &applied)
		if len(applied.Revisions) != 1 || applied.Revisions[0].Created != want.created ||
			applied.Revisions[0].Pipeline != "extra" {
			t.Fatalf("apply %d = %+v", i, applied)
		}
	}
	revs := get[[]Revision](t, h, "/api/v1/pipelines/extra/revisions")
	if len(revs) != 2 || !revs[0].Active || revs[0].Message != "apply 1" {
		t.Fatalf("revisions = %+v", revs)
	}

	// Disable: the pipeline's triggers refuse to fire until it is resumed.
	if res := post("/pipelines/extra/disable", ""); res.StatusCode != http.StatusNoContent {
		t.Fatalf("disable: %d", res.StatusCode)
	}
	if p := get[Pipeline](t, h, "/api/v1/pipelines/extra"); p.State != "disabled" {
		t.Fatalf("state = %s", p.State)
	}
	wantError(t, do(t, h, request{method: http.MethodPost, target: "/api/v1/pipelines/extra/runs", body: "{}"}), 409,
		codeConflict)
	wantError(t, do(t, h, request{method: http.MethodPost, target: "/api/v1/pipelines/extra/pause"}), 409, codeConflict)
	if res := post("/pipelines/extra/resume", ""); res.StatusCode != http.StatusNoContent {
		t.Fatalf("resume: %d", res.StatusCode)
	}
	if p := get[Pipeline](t, h, "/api/v1/pipelines/extra"); p.State != "active" {
		t.Fatalf("state after resume = %s", p.State)
	}

	// Rollback: the revision before the active one by default, or the one named.
	res := post("/pipelines/extra/rollback", "{}")
	var rev Revision
	decode(res, &rev)
	if res.StatusCode != http.StatusOK || rev.ID != 1 || !rev.Active {
		t.Fatalf("rollback: %d %+v", res.StatusCode, rev)
	}
	res = post("/pipelines/extra/rollback", `{"revision":2}`)
	decode(res, &rev)
	if res.StatusCode != http.StatusOK || rev.ID != 2 {
		t.Fatalf("rollback to 2: %d %+v", res.StatusCode, rev)
	}
	errs := []struct {
		target, body string
		status       int
		code         string
	}{
		{"/pipelines/extra/rollback", `{"revision":2}`, 409, codeConflict},
		{"/pipelines/extra/rollback", `{"revision":9}`, 404, codeNotFound},
		{"/pipelines/extra/rollback", `{"revision":-1}`, 400, codeInvalid},
		{"/pipelines/nope/rollback", ``, 404, codeNotFound},
		{"/pipelines/nope/disable", ``, 404, codeNotFound},
		{"/revisions", `{"packages":[]}`, 400, codeInvalid},
		{"/revisions", `{"packages":[{"files":{"other.yaml":""}}]}`, 400, codeInvalid},
		{"/revisions", applyBody("", "pipeline: bad\nsteps: { s: { action: ./actions/none, in: trigger.x } }\n"), 400,
			codeInvalid},
		{"/revisions", applyBody("", fmt.Sprintf(extraSrc, strings.Repeat("x", 5000))), 413, codeTooLarge},
	}
	for _, e := range errs {
		wantError(t, do(t, h, request{method: http.MethodPost, target: "/api/v1" + e.target, body: e.body}), e.status,
			e.code)
	}

	// Without an engine, apply is unavailable.
	bare, err := New(Options{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	wantError(t, do(t, bare, request{method: http.MethodPost, target: "/api/v1/revisions", body: applyBody("", extraSrc)}),
		503, codeUnavailable)
}
