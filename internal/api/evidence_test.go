package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/pipeline"
)

// An export answers a bundle recorded under the request's user; a development deployment imports it and a production
// one refuses it (spec section 11.2).
func TestEvidenceActions(t *testing.T) {
	f := scratchFixture(t)
	server := func(mode string) *Server {
		srv, err := New(Options{Store: f.st, Artifacts: f.arts, User: "tester", Clock: f.clock,
			Evidence: EvidenceSettings{Mode: mode, Sample: 5}})
		if err != nil {
			t.Fatal(err)
		}
		return srv
	}
	dev := server(pipeline.ModeDevelopment)
	post := func(srv *Server, target, body string) int {
		t.Helper()
		return do(t, srv, request{method: http.MethodPost, target: target, body: body}).Code
	}
	rec := do(t, dev, request{method: http.MethodPost, target: "/api/v1/pipelines/listings/evidence",
		body: `{"since":"7d"}`})
	if rec.Code != http.StatusCreated || rec.Header().Get("Content-Type") != "application/gzip" {
		t.Fatalf("export: %d %s", rec.Code, rec.Header())
	}
	b, err := evidence.Decode(rec.Body.Bytes())
	if err != nil || b.ID != rec.Header().Get(BundleHeader) || b.Scope.Sample != 5 || len(b.Items) == 0 {
		t.Fatalf("exported bundle = %+v, %v", b, err)
	}
	if xs, err := f.st.EvidenceExports(f.ctx, "listings"); err != nil || len(xs) == 0 ||
		xs[len(xs)-1].ExportedBy != "tester" {
		t.Fatalf("exports = %+v, %v", xs, err)
	}
	if code := post(dev, "/api/v1/pipelines/listings/evidence", `{"since":"soon"}`); code != http.StatusBadRequest {
		t.Errorf("export with a bad since: %d", code)
	}
	if code := post(dev, "/api/v1/pipelines/nope/evidence", `{}`); code != http.StatusNotFound {
		t.Errorf("export of no pipeline: %d", code)
	}
	body, _ := json.Marshal(EvidenceImportRequest{Bundle: rec.Body.Bytes()})
	got := do(t, dev, request{method: http.MethodPost, target: "/api/v1/evidence/imports", body: string(body)})
	var im EvidenceImport
	if err := json.Unmarshal(got.Body.Bytes(), &im); err != nil || got.Code != http.StatusCreated || im.ID != b.ID ||
		im.Diverged || im.LocalRevision != b.Revision.PackageHash {
		t.Fatalf("import: %d %s", got.Code, got.Body.String())
	}
	if code := post(server(pipeline.ModeProduction), "/api/v1/evidence/imports", string(body)); code != http.StatusConflict {
		t.Errorf("import in production: %d", code)
	}
	if code := post(dev, "/api/v1/evidence/imports", `{}`); code != http.StatusBadRequest {
		t.Errorf("import of nothing: %d", code)
	}
}
