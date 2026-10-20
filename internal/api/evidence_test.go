package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
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

// A development deployment lists the open insights of the bundles it imported beside its own, naming the bundle, and
// reads an item it lacks, with its lineage, in them (spec section 11.2): what the improve pipeline drafts from.
func TestImportedEvidenceReads(t *testing.T) {
	f := scratchFixture(t)
	ts := f.clock.Now()
	payload := json.RawMessage(`{"score":40}`)
	b := evidence.Bundle{
		Manifest: evidence.Manifest{Format: evidence.Format, Version: evidence.Version, ID: "evidence_prod",
			Pipeline: "prod-only", ExportedAt: ts, ExpiresAt: ts.Add(time.Hour)},
		Evidence: evidence.Evidence{
			Insights: []evidence.Insight{
				{ID: "in-open", Kind: pipeline.InsightReviewPattern, Scope: "review", Severity: "warning",
					State: string(store.InsightOpen), Evidence: json.RawMessage(`{}`)},
				{ID: "in-muted", Kind: pipeline.InsightSlowStep, Scope: "score", Severity: "info",
					State: string(store.InsightMuted), Evidence: json.RawMessage(`{}`)},
			},
			Items: []evidence.Item{
				{ID: "it-root", Run: "run-1", Stream: "listings.raw", Key: "k1", TS: ts, Payload: payload},
				{ID: "it-scored", Run: "run-1", Stream: "listings.scored", Step: "score", Key: "k1",
					Parents: []string{"it-root"}, TS: ts, Payload: payload},
			},
		},
	}
	data, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evidence.Import(f.ctx, f.st, f.arts, ts, pipeline.ModeDevelopment, data); err != nil {
		t.Fatal(err)
	}
	srv := f.server()
	ins := get[[]Insight](t, srv, "/api/v1/insights?pipeline=prod-only")
	if len(ins) != 1 || ins[0].ID != "in-open" || ins[0].Imported != "evidence_prod" || ins[0].State != "open" {
		t.Fatalf("insights = %+v", ins)
	}
	if ins := get[[]Insight](t, srv, "/api/v1/insights?pipeline=prod-only&state=muted"); len(ins) != 0 {
		t.Errorf("muted insights = %+v", ins)
	}
	if it := get[Item](t, srv, "/api/v1/pipelines/prod-only/items/it-scored"); it.ID != "it-scored" ||
		string(it.Payload) != string(payload) {
		t.Errorf("imported item = %+v", it)
	}
	lin := get[Lineage](t, srv, "/api/v1/pipelines/prod-only/items/it-root/lineage")
	if len(lin.Up) != 0 || len(lin.Down) != 1 || lin.Down[0].ID != "it-scored" {
		t.Errorf("imported lineage = %+v", lin)
	}
	if lin := get[Lineage](t, srv, "/api/v1/pipelines/prod-only/items/it-scored/lineage"); len(lin.Up) != 1 ||
		lin.Up[0].ID != "it-root" {
		t.Errorf("imported lineage up = %+v", lin)
	}
	wantError(t, do(t, srv, request{method: http.MethodGet, target: "/api/v1/pipelines/prod-only/items/nope"}),
		http.StatusNotFound, "notFound")
	// An insight the deployment records itself wins over the imported copy.
	if err := f.st.RecordInsights(f.ctx, store.InsightReport{Pipeline: "prod-only",
		Kinds: []string{pipeline.InsightReviewPattern}, Insights: []store.Insight{{Pipeline: "prod-only",
			ID: "in-open", Kind: pipeline.InsightReviewPattern, Scope: "review", Severity: "warning",
			WindowStart: ts, WindowEnd: ts, Evidence: json.RawMessage(`{}`)}}}); err != nil {
		t.Fatal(err)
	}
	if ins := get[[]Insight](t, srv, "/api/v1/insights?pipeline=prod-only"); len(ins) != 1 || ins[0].Imported != "" {
		t.Errorf("insights with an own copy = %+v", ins)
	}
}
