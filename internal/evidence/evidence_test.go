package evidence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/store/sqlite/sqlitetest"
)

func TestMain(m *testing.M) {
	sqlitetest.Warm()
	dir, err := os.MkdirTemp("", "evidence-test-")
	if err != nil {
		panic(err)
	}
	production = seed(dir) // the history, built once outside the budget; TestBundleRoundTrip alone exports from it
	warm(dir)
	code := m.Run()
	_ = production.st.Close()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// production is the seeded production deployment.
var production *seeded

var t0 = time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)

// source is the production pipeline: listings carry a seller whose phone is redacted, a score passes it on, and a
// review gate approves, edits or rejects; score dead-letters what it cannot read.
const source = `
pipeline: listings
triggers:
  daily: { cron: "0 6 * * *" }
streams:
  listings.raw: { redact: [/seller/phone, /price] }
steps:
  crawl: { action: ./actions/crawl, in: trigger.daily, out: listings.raw }
  score: { action: ./actions/score, in: listings.raw, out: listings.scored, retry: { maxAttempts: 1 } }
  gate: { action: review, in: listings.scored, out: listings.ok }
`

// Phones of the sellers, which must never appear in a bundle.
var phones = []string{"06 11 11 11 11", "06 22 22 22 22", "06 33 33 33 33", "06 44 44 44 44"}

const editedPhone = "07 99 99 99 99"

// seeded is a production store with one run settled through every outcome: a approved, b edited, c rejected, d dead.
type seeded struct {
	st   *sqlite.Store
	arts *artifacts.Dir
	now  time.Time
}

// seed builds the production store, its artifacts under dir. It panics on an error: TestMain calls it.
func seed(dir string) *seeded {
	clock := t0
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: func() time.Time { return clock }})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	p, err := pipeline.Parse([]byte(source))
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256([]byte(source))
	rev, err := st.ApplyRevision(ctx, store.PackageRecord{Hash: hex.EncodeToString(sum[:]), Bundle: []byte(source),
		Pipeline: p})
	if err != nil {
		panic(err)
	}
	if _, err := st.Fire(ctx, store.Firing{Pipeline: "listings", Trigger: "daily", Key: "k", RunID: "r1",
		Source: store.SourceManual, Revision: rev, Payload: json.RawMessage(`{}`)}); err != nil {
		panic(err)
	}
	arts := artifacts.New(dir)
	var zipped bytes.Buffer
	zw := gzip.NewWriter(&zipped)
	_, _ = zw.Write([]byte(`{"role":"user","text":"score the listing of ` + phones[0] + `"}` + "\n"))
	_ = zw.Close()
	transcript, err := arts.Put(ctx, "listings", "transcript.jsonl", &zipped)
	if err != nil {
		panic(err)
	}
	done := func(l store.Lease, arts []store.ArtifactRef, outs ...store.Output) {
		if _, err := st.Done(ctx, store.DoneRequest{LeaseRef: l.Ref(), Owner: "n", Attempt: l.Attempt, Outputs: outs,
			Artifacts: arts}); err != nil {
			panic(err)
		}
	}
	for steps := 0; steps < 4; steps++ {
		clock = clock.Add(time.Minute)
		ls, err := st.Lease(ctx, store.LeaseRequest{Owner: "n", Max: 10, TTL: time.Minute})
		if err != nil {
			panic(err)
		}
		for _, l := range ls {
			switch l.Step {
			case "crawl":
				var outs []store.Output
				for i, k := range []string{"a", "b", "c", "d"} {
					outs = append(outs, store.Output{Key: k, Payload: json.RawMessage(`{"price":` +
						string(rune('1'+i)) + `00000,"seller":{"name":"Seller ` + k + `","phone":"` + phones[i] + `"}}`)})
				}
				done(l, nil, outs...)
			case "score":
				if l.Item.Key == "d" {
					if _, err := st.Fail(ctx, store.FailRequest{LeaseRef: l.Ref(), Owner: "n", Attempt: l.Attempt,
						Class: "action", Message: "cannot read " + phones[3]}); err != nil {
						panic(err)
					}
					continue
				}
				var refs []store.ArtifactRef
				if l.Item.Key == "a" {
					refs = []store.ArtifactRef{transcript}
				}
				var in map[string]any
				_ = json.Unmarshal(l.Item.Payload, &in)
				in["score"] = 80
				body, _ := json.Marshal(in)
				done(l, refs, store.Output{Key: l.Item.Key, Payload: body})
			case "gate":
				if _, err := st.Park(ctx, store.ParkRequest{LeaseRef: l.Ref(), Attempt: l.Attempt,
					Spec: store.ReviewSpec{Inbox: "analysts", Editable: []string{"/seller/phone", "/notes"},
						Reasons: []string{"overpriced"}}, Display: "listing"}); err != nil {
					panic(err)
				}
				decision := store.ReviewDecisionRequest{Pipeline: "listings", DeliveryID: l.DeliveryID, Version: 1,
					Decision: store.DecisionApprove, Reviewer: "ana", Text: "called " + phones[2]}
				switch l.Item.Key {
				case "b":
					decision.Patch = json.RawMessage(`[{"op":"replace","path":"/seller/phone","value":"` +
						editedPhone + `"},{"op":"add","path":"/notes","value":"was ` + phones[1] + `"}]`)
				case "c":
					decision.Decision, decision.Reason = store.DecisionReject, "overpriced"
				}
				if _, err := st.DecideReview(ctx, decision); err != nil {
					panic(err)
				}
			}
		}
	}
	if err := st.RecordInsights(ctx, store.InsightReport{Pipeline: "listings", Kinds: []string{"failureCluster"},
		Insights: []store.Insight{{ID: "in1", Kind: "failureCluster", Scope: "score", Severity: "error",
			Evidence: json.RawMessage(`{"samples":["it_gone"]}`), WindowStart: t0, WindowEnd: clock}}}); err != nil {
		panic(err)
	}
	return &seeded{st: st, arts: arts, now: clock.Add(time.Hour)}
}

// archive returns the decompressed content of a bundle's archive, every entry concatenated.
func archive(t *testing.T, data []byte) string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var all bytes.Buffer
	tr := tar.NewReader(zr)
	for {
		if _, err := tr.Next(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(&all, tr)
	}
	return all.String()
}

// TestBundleRoundTrip: an export samples every outcome with its lineage, redacts the phones wherever they travel
// (payloads downstream, the edit, the error, the transcript) and the price at its pointer, names no reviewer and
// keeps no free text, and records the item keys it holds; a development deployment imports it, reads it back with
// Load and reports the divergence, while a production deployment, an expired bundle and a corrupt one are refused.
func TestBundleRoundTrip(t *testing.T) {
	src := production
	ctx := context.Background()
	data, rec, err := Export(ctx, src.st, src.arts, src.now, ExportRequest{Pipeline: "listings", By: "ana",
		Since: t0})
	if err != nil {
		t.Fatal(err)
	}
	text := archive(t, data)
	for _, secret := range append(slices.Clone(phones), editedPhone, "ana", "called ", "100000") {
		if strings.Contains(text, secret) {
			t.Errorf("the bundle holds %q", secret)
		}
	}
	b, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	strata := map[string]int{StratumDone: 4, StratumApproved: 1, StratumEdited: 1, StratumRejected: 1, StratumDead: 1}
	if b.Pipeline != "listings" || b.Revision.PackageHash == "" || string(b.Package) != source ||
		!b.ExpiresAt.Equal(src.now.Add(DefaultTTL)) || len(b.Strata) != len(strata) {
		t.Fatalf("manifest = %+v", b.Manifest)
	}
	for s, n := range strata {
		if b.Strata[s] != n {
			t.Errorf("stratum %s: %d sampled, want %d", s, b.Strata[s], n)
		}
	}
	if len(b.Items) != 1+4+3+2+1 || len(b.Reviews) != 3 || len(b.Transcripts) != 1 || len(b.Deliveries) != 1+4+3 {
		t.Fatalf("bundle holds %d items, %d deliveries, %d reviews, %d transcripts", len(b.Items), len(b.Deliveries),
			len(b.Reviews), len(b.Transcripts))
	}
	hashes := map[string]string{} // each item's redacted phone, by key and stream
	for _, it := range b.Items {
		var p struct {
			Seller struct{ Phone string }
		}
		_ = json.Unmarshal(it.Payload, &p)
		hashes[it.Key+" "+it.Stream] = p.Seller.Phone
	}
	if h := hashes["a listings.raw"]; !strings.HasPrefix(h, HashPrefix) || hashes["a listings.scored"] != h ||
		hashes["a listings.ok"] != h || hashes["b listings.raw"] == h {
		t.Errorf("phones redacted as %v: want one stable hash per phone along its lineage", hashes)
	}
	for _, r := range b.Reviews {
		if r.Decider != DeciderPerson {
			t.Errorf("review %+v names its decider", r)
		}
		if r.Decision == store.DecisionReject && r.Reason != "overpriced" {
			t.Errorf("rejection lost its reason code: %+v", r)
		}
	}
	if !strings.Contains(text, hashes["a listings.raw"]) || !strings.Contains(text, hashes["d listings.raw"]) {
		t.Error("the transcript or the error does not carry the phone's hash")
	}
	if rec.ExportedBy != "ana" || len(rec.Keys) != len(b.Items) {
		t.Errorf("export record = %+v", rec)
	}
	xs, err := src.st.EvidenceExports(ctx, "listings")
	if i := slices.IndexFunc(xs, func(x store.EvidenceExport) bool { return x.ID == b.ID }); err != nil || i < 0 ||
		!slices.Equal(xs[i].Keys, []string{"a", "b", "c", "d", "k"}) {
		t.Fatalf("EvidenceExports = %+v, %v; want %s with its keys", xs, err, b.ID)
	}

	// One insight's evidence: its step's deliveries, without transcripts when there is no artifact store.
	one, _, err := Export(ctx, src.st, nil, src.now, ExportRequest{Pipeline: "listings", Since: t0, Insight: "in1"})
	if err != nil {
		t.Fatal(err)
	}
	if b1, err := Decode(one); err != nil || len(b1.Insights) != 1 || len(b1.Sample) != 4 || b1.Strata[StratumDead] != 1 ||
		len(b1.Transcripts) != 0 {
		t.Fatalf("insight bundle = %+v, %v", b1, err)
	}
	for _, req := range []ExportRequest{{Pipeline: "listings", Insight: "nope"}, {Pipeline: "nope"}} {
		if _, _, err := Export(ctx, src.st, nil, src.now, req); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("Export %+v: %v, want not found", req, err)
		}
	}

	dst, err := sqlite.Open(sqlite.Memory, sqlite.Options{Now: func() time.Time { return src.now }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()
	dstArts := artifacts.New(t.TempDir())
	for _, c := range []struct {
		mode string
		now  time.Time
		data []byte
		want error
	}{
		{pipeline.ModeProduction, src.now, data, ErrProduction},
		{pipeline.ModeDevelopment, b.ExpiresAt, data, ErrExpired},
		{pipeline.ModeDevelopment, src.now, data[:len(data)/2], ErrFormat},
	} {
		if _, err := Import(ctx, dst, dstArts, c.now, c.mode, c.data); !errors.Is(err, c.want) {
			t.Errorf("Import in %s at %s: %v, want %v", c.mode, c.now, err, c.want)
		}
	}
	im, err := Import(ctx, dst, dstArts, src.now, pipeline.ModeDevelopment, data)
	if err != nil || !im.Created || !im.Diverged() || im.LocalRevision != "" || im.Items != len(b.Items) {
		t.Fatalf("Import = %+v, %v", im, err)
	}
	loaded, err := Load(ctx, dst, dstArts, "listings", src.now)
	if err != nil || len(loaded) != 1 || loaded[0].ID != b.ID || len(loaded[0].StoreItems()) != len(b.Items) ||
		len(loaded[0].StoreDeliveries()) != len(b.Deliveries) || loaded[0].StoreReviews()[0].Reviewer != DeciderPerson {
		t.Fatalf("Load = %+v, %v", loaded, err)
	}
	if late, err := Load(ctx, dst, dstArts, "listings", b.ExpiresAt); err != nil || len(late) != 0 {
		t.Fatalf("Load after expiry = %d bundles, %v", len(late), err)
	}
	if _, err := dst.ApplyRevision(ctx, store.PackageRecord{Hash: b.Revision.PackageHash, Bundle: b.Package,
		Pipeline: must(pipeline.Parse(b.Package))}); err != nil {
		t.Fatal(err)
	}
	if again, err := Import(ctx, dst, dstArts, src.now, pipeline.ModeDevelopment, data); err != nil || again.Created ||
		again.Diverged() {
		t.Fatalf("Import on the production package = %+v, %v", again, err)
	}
	if local, err := dst.Items(ctx, store.ItemFilter{}); err != nil || len(local) != 0 {
		t.Fatalf("local items after the import = %d, %v: imported evidence stays out of local history", len(local), err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// warm runs an export and an import once, so that the process prepares their statements before the tests: the
// stores closed hand them on to the next Memory store (sqlitetest).
func warm(dir string) {
	ctx := context.Background()
	data, _, err := Export(ctx, production.st, production.arts, production.now, ExportRequest{Pipeline: "listings",
		Since: t0})
	if err != nil {
		panic(err)
	}
	dst := must(sqlite.Open(sqlite.Memory, sqlite.Options{Now: func() time.Time { return production.now }}))
	defer func() { _ = dst.Close() }()
	if _, err := Import(ctx, dst, artifacts.New(filepath.Join(dir, "warm")), production.now,
		pipeline.ModeDevelopment, data); err != nil {
		panic(err)
	}
}
