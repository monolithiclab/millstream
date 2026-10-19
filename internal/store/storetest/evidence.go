package storetest

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// TestEvidenceExports: the redaction key is made once and kept; an export records its item keys once each, refusing
// a second record of the same bundle; a purge lists the bundles that held a key it erases, by its key or by an
// item of its lineage, even one no item holds any more, and forgets the key in the exports (spec section 11.2).
func TestEvidenceExports(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	k1, err := e.s.EvidenceKey(e.ctx)
	if err != nil || len(k1) != 32 {
		t.Fatalf("EvidenceKey = %x, %v", k1, err)
	}
	if k2, err := e.s.EvidenceKey(e.ctx); err != nil || !bytes.Equal(k1, k2) {
		t.Fatalf("EvidenceKey again = %x, %v; want the first", k2, err)
	}
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{"score":80}`), out("b", `{"score":90}`))
	expiry := t0.Add(30 * 24 * time.Hour)
	for _, x := range []store.EvidenceExport{
		{ID: "ev1", Pipeline: "listings", ExportedBy: "ana", Scope: []byte(`{}`), Keys: []string{"b", "a", "a", ""}},
		{ID: "ev2", Pipeline: "listings", Scope: []byte(`{}`), Keys: []string{"b"}},
		{ID: "ev3", Pipeline: "listings", Scope: []byte(`{}`), Keys: []string{"gone"}},
	} {
		x.ExpiresAt = expiry
		if err := e.s.RecordExport(e.ctx, x); err != nil {
			t.Fatalf("RecordExport %s: %v", x.ID, err)
		}
	}
	wantErr(t, e.s.RecordExport(e.ctx, store.EvidenceExport{ID: "ev1", Pipeline: "listings", Scope: []byte(`{}`),
		ExpiresAt: expiry}), store.ErrConflict)
	wantErr(t, e.s.RecordExport(e.ctx, store.EvidenceExport{ID: "ev9", Pipeline: "listings", Scope: []byte(`{`),
		ExpiresAt: expiry}), store.ErrInvalid)
	keys := func() map[string][]string {
		t.Helper()
		xs, err := e.s.EvidenceExports(e.ctx, "listings")
		if err != nil {
			t.Fatal(err)
		}
		out := map[string][]string{}
		for _, x := range xs {
			out[x.ID] = x.Keys
		}
		return out
	}
	xs, err := e.s.EvidenceExports(e.ctx, "")
	if err != nil || len(xs) != 3 || xs[0].ExportedBy != "ana" || !xs[0].ExpiresAt.Equal(expiry) ||
		!xs[0].ExportedAt.Equal(t0) || !slices.Equal(xs[0].Keys, []string{"a", "b"}) {
		t.Fatalf("EvidenceExports = %+v, %v", xs, err)
	}
	p, _, err := e.s.StartPurge(e.ctx, store.PurgeRequest{ID: "p_a", Key: "a"})
	if err != nil || !slices.Equal(p.Bundles, []string{"ev1"}) {
		t.Fatalf("purge of a = %+v, %v; want ev1 listed", p, err)
	}
	if again, err := e.s.Purge(e.ctx, "p_a"); err != nil || !slices.Equal(again.Bundles, p.Bundles) {
		t.Fatalf("Purge = %+v, %v", again, err)
	}
	if got := keys(); !slices.Equal(got["ev1"], []string{"b"}) {
		t.Fatalf("keys after purging a = %v, want a forgotten", got)
	}
	b := e.itemsIn("listings", "r1", "listings.raw")[1]
	if b.Key != "b" {
		t.Fatalf("item %+v, want b's", b)
	}
	p, _, err = e.s.StartPurge(e.ctx, store.PurgeRequest{ID: "p_b", Item: b.ID})
	if err != nil || !slices.Equal(p.Bundles, []string{"ev1", "ev2"}) {
		t.Fatalf("purge of b's item = %+v, %v; want ev1 and ev2 listed", p, err)
	}
	p, _, err = e.s.StartPurge(e.ctx, store.PurgeRequest{ID: "p_gone", Key: "gone", Pipeline: "listings"})
	if err != nil || p.Items != 0 || p.State != store.PurgeDone || !slices.Equal(p.Bundles, []string{"ev3"}) {
		t.Fatalf("purge of a key no item holds = %+v, %v; want ev3 listed", p, err)
	}
	if got := keys(); len(got["ev1"])+len(got["ev2"])+len(got["ev3"]) != 0 {
		t.Fatalf("keys after the purges = %v, want none", got)
	}
}

// TestEvidenceImports: an import records the bundle and counts a reference to its file; one expired by the store
// clock is refused and leaves nothing; importing it again changes nothing; once it expires, ExpireEvidence deletes
// it and drops the reference (spec section 11.2).
func TestEvidenceImports(t *testing.T, open Opener) {
	e := newEnv(t, open)
	refs := func() map[string]int64 {
		t.Helper()
		arts, err := e.s.Artifacts(e.ctx, "listings")
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]int64{}
		for _, a := range arts {
			if a.Type != store.EvidenceType || a.Size != 10 {
				t.Fatalf("artifact entry %+v", a)
			}
			out[a.SHA256] = a.Refcount
		}
		return out
	}
	im := store.EvidenceImport{ID: "ev1", Pipeline: "listings", RevisionHash: "rev", SHA256: shaA, Size: 10, Items: 3,
		ExportedAt: t0.Add(-time.Hour), ExpiresAt: t0}
	wantErr(t, func() error { _, err := e.s.RecordImport(e.ctx, im); return err }(), store.ErrConflict)
	if got := refs(); len(got) != 0 {
		t.Fatalf("refcounts after a refused import = %v", got)
	}
	wantErr(t, func() error { _, err := e.s.RecordImport(e.ctx, store.EvidenceImport{ID: "x"}); return err }(),
		store.ErrInvalid)
	im.ExpiresAt = t0.Add(time.Hour)
	for i, want := range []bool{true, false} {
		if created, err := e.s.RecordImport(e.ctx, im); err != nil || created != want {
			t.Fatalf("RecordImport %d = %v, %v; want %v", i, created, err, want)
		}
	}
	if got := refs(); got[shaA] != 1 {
		t.Fatalf("refcounts = %v, want the bundle's file counted once", got)
	}
	got, err := e.s.EvidenceImports(e.ctx, "listings")
	if err != nil || len(got) != 1 || got[0].SHA256 != shaA || got[0].Items != 3 || got[0].RevisionHash != "rev" ||
		!got[0].ImportedAt.Equal(t0) || !got[0].ExpiresAt.Equal(im.ExpiresAt) ||
		!got[0].ExportedAt.Equal(im.ExportedAt) {
		t.Fatalf("EvidenceImports = %+v, %v", got, err)
	}
	if gone, err := e.s.ExpireEvidence(e.ctx); err != nil || len(gone) != 0 {
		t.Fatalf("ExpireEvidence before expiry = %+v, %v", gone, err)
	}
	e.clock.Advance(time.Hour)
	if gone, err := e.s.ExpireEvidence(e.ctx); err != nil || len(gone) != 1 || gone[0].ID != "ev1" {
		t.Fatalf("ExpireEvidence = %+v, %v", gone, err)
	}
	if got, err := e.s.EvidenceImports(e.ctx, ""); err != nil || len(got) != 0 {
		t.Fatalf("EvidenceImports after expiry = %+v, %v", got, err)
	}
	if got := refs(); got[shaA] != 0 {
		t.Fatalf("refcounts after expiry = %v, want the bundle unreferenced", got)
	}
}
