//go:build e2e

package storetest

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/monolithiclab/millstream/internal/store"
)

// Artifact variants the e2e build adds: the default build covers the same paths through the other artifact tests.

// TestDoneCountsArtifactsPerItem: each published item counts one reference to each artifact it carries; crawl
// publishes every output on two streams, so two items reference each artifact per output.
func TestDoneCountsArtifactsPerItem(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	crawl := e.leaseStep("crawl")
	e.done(crawl,
		withArtifacts(out("a", `{"score": 80}`), ref("page.html", shaA), ref("photo.jpg", shaB)),
		withArtifacts(out("b", `{"score": 50}`), ref("page.html", shaA)))
	if got, want := e.refcounts("listings"), map[string]int64{shaA: 4, shaB: 2}; !maps.Equal(got, want) {
		t.Fatalf("refcounts = %v, want %v", got, want)
	}
	raw := e.items("listings.raw")
	if len(raw) != 2 || !slices.Equal(raw[0].Artifacts, []store.ArtifactRef{ref("page.html", shaA), ref("photo.jpg", shaB)}) ||
		len(raw[1].Artifacts) != 1 {
		t.Fatalf("raw items %+v", raw)
	}
	// The lease of a downstream delivery carries its item's artifacts.
	score := e.leaseStep("score")
	if len(score.Item.Artifacts) == 0 || score.Item.Artifacts[0] != ref("page.html", shaA) {
		t.Fatalf("score lease item artifacts = %+v", score.Item.Artifacts)
	}
	if got := e.refcounts("other"); len(got) != 0 {
		t.Fatalf("another pipeline sees %v", got)
	}
}

// TestArtifactsTravelWithItems: a dedupe pass republishes the item with its artifacts, a dead input's dead letter
// copies them, and an unrouted output keeps them for its replay.
func TestArtifactsTravelWithItems(t *testing.T, open Opener) {
	t.Run("dedupe", func(t *testing.T) {
		e := newEnv(t, open, dedupePipeline)
		if _, err := e.s.Fire(e.ctx, store.Firing{Pipeline: "dd", Trigger: "adhoc", Key: "r1", RunID: "r1"}); err != nil {
			t.Fatal(err)
		}
		e.done(e.leaseOf("crawl"), withArtifacts(out("k", `{"url":"x"}`), ref("page.html", shaA)))
		e.dedupe(e.leaseOf("unique"), "x", window30d)
		kept := e.itemsOf("kept")
		if len(kept) != 1 || !slices.Equal(kept[0].Artifacts, []store.ArtifactRef{ref("page.html", shaA)}) {
			t.Fatalf("kept = %+v", kept)
		}
		if got := e.refcounts("dd"); got[shaA] != 2 {
			t.Fatalf("refcounts = %v, want 2: the raw item and the kept one", got)
		}
	})
	// The dead-letter and replay cases share a store, in turn, each with its run.
	listings := newEnv(t, open, fixture)
	t.Run("dead letter", func(t *testing.T) {
		e := listings.on(t)
		e.fire("r1")
		e.done(e.leaseStep("crawl"), withArtifacts(out("a", `{"score": 80}`), ref("page.html", shaA)))
		e.fail(e.leaseStep("score"), false)
		dls := e.items("score.dlq")
		if len(dls) != 1 {
			t.Fatalf("dead letters %+v", dls)
		}
		var letter struct {
			Item struct {
				Artifacts []store.ArtifactRef `json:"artifacts"`
			} `json:"item"`
		}
		if err := json.Unmarshal(dls[0].Payload, &letter); err != nil ||
			!slices.Equal(letter.Item.Artifacts, []store.ArtifactRef{ref("page.html", shaA)}) {
			t.Fatalf("dead letter %s (%v)", dls[0].Payload, err)
		}
	})
	t.Run("unrouted replay", func(t *testing.T) {
		e := listings.on(t)
		e.fire("r2")
		e.done(e.leaseStep("crawl"), out("a", `{"score": -1}`))
		e.done(e.leaseStep("score"), withArtifacts(out("", `{"score": -1}`), ref("chart.png", shaB)))
		if got := e.refcounts("listings"); got[shaB] != 0 {
			t.Fatalf("an unrouted output counted %v", got)
		}
		e.apply(fixtureFixed)
		if n, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r2", nil, true); err != nil || n != 1 {
			t.Fatalf("ReplayDeadLetters = %d, %v", n, err)
		}
		rest := e.itemsIn("listings", "r2", "listings.rest")
		if len(rest) != 1 || !slices.Equal(rest[0].Artifacts, []store.ArtifactRef{ref("chart.png", shaB)}) {
			t.Fatalf("replayed output %+v", rest)
		}
		if got := e.refcounts("listings"); got[shaB] != 1 {
			t.Fatalf("refcounts = %v", got)
		}
	})
}

// TestRepublishedItemCountsNothing: an output published again under the same id (a retry) is a no-op, its
// artifacts included.
func TestRepublishedItemCountsNothing(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	l := e.leaseStep("crawl")
	e.done(l, withArtifacts(out("a", `{"score": 1}`), ref("x", shaA)))
	e.redo(l, withArtifacts(out("a", `{"score": 1}`), ref("x", shaA)))
	if got := e.refcounts("listings"); got[shaA] != 2 { // the item on each of listings.raw and listings.audit
		t.Fatalf("refcounts = %v, want 2", got)
	}
}
