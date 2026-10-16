package sqlite

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

var (
	shaA = strings.Repeat("a", 64)
	shaB = strings.Repeat("b", 64)
)

func ref(name, sha string) store.ArtifactRef {
	return store.ArtifactRef{Name: name, SHA256: sha, Size: 12, Type: "text/plain; charset=utf-8"}
}

func withArtifacts(o store.Output, refs ...store.ArtifactRef) store.Output {
	o.Artifacts = refs
	return o
}

func (e *env) refcounts(pipelineName string) map[string]int64 {
	e.t.Helper()
	arts, err := e.s.Artifacts(e.ctx, pipelineName)
	if err != nil {
		e.t.Fatalf("Artifacts: %v", err)
	}
	out := map[string]int64{}
	for _, a := range arts {
		out[a.SHA256] = a.Refcount
		if a.Size != 12 || a.Type != "text/plain; charset=utf-8" || a.Pipeline != pipelineName {
			e.t.Fatalf("artifact entry %+v", a)
		}
	}
	return out
}

// TestDoneCountsArtifactsPerItem: each published item counts one reference to each artifact it carries; crawl
// publishes every output on two streams, so two items reference each artifact per output.
func TestDoneCountsArtifactsPerItem(t *testing.T) {
	e := newEnv(t, fixture)
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

func TestDoneRefusesInvalidArtifacts(t *testing.T) {
	tests := map[string]store.ArtifactRef{
		"empty name":      ref("", shaA),
		"path as name":    ref("out/x.txt", shaA),
		"dot dot":         ref("..", shaA),
		"backslash":       ref(`a\b`, shaA),
		"short hash":      ref("x", "abc"),
		"uppercase hash":  ref("x", strings.Repeat("A", 64)),
		"no type":         {Name: "x", SHA256: shaA},
		"negative size":   {Name: "x", SHA256: shaA, Size: -1, Type: "t"},
		"name too long":   ref(strings.Repeat("n", 256), shaA),
		"nul in the name": ref("a\x00b", shaA),
	}
	// A refused Done leaves the lease held: every case reports on the same one.
	e := newEnv(t, fixture)
	e.fire("r1")
	l := e.leaseStep("crawl")
	for name, a := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1,
				Outputs: []store.Output{withArtifacts(out("", `{}`), a)}})
			wantErr(t, err, store.ErrInvalid)
		})
	}
	wantState(t, "delivery", e.deliveries("r1")[0].State, store.DeliveryLeased)
}

// TestArtifactsTravelWithItems: a dedupe pass republishes the item with its artifacts, a dead input's dead letter
// copies them, and an unrouted output keeps them for its replay.
func TestArtifactsTravelWithItems(t *testing.T) {
	t.Run("dedupe", func(t *testing.T) {
		e := newEnv(t, dedupePipeline)
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
	listings := newEnv(t, fixture)
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

// TestAttemptArtifacts: each attempt keeps its own artifacts, a failed one included, apart from the outputs; each
// counts one reference. Invalid or twice-named references are refused whole.
func TestAttemptArtifacts(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	l := e.leaseStep("crawl")
	if _, err := e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: 1, Class: "action", Message: "bad",
		Retryable: true, Artifacts: []store.ArtifactRef{ref("transcript.json", shaA)}}); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	e.clock.Advance(time.Hour)
	l = e.leaseStep("crawl")
	if _, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 2,
		Outputs:   []store.Output{out("", `{"score": 1}`)},
		Artifacts: []store.ArtifactRef{ref("transcript.json", shaB)}}); err != nil {
		t.Fatalf("Done: %v", err)
	}
	got, err := e.s.AttemptArtifacts(e.ctx, "listings", l.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	want := []store.AttemptArtifact{{Attempt: 1, ArtifactRef: ref("transcript.json", shaA)},
		{Attempt: 2, ArtifactRef: ref("transcript.json", shaB)}}
	if !slices.Equal(got, want) {
		t.Fatalf("attempt artifacts = %+v, want %+v", got, want)
	}
	if got := e.refcounts("listings"); got[shaA] != 1 || got[shaB] != 1 {
		t.Fatalf("refcounts = %v", got)
	}
	if raw := e.items("listings.raw"); len(raw) != 1 || len(raw[0].Artifacts) != 0 {
		t.Fatalf("the output carries the attempt's artifacts: %+v", raw)
	}

	e.fire("r2")
	l = e.leaseStep("crawl")
	for _, refs := range [][]store.ArtifactRef{{ref("../t", shaA)}, {ref("t", shaA), ref("t", shaB)}} {
		_, err = e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: 1, Class: "action", Artifacts: refs})
		wantErr(t, err, store.ErrInvalid)
		_, err = e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1, Artifacts: refs})
		wantErr(t, err, store.ErrInvalid)
	}
}
