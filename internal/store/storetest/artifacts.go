package storetest

import (
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

func TestDoneRefusesInvalidArtifacts(t *testing.T, open Opener) {
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
	e := newEnv(t, open, fixture)
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

// TestAttemptArtifacts: each attempt keeps its own artifacts, a failed one included, apart from the outputs; each
// counts one reference. Invalid or twice-named references are refused whole.
func TestAttemptArtifacts(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
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
