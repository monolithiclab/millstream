package storetest

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// purge runs a purge to the end as the engine does, without the artifact files: start, erase, finish.
func (e *env) purge(req store.PurgeRequest) (store.Purge, error) {
	p, _, err := e.s.StartPurge(e.ctx, req)
	for err == nil && p.State == store.PurgeErasing {
		p, err = e.s.ErasePurge(e.ctx, req.ID)
	}
	if err == nil && p.State == store.PurgeSweeping {
		p, err = e.s.FinishPurge(e.ctx, req.ID)
	}
	return p, err
}

// item returns an item of a pipeline by id.
func (e *env) item(pipelineName, id string) store.Item {
	e.t.Helper()
	items, err := e.s.Items(e.ctx, store.ItemFilter{Pipeline: pipelineName, IDs: []string{id}})
	if err != nil || len(items) != 1 {
		e.t.Fatalf("item %s of %s: %+v, %v", id, pipelineName, items, err)
	}
	return items[0]
}

// TestPurgeAcrossImports: a purge by key fences and erases the items with the key, their descendants and the copies
// imports made of them down a chain of imports, and nothing else. Fenced, pending deliveries are cancelled and a
// leased one's outputs are discarded when it reports; erased, an item has no payload, key or artifacts, and its
// deliveries' transcripts are gone. Its events never carry the key.
func TestPurgeAcrossImports(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline)
	e.applyVars(dealPipeline, map[string]any{"tag": "y"})
	e.apply(relayPipeline)
	e.firePipeline("src", "go", "r1")
	e.done(e.leaseIn("src", "crawl"), withArtifacts(out("k1", `{"n":1}`), ref("photo", shaA)), out("k2", `{"n":2}`))
	listings := e.itemsIn("src", "r1", "listings")
	a, b := listings[0].ID, listings[1].ID
	score := e.leaseIn("src", "score")
	if _, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: score.Ref(), Attempt: 1,
		Outputs:   []store.Output{out("k1", `{"score":5}`)},
		Artifacts: []store.ArtifactRef{ref("transcript.json", shaB)}}); err != nil {
		t.Fatal(err)
	}
	good := e.itemsIn("src", "r1", "good")[0].ID
	dealRun, approved := ids.ImportRun("approved", "r1"), ids.ImportItem("approved", a)
	notify := e.leaseIn("deal", "notify") // a's copy, leased while the purge fences it
	wantState(t, "leased copy", notify.Item.ID, approved)
	lineage := map[string][]string{
		"src":   {a, good},
		"deal":  {approved, ids.ImportItem("scored", good)},
		"relay": {ids.ImportItem("again", approved)},
	}

	e.fail(e.leaseIn("deal", "file"), true) // its error quoted by a deliveryRetry event, which erasing scrubs
	p, created, err := e.s.StartPurge(e.ctx, store.PurgeRequest{ID: "p1", Key: "k1"})
	if err != nil || !created || p.Items != 5 || p.State != store.PurgeErasing || p.Scope != store.PurgeKey ||
		!slices.Equal(p.Pipelines, []string{"deal", "relay", "src"}) {
		t.Fatalf("StartPurge = %+v, %v, %v", p, created, err)
	}
	for name, its := range lineage {
		for _, id := range its {
			if it := e.item(name, id); it.Tombstone != store.TombstoneErased || string(it.Payload) != "null" {
				t.Fatalf("fenced %s %s = %+v", name, id, it)
			}
		}
	}
	if it := e.item("src", b); it.Tombstone != "" {
		t.Fatalf("k2 fenced: %+v", it)
	}
	// No lease on the lineage: its pending deliveries are cancelled; k2's are still there to lease.
	for _, l := range e.lease(100) {
		if slices.Contains(lineage[l.Pipeline], l.Item.ID) {
			t.Fatalf("leased %s on fenced %s", l.Step, l.Item.ID)
		}
		e.done(l)
	}
	for name, its := range lineage {
		for _, id := range its {
			ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: name, ItemID: id})
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range ds {
				if d.State == store.DeliveryPending {
					t.Fatalf("delivery %s of fenced %s still pending", d.ID, id)
				}
			}
		}
	}
	// The leased copy reports: cancelled, its outputs discarded.
	if st, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: notify.Ref(), Attempt: 1}); err != nil ||
		st != store.DeliveryCancelled {
		t.Fatalf("Done on a fenced item = %s, %v", st, err)
	}
	if again, created, err := e.s.StartPurge(e.ctx, store.PurgeRequest{ID: "p1", Key: "k1"}); err != nil || created ||
		again.Items != 5 {
		t.Fatalf("StartPurge again = %+v, %v, %v", again, created, err)
	}

	p, err = e.purge(store.PurgeRequest{ID: "p1", Key: "k1"})
	if err != nil || p.State != store.PurgeDone || p.Erased != 5 || p.FinishedAt.IsZero() {
		t.Fatalf("purge = %+v, %v", p, err)
	}
	for name, its := range lineage {
		for _, id := range its {
			if it := e.item(name, id); it.Key != "" || it.Tombstone != store.TombstoneErased || len(it.Artifacts) != 0 {
				t.Fatalf("erased %s %s = %+v", name, id, it)
			}
		}
	}
	if rc := e.refcounts("src"); rc[shaA] != 0 || rc[shaB] != 0 {
		t.Fatalf("src refcounts = %v, want the photo and the transcript unreferenced", rc)
	}
	if arts, err := e.s.AttemptArtifacts(e.ctx, "src", score.DeliveryID); err != nil || len(arts) != 0 {
		t.Fatalf("an erased item's transcripts = %+v, %v", arts, err)
	}
	if it := e.item("src", b); it.Key != "k2" || string(it.Payload) != `{"n":2}` {
		t.Fatalf("k2 = %+v", it)
	}
	if r := e.runIn("deal", dealRun); r.State == store.RunCancelled {
		t.Fatalf("the consumer run was cancelled: %+v", r)
	}
	started, completed := 0, 0
	for _, ev := range e.events() {
		if strings.Contains(string(ev.Data), "k1") || strings.Contains(string(ev.Data), "boom") {
			t.Fatalf("event %s carries the key: %s", ev.Kind, ev.Data)
		}
		switch ev.Kind {
		case store.EventPurgeStarted:
			started++
		case store.EventPurgeCompleted:
			completed++
		}
	}
	if started != 3 || completed != 3 {
		t.Fatalf("%d purgeStarted and %d purgeCompleted events, want one per pipeline", started, completed)
	}
	if got, err := e.s.Purge(e.ctx, "p1"); err != nil || got.State != store.PurgeDone {
		t.Fatalf("Purge = %+v, %v", got, err)
	}
	if open, err := e.s.Purges(e.ctx); err != nil || len(open) != 0 {
		t.Fatalf("unfinished purges = %+v, %v", open, err)
	}
	// Erased, the key matches nothing: a purge of it again is done at once.
	if p, err := e.purge(store.PurgeRequest{ID: "p2", Key: "k1"}); err != nil || p.Items != 0 || p.State != store.PurgeDone {
		t.Fatalf("purge again = %+v, %v", p, err)
	}
	// Scoped by pipeline and by item.
	if p, err := e.purge(store.PurgeRequest{ID: "p3", Item: ids.ImportItem("approved", b), Pipeline: "deal"}); err != nil ||
		p.Items != 2 || p.Scope != store.PurgeItem || !slices.Equal(p.Pipelines, []string{"deal", "relay"}) {
		t.Fatalf("purge of a copy = %+v, %v", p, err)
	}
	if it := e.item("src", b); it.Tombstone != "" {
		t.Fatalf("purging a copy erased its producer item: %+v", it)
	}
	for _, req := range []store.PurgeRequest{{Key: "k"}, {ID: "x"}, {ID: "x", Key: "k", Item: "i"}} {
		if _, _, err := e.s.StartPurge(e.ctx, req); !errors.Is(err, store.ErrInvalid) {
			t.Fatalf("StartPurge %+v = %v", req, err)
		}
	}
	if _, err := e.s.Purge(e.ctx, "p_missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Purge of a missing purge = %v", err)
	}
}

// TestPurgeResumesAfterCrash covers review focus 5: a purge across two pipelines linked by an import, crashed at
// each boundary of each of its units, resumes and finishes; once its fence committed, no lease is granted on the
// lineage meanwhile.
func TestPurgeResumesAfterCrash(t *testing.T, open Opener) {
	e := newEnv(t, open, srcPipeline)
	e.applyVars(dealPipeline, map[string]any{"tag": "y"})
	e.firePipeline("src", "go", "r1")
	// The default build crashes after the fence and before the erasure committed; the e2e build at every boundary.
	names, keys := []string{"startPurge.afterCommit", "erasePurge.beforeCommit"}, []string(nil)
	if fullScale {
		names = nil
		for _, unit := range []string{"startPurge", "erasePurge", "finishPurge"} {
			for _, phase := range []string{"beforeCommit", "afterCommit"} {
				names = append(names, unit+"."+phase)
			}
		}
	}
	outs := make([]store.Output, len(names))
	for i := range names {
		keys = append(keys, fmt.Sprintf("k%d", i))
		outs[i] = out(keys[i], `{}`)
	}
	e.done(e.leaseIn("src", "crawl"), outs...)
	byKey := map[string]string{}
	for _, it := range e.itemsIn("src", "r1", "listings") {
		byKey[it.Key] = it.ID
	}
	notifyOnly := map[store.StepRef]int{}
	for p, steps := range importSteps {
		for _, s := range steps {
			if p != "deal" || s != "notify" {
				notifyOnly[store.StepRef{Pipeline: p, Step: s}] = 0
			}
		}
	}
	for i, name := range names {
		t.Run(name, func(t *testing.T) {
			e := e.on(t)
			req := store.PurgeRequest{ID: "p_" + name, Key: keys[i]}
			copyID := ids.ImportItem("approved", byKey[keys[i]])
			reset := failpoint.Set(name)
			_, err := e.purge(req)
			reset()
			if !errors.As(err, new(failpoint.Crash)) {
				t.Fatalf("purge = %v, want the crash at %s", err, name)
			}
			_, perr := e.s.Purge(e.ctx, req.ID)
			fenced := perr == nil
			ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "n", Max: 100, TTL: time.Minute, Slots: notifyOnly})
			if err != nil {
				t.Fatal(err)
			}
			for _, l := range ls {
				if fenced && l.Item.ID == copyID {
					t.Fatalf("leased the fenced copy %s after the crash", copyID)
				}
				e.done(l)
			}
			p, err := e.purge(req)
			if err != nil || p.State != store.PurgeDone || p.Items != 2 || p.Erased != 2 {
				t.Fatalf("resumed purge = %+v, %v", p, err)
			}
			if it := e.item("src", byKey[keys[i]]); it.Tombstone != store.TombstoneErased || it.Key != "" {
				t.Fatalf("producer item = %+v", it)
			}
			if it := e.item("deal", copyID); it.Tombstone != store.TombstoneErased || it.Key != "" {
				t.Fatalf("copy = %+v", it)
			}
		})
	}
}
