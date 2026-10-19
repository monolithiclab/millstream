package storetest

import (
	"errors"
	"fmt"
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
