package storetest

import (
	"cmp"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// retain runs one retention pass with a day of retention on the streams given of listings.
func (e *env) retain(req store.RetentionRequest, streams ...string) store.RetentionResult {
	e.t.Helper()
	for _, s := range streams {
		req.Streams = append(req.Streams, store.StreamRetention{Pipeline: "listings", Stream: s, Age: 24 * time.Hour})
	}
	res, err := e.s.Retain(e.ctx, req)
	if err != nil {
		e.t.Fatalf("Retain: %v", err)
	}
	return res
}

// itemByKey returns the item of a stream of listings with key.
func (e *env) itemByKey(stream, key string) store.Item {
	e.t.Helper()
	for _, it := range e.items(stream) {
		if it.Key == key {
			return it
		}
	}
	e.t.Fatalf("no item %s on %s", key, stream)
	return store.Item{}
}

// TestRetainKeepsWhatUnfinishedWorkNeeds covers review focus 4: retention tombstones the items of a stream older
// than its retention (payload null, artifacts dropped, envelope and key kept), but never one with a delivery not
// terminal, an open dead letter, or a dead delivery whose dead letter is open; once that work is resolved, the next
// pass takes them.
func TestRetainKeepsWhatUnfinishedWorkNeeds(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), withArtifacts(out("a", `{"n":1}`), ref("a.txt", shaA)), out("c", `{"n":3}`),
		out("d", `{"n":4}`), out("b", `{"n":2}`)) // b last: its score stays pending
	for _, key := range []string{"a", "c", "d"} {
		l := e.leaseStep("score")
		wantState(t, "score key", l.Item.Key, key)
		switch key {
		case "a":
			e.done(l, out("a", `{"score":90}`))
		case "c":
			e.fail(l, false) // dead, its dead letter open
		case "d":
			e.done(l, out("d", `{"score":-1}`)) // unrouted: an open dead letter of its own; d's delivery is done
		}
	}
	for range 2 {
		e.done(e.leaseStep("alert")) // the dead letters' own deliveries: only their open state keeps them
	}
	e.clock.Advance(25 * time.Hour)

	res := e.retain(store.RetentionRequest{}, "listings.raw", "score.dlq")
	if res.Items != 2 || res.More {
		t.Fatalf("first pass = %+v, want a and d", res)
	}
	for key, want := range map[string]store.Tombstone{"a": store.TombstoneExpired, "b": "", "c": "",
		"d": store.TombstoneExpired} {
		it := e.itemByKey("listings.raw", key)
		if it.Tombstone != want || (want != "") != (string(it.Payload) == "null") || it.Key != key {
			t.Fatalf("item %s = %+v, want tombstone %q", key, it, want)
		}
	}
	if a := e.itemByKey("listings.raw", "a"); len(a.Artifacts) != 0 {
		t.Fatalf("an expired item kept its artifacts: %+v", a.Artifacts)
	}
	if rc := e.refcounts("listings")[shaA]; rc != 1 { // the audit copy of a still holds it
		t.Fatalf("refcount of a's artifact = %d, want 1", rc)
	}
	for _, dl := range e.items("score.dlq") {
		if dl.Tombstone != "" {
			t.Fatalf("an open dead letter expired: %+v", dl)
		}
	}
	if evs := e.eventKinds(store.EventItemsExpired); len(evs) != 1 || string(evs[0].Data) != `{"items":2,"stream":"listings.raw"}` {
		t.Fatalf("itemsExpired events = %+v", evs)
	}
	if again := e.retain(store.RetentionRequest{}, "listings.raw", "score.dlq"); again.Items != 0 {
		t.Fatalf("a pass repeated expired %d more", again.Items)
	}

	// Resolved, the rest goes: b scored, both dead letters discarded. The limit bounds a pass.
	e.done(e.leaseStep("score"), out("b", `{"score":80}`))
	var letters []string
	for _, dl := range e.deadLetters("r1") {
		letters = append(letters, dl.ID)
	}
	if n, err := e.s.DiscardDeadLetters(e.ctx, "listings", letters); err != nil || n != 2 {
		t.Fatalf("discard = %d, %v", n, err)
	}
	if res := e.retain(store.RetentionRequest{Limit: 3}, "listings.raw", "score.dlq"); res.Items != 3 || !res.More {
		t.Fatalf("limited pass = %+v", res)
	}
	if res := e.retain(store.RetentionRequest{Limit: 3}, "listings.raw", "score.dlq"); res.Items != 1 || res.More {
		t.Fatalf("last pass = %+v", res)
	}
	// Nothing newer than the retention goes.
	if good := e.items("listings.good"); len(good) != 2 || good[0].Tombstone != "" {
		t.Fatalf("good = %+v", good)
	}
	for _, bad := range []store.RetentionRequest{{Streams: []store.StreamRetention{{Pipeline: "listings"}}}, {Limit: -1}} {
		if _, err := e.s.Retain(e.ctx, bad); err == nil {
			t.Fatalf("Retain %+v was accepted", bad)
		}
	}
	if n, err := e.s.DeleteArtifacts(e.ctx, nil); err != nil || n != 0 {
		t.Fatalf("DeleteArtifacts of nothing = %d, %v", n, err)
	}
}

// TestRetainEventsTranscriptsRollups: events go by age once every projection has read them, never the last one, so
// the sequence carries on; attempt artifacts go by age but those of a live delivery; rollups go by their minute.
// DeleteArtifacts then removes the entries nothing references. Webhook trigger tokens are no record retention ages.
func TestRetainEventsTranscriptsRollups(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.createToken("listings", "hook")
	e.fire("r1")
	crawl := e.leaseStep("crawl")
	if _, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: crawl.Ref(), Attempt: 1,
		Outputs: []store.Output{out("a", `{}`)}, Artifacts: []store.ArtifactRef{ref("transcript.json", shaA)}}); err != nil {
		t.Fatal(err)
	}
	score := e.leaseStep("score")
	if _, err := e.s.Fail(e.ctx, store.FailRequest{LeaseRef: score.Ref(), Attempt: 1, Class: "test", Message: "boom",
		Retryable: true, Artifacts: []store.ArtifactRef{ref("transcript.json", shaB)}}); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(25 * time.Hour)
	all := store.RetentionRequest{Events: 24 * time.Hour, Transcripts: 24 * time.Hour, Rollups: 24 * time.Hour}

	// No projection has read the events yet: they all stay.
	if res := e.retain(all); res.Events != 0 || res.Transcripts != 1 || res.Rollups != 0 {
		t.Fatalf("pass before any rollup = %+v", res)
	}
	if rc := e.refcounts("listings"); rc[shaA] != 0 || rc[shaB] != 1 {
		t.Fatalf("refcounts = %v, want the done delivery's transcript unreferenced and the live one's kept", rc)
	}
	arts, err := e.s.AttemptArtifacts(e.ctx, "listings", score.DeliveryID)
	if err != nil || len(arts) != 1 {
		t.Fatalf("live delivery's attempt artifacts = %+v, %v", arts, err)
	}
	if n, err := e.s.DeleteArtifacts(e.ctx, []store.ArtifactKey{{Pipeline: "listings", SHA256: shaA},
		{Pipeline: "listings", SHA256: shaB}}); err != nil || n != 1 {
		t.Fatalf("DeleteArtifacts = %d, %v, want the unreferenced one", n, err)
	}
	if rc := e.refcounts("listings"); len(rc) != 1 || rc[shaB] != 1 {
		t.Fatalf("artifact entries after the sweep = %v", rc)
	}

	e.rollup()
	before := e.events()
	last := before[len(before)-1].Seq
	if len(e.rollups("items_out", "crawl")) == 0 {
		t.Fatal("no rollup to age")
	}
	res := e.retain(all)
	if res.Events != len(before)-1 || res.Rollups == 0 {
		t.Fatalf("pass after the rollup = %+v, want every event but the last (%d)", res, len(before)-1)
	}
	if evs := e.events(); len(evs) != 1 || evs[0].Seq != last {
		t.Fatalf("events left = %+v, want the last, %d", evs, last)
	}
	if rs := e.rollups("items_out", "crawl"); len(rs) != 0 {
		t.Fatalf("rollups left = %+v", rs)
	}
	e.wantToken("listings", "hook")
	e.fire("r2") // numbered on from the last event kept
	if evs := e.events(); evs[1].Seq != last+1 {
		t.Fatalf("next event seq = %d, want %d", evs[1].Seq, last+1)
	}
}

// createToken stores a webhook trigger's token hash, hash-<trigger>, as an apply does.
func (e *env) createToken(pipelineName, trigger string) {
	e.t.Helper()
	if _, err := e.s.CreateTriggerTokens(e.ctx, pipelineName, map[string]string{trigger: "hash-" + trigger}); err != nil {
		e.t.Fatal(err)
	}
}

// wantToken checks that a webhook trigger's token hash is still the one createToken stored.
func (e *env) wantToken(pipelineName, trigger string) {
	e.t.Helper()
	if h, err := e.s.TriggerTokenHash(e.ctx, pipelineName, trigger); err != nil || h != "hash-"+trigger {
		e.t.Fatalf("token of %s/%s = %q, %v", pipelineName, trigger, h, err)
	}
}

// allRollups returns every rollup the store holds.
func (e *env) allRollups() []store.MetricRollup {
	e.t.Helper()
	rs, err := e.s.MetricRollups(e.ctx, store.MetricFilter{})
	if err != nil {
		e.t.Fatalf("MetricRollups: %v", err)
	}
	return rs
}

// totals returns the store's metric totals.
func (e *env) totals() []store.MetricTotal {
	e.t.Helper()
	ts, err := e.s.MetricTotals(e.ctx)
	if err != nil {
		e.t.Fatalf("MetricTotals: %v", err)
	}
	return ts
}

// sortedTotals orders totals as the store returns them: by pipeline, name and labels.
func sortedTotals(ts []store.MetricTotal) []store.MetricTotal {
	slices.SortFunc(ts, func(a, b store.MetricTotal) int {
		return cmp.Or(cmp.Compare(a.Pipeline, b.Pipeline), cmp.Compare(a.Name, b.Name),
			cmp.Compare(labelsKey(a.Labels), labelsKey(b.Labels)))
	})
	return ts
}

// labelsKey is a label set as the stores order it: JSON, keys sorted.
func labelsKey(labels map[string]string) string {
	if labels == nil {
		labels = map[string]string{}
	}
	body, _ := json.Marshal(labels) // a map of strings always marshals
	return string(body)
}
