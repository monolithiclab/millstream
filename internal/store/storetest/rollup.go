package storetest

import (
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/metrics"
	"github.com/monolithiclab/millstream/internal/store"
)

// rollups returns the rollups of a metric for a step, by minute.
func (e *env) rollups(name, step string) []store.MetricRollup {
	e.t.Helper()
	rs, err := e.s.MetricRollups(e.ctx, store.MetricFilter{Pipeline: "listings", Name: name})
	if err != nil {
		e.t.Fatalf("MetricRollups: %v", err)
	}
	var out []store.MetricRollup
	for _, r := range rs {
		if r.Labels["step"] == step {
			out = append(out, r)
		}
	}
	return out
}

func (e *env) rollup() int {
	e.t.Helper()
	n, err := e.s.Rollup(e.ctx)
	if err != nil {
		e.t.Fatalf("Rollup: %v", err)
	}
	return n
}

// The rollup adds each event's points to the rollup of its minute: count, sum, min and max over the minute, a
// boundary starting a new row, and a second pass finding nothing new.
func TestRollupMath(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.clock.Advance(50 * time.Second) // 06:00:50
	e.fire("r1")
	l := e.leaseStep("crawl")
	e.clock.Advance(2 * time.Second) // 06:00:52
	if _, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1, CostUSD: 0.25,
		Outputs: []store.Output{out("a", `{"score":80}`), out("b", `{"score":10}`)},
		Metrics: []store.Metric{
			{Name: "pages", Value: 3, Labels: map[string]string{"source": "x"}},
			{Name: "pages", Value: 7, Labels: map[string]string{"source": "x"}},
			{Name: metrics.ItemsOut, Value: 1}, // an engine name: kept apart
		}}); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(10 * time.Second) // 06:01:02: the next minute
	for range 2 {
		l := e.leaseStep("score")
		e.clock.Advance(time.Second)
		e.done(l, out("", `{"score":80}`))
	}
	if n := e.rollup(); n == 0 {
		t.Fatal("rollup read no event")
	}
	minute0, minute1 := t0, t0.Add(time.Minute)

	pages := e.rollups("pages", "crawl")
	if len(pages) != 1 || pages[0].Count != 2 || pages[0].Sum != 10 || pages[0].Min != 3 || pages[0].Max != 7 ||
		pages[0].Labels["source"] != "x" || !pages[0].Minute.Equal(minute0) {
		t.Fatalf("pages = %+v", pages)
	}
	if got := e.rollups("action_items_out", "crawl"); len(got) != 1 || got[0].Sum != 1 {
		t.Errorf("an action metric named like an engine metric: %+v", got)
	}
	if got := e.rollups(metrics.DeliveryCost, "crawl"); len(got) != 1 || got[0].Sum != 0.25 {
		t.Errorf("cost = %+v", got)
	}
	work := e.rollups(metrics.DeliveryWork, "score")
	if len(work) != 1 || work[0].Count != 2 || work[0].Sum != 2 || !work[0].Minute.Equal(minute1) {
		t.Errorf("score work = %+v", work)
	}
	in := e.rollups(metrics.ItemsIn, "score")
	if len(in) != 1 || in[0].Count != 2 {
		t.Errorf("score items in = %+v", in)
	}
	latency := e.rollups(metrics.DeliveryLatency, "score")
	if len(latency) != 1 || latency[0].Min != 11 || latency[0].Max != 12 {
		t.Errorf("score latency = %+v", latency) // created at 06:00:52, done at 06:01:03 and 06:01:04
	}
	buckets := map[string]int64{}
	for _, b := range e.rollups(metrics.DeliveryLatency+metrics.BucketSuffix, "score") {
		buckets[b.Labels["le"]] = b.Count
	}
	if buckets["10"] != 0 || buckets["30"] != 2 || buckets["+Inf"] != 2 {
		t.Errorf("latency buckets = %v", buckets)
	}

	// Idempotence: nothing new, nothing added; the queue gauges are sampled at every pass.
	before := e.rollups(metrics.DeliveryWork, "score")
	if n := e.rollup(); n != 0 {
		t.Fatalf("second pass read %d events", n)
	}
	if after := e.rollups(metrics.DeliveryWork, "score"); after[0].Count != before[0].Count || after[0].Sum != before[0].Sum {
		t.Fatalf("work after a second pass = %+v, want %+v", after, before)
	}
	depth := e.rollups(metrics.QueueDepth, "report")
	if len(depth) != 1 || depth[0].Count != 2 || depth[0].Max != 2 {
		t.Errorf("report queue depth = %+v", depth) // two passes, two deliveries due
	}
	if got := e.rollups(metrics.QueueDepth, "alert"); len(got) != 1 || got[0].Max != 0 {
		t.Errorf("an empty queue samples zero: %+v", got)
	}

	// A later pass in the same minute merges into its rows: a third sample of the report queue, now one deep.
	e.done(e.leaseStep("report"))
	e.rollup()
	depth = e.rollups(metrics.QueueDepth, "report")
	if len(depth) != 1 || depth[0].Count != 3 || depth[0].Sum != 5 || depth[0].Min != 1 || depth[0].Max != 2 {
		t.Errorf("report queue depth after a merge = %+v", depth)
	}
}

// Invalid metrics are refused before the unit runs.
func TestDoneRefusesInvalidMetrics(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	e.fire("r1")
	l := e.leaseStep("crawl")
	for _, m := range []store.Metric{{Name: "", Value: 1}, {Name: "x", Value: nanValue()}} {
		_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1, Metrics: []store.Metric{m}})
		wantErr(t, err, store.ErrInvalid)
		_, err = e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: 1, Metrics: []store.Metric{m}})
		wantErr(t, err, store.ErrInvalid)
	}
	many := make([]store.Metric, store.MaxAttemptMetrics+1)
	for i := range many {
		many[i] = store.Metric{Name: "m", Value: 1}
	}
	_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: 1, Metrics: many})
	wantErr(t, err, store.ErrInvalid)
}

func nanValue() float64 {
	zero := 0.0
	return zero / zero
}
