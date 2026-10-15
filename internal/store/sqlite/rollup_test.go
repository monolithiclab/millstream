package sqlite

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/failpoint"
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

// Delivery timings: the queue wait counts from when an attempt is due (backoff excluded), the work from its lease,
// and the latency from creation to terminal; each attempt's end records its share in its event.
func TestDeliveryTimings(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.clock.Advance(2 * time.Second) // queue wait
	l := e.leaseStep("crawl")
	e.clock.Advance(3 * time.Second) // work
	wantState(t, "first attempt", e.fail(l, true), store.DeliveryPending)
	e.clock.Advance(time.Minute) // past the 5s backoff (10s with a jitter of one half), plus 55s of queue wait
	l = e.leaseStep("crawl")
	e.clock.Advance(4 * time.Second)
	e.done(l, out("a", `{"score":80}`))

	d := e.deliveries("r1")[0]
	wantState(t, "wait", d.Wait, 2*time.Second+55*time.Second)
	wantState(t, "work", d.Work, 7*time.Second)
	wantState(t, "created", d.CreatedAt, t0)
	wantState(t, "finished", d.FinishedAt, t0.Add(69*time.Second))
	wantState(t, "latency", d.Latency(time.Time{}), 69*time.Second)

	var data map[string]any
	done := e.eventKinds(store.EventDeliveryDone)[0]
	if err := json.Unmarshal(done.Data, &data); err != nil {
		t.Fatal(err)
	}
	wantState(t, "workMs", data["workMs"], any(float64(4000)))
	wantState(t, "latencyMs", data["latencyMs"], any(float64(69000)))
	retry := e.eventKinds(store.EventDeliveryRetry)[0]
	data = nil
	if err := json.Unmarshal(retry.Data, &data); err != nil {
		t.Fatal(err)
	}
	wantState(t, "retry error", data["error"], any("boom"))
	if _, terminal := data["latencyMs"]; terminal {
		t.Errorf("a retried attempt records a latency: %s", retry.Data)
	}
	// A delivery still pending has no finish, and its latency runs to now.
	pending := e.deliveries("r1")[1]
	if !pending.FinishedAt.IsZero() || pending.Latency(t0.Add(time.Hour)) != time.Hour-69*time.Second {
		t.Errorf("pending delivery: finished %v, latency %v", pending.FinishedAt, pending.Latency(t0.Add(time.Hour)))
	}
}

// A dead-letter replay restarts the delivery's timings.
func TestReplayResetsTimings(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	l := e.leaseStep("crawl")
	e.clock.Advance(time.Second)
	e.fail(l, false)
	e.clock.Advance(time.Hour)
	if n, err := e.s.ReplayDeadLetters(e.ctx, "listings", "r1", nil, false); err != nil || n != 1 {
		t.Fatalf("replay = %d, %v", n, err)
	}
	d := e.deliveries("r1")[0]
	if d.CreatedAt != t0.Add(time.Hour+time.Second) || !d.FinishedAt.IsZero() || d.Wait != 0 || d.Work != 0 {
		t.Fatalf("replayed delivery timings: %+v", d)
	}
}

// The rollup adds each event's points to the rollup of its minute: count, sum, min and max over the minute, a
// boundary starting a new row, and a second pass finding nothing new.
func TestRollupMath(t *testing.T) {
	e := newEnv(t, fixture)
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
func TestDoneRefusesInvalidMetrics(t *testing.T) {
	e := newEnv(t, fixture)
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

// A crash before the rollup commits leaves the cursor where it was: the next pass rolls the same events up once.
func TestRollupFailpoint(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	e.done(e.leaseStep("crawl"), out("a", `{"score":80}`))
	reset := failpoint.Set("rollup.beforeCommit")
	_, err := e.s.Rollup(e.ctx)
	reset()
	if !errors.As(err, new(failpoint.Crash)) {
		t.Fatalf("Rollup = %v, want the failpoint crash", err)
	}
	if got := e.rollups(metrics.ItemsIn, "crawl"); len(got) != 0 {
		t.Fatalf("rollups after a crash before commit: %+v", got)
	}
	e.rollup()
	if got := e.rollups(metrics.ItemsIn, "crawl"); len(got) != 1 || got[0].Count != 1 {
		t.Fatalf("items in after the retried pass: %+v", got)
	}
}

// Rollups and pipeline events are filtered by pipeline, name, kind and time.
func TestMetricAndEventFilters(t *testing.T) {
	e := newEnv(t, fixture, otherPipeline)
	e.fire("r1")
	e.clock.Advance(time.Hour)
	e.firePipeline("other", "daily", "r2")
	e.rollup()
	rs, err := e.s.MetricRollups(e.ctx, store.MetricFilter{Name: metrics.ItemsOut, Since: t0.Add(time.Minute)})
	if err != nil || len(rs) != 1 || rs[0].Pipeline != "other" || rs[0].Labels["stream"] != "trigger.daily" {
		t.Fatalf("rollups since an hour = %+v, %v", rs, err)
	}
	evs, err := e.s.PipelineEvents(e.ctx, store.EventFilter{Pipeline: "listings",
		Kinds: []string{store.EventRunStarted, store.EventItemPublished}})
	if err != nil || len(evs) != 2 || evs[0].Kind != store.EventRunStarted {
		t.Fatalf("listings events = %+v, %v", evs, err)
	}
	evs, err = e.s.PipelineEvents(e.ctx, store.EventFilter{Since: t0.Add(time.Hour)})
	if err != nil || len(evs) == 0 || evs[0].Pipeline != "other" {
		t.Fatalf("events since an hour = %+v, %v", evs, err)
	}
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Since: t0.Add(time.Minute)})
	if err != nil || len(ds) != 1 || ds[0].Pipeline != "other" {
		t.Fatalf("deliveries since an hour = %+v, %v", ds, err)
	}
}
