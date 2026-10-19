//go:build e2e

package api

import (
	"strconv"
	"testing"
	"time"
)

// Variants the e2e build adds: the default build covers the same paths through the other tests.

// Snapshots coalesce: the first event after a quiet spell is sent at once, the next ones at most once per
// SnapshotInterval, whatever the number of events.
func TestLiveSnapshotCoalescing(t *testing.T) {
	e := newLiveEnv(t, BrokerOptions{}, ownFixture)
	sub, first := must2(e.b.SubscribePipeline(e.ctx, "listings"))
	defer sub.Close()
	if first.Pipeline != "listings" || len(first.Steps) != 3 {
		t.Fatalf("first snapshot = %+v", first)
	}
	byStep := map[string]StepSnapshot{}
	for _, st := range first.Steps {
		byStep[st.Step] = st
	}
	if c := byStep["check"]; c.AwaitingReview != 3 {
		t.Errorf("check = %+v", c)
	}
	if s := byStep["score"]; s.ThroughputPerMin == 0 || s.ErrorRate != 1.0/6 || s.P95LatencyMs == 0 {
		t.Errorf("score = %+v", s)
	}
	if _, ok := e.b.nextDue(); ok {
		t.Fatal("a snapshot is due with no event")
	}
	sent := 0
	for i := range 8 { // an event every 125 ms over a second
		e.fire("burst-" + strconv.Itoa(i))
		e.b.pump(e.ctx)
		if due, ok := e.b.nextDue(); ok && !due.After(e.clock.Now()) {
			e.b.flush(e.ctx)
		}
		select {
		case <-sub.C():
			sent++
		default:
		}
		e.clock.Advance(125 * time.Millisecond)
	}
	if sent != 4 { // at 0, 250, 500 and 750 ms
		t.Errorf("%d snapshots in a second of events, want 4", sent)
	}
	// The pipeline stream sends them.
	s := e.open(t, "/api/v1/live/pipelines/listings", nil)
	evs := parseSSE(s.w.waitFor(t, events(1)))
	if evs[0].name != sseSnapshot || evs[0].id != "" {
		t.Errorf("snapshot event = %+v", evs[0])
	}
	e.fire("after")
	e.b.pump(e.ctx)
	e.clock.Advance(time.Second)
	e.b.flush(e.ctx)
	s.w.waitFor(t, events(2))
}
