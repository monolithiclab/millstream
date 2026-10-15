package sqlite

import (
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// crawl retries with backoff 10s capped at 30s and three attempts; the jitter is one half.
func TestFailRetrySchedule(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	waits := []time.Duration{5 * time.Second, 10 * time.Second} // half of 10s, then half of 20s
	for attempt, wait := range waits {
		l := e.lease(1)[0]
		wantState(t, "attempt", l.Attempt, attempt+1)
		wantState(t, "state", e.fail(l, true), store.DeliveryPending)
		d := e.deliveries("r1")[0]
		if d.State != store.DeliveryPending || d.Attempt != attempt+2 || !d.NextAttemptAt.Equal(e.clock.Now().Add(wait)) ||
			d.ErrorClass != "test" || d.ErrorMessage != "boom" {
			t.Fatalf("after attempt %d: %+v; want pending, next attempt in %v", attempt+1, d, wait)
		}
		e.clock.Advance(wait)
	}
	l := e.lease(1)[0]
	wantState(t, "last attempt", l.Attempt, 3)
	wantState(t, "state", e.fail(l, true), store.DeliveryDead)
	if n := len(e.eventKinds(store.EventDeliveryRetry)); n != 2 {
		t.Fatalf("%d deliveryRetry events, want 2", n)
	}
	if dls := e.deadLetters("r1"); len(dls) != 1 || dls[0].Error != "test: boom" {
		t.Fatalf("dead letters = %+v", dls)
	}
}

func TestFailNonRetryableIsDeadAtOnce(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	wantState(t, "state", e.fail(e.lease(1)[0], false), store.DeliveryDead)
	if d := e.deliveries("r1")[0]; d.Attempt != 1 {
		t.Fatalf("attempt %d", d.Attempt)
	}
}

func TestFailTruncatesMessage(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	l := e.lease(1)[0]
	long := strings.Repeat("é", store.MaxErrorMessage) // two bytes each
	if _, err := e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: 1, Message: long, CostUSD: 1.5}); err != nil {
		t.Fatal(err)
	}
	d := e.deliveries("r1")[0]
	if len(d.ErrorMessage) != store.MaxErrorMessage || d.ErrorMessage != long[:store.MaxErrorMessage] || d.CostUSD != 1.5 {
		t.Fatalf("message %d bytes, cost %v", len(d.ErrorMessage), d.CostUSD)
	}
	// Without a class, the dead letter carries the bare message.
	if dls := e.deadLetters("r1"); dls[0].Error != d.ErrorMessage {
		t.Fatal("dead-letter error differs from the delivery's")
	}
}

func Test_truncate(t *testing.T) {
	tests := []struct {
		in   string
		n    int
		want string
	}{
		{"short", 10, "short"},
		{"abcdef", 3, "abc"},
		{"aé", 2, "a"}, // é is two bytes: never split
		{"éé", 3, "é"},
	}
	for _, tt := range tests {
		if got := truncate(tt.in, tt.n); got != tt.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.in, tt.n, got, tt.want)
		}
	}
}

func Test_backoff(t *testing.T) {
	r := pipeline.Retry{MaxAttempts: 10, Backoff: 10 * time.Second, MaxBackoff: time.Minute}
	tests := []struct {
		attempt int
		jitter  float64
		want    time.Duration
	}{
		{1, 1, 10 * time.Second},
		{2, 1, 20 * time.Second},
		{3, 1, 40 * time.Second},
		{4, 1, time.Minute}, // capped
		{60, 1, time.Minute},
		{3, 0.25, 10 * time.Second},
		{1, 0, 0},
	}
	for _, tt := range tests {
		if got := backoff(r, tt.attempt, tt.jitter); got != tt.want {
			t.Errorf("backoff(attempt %d, jitter %v) = %v, want %v", tt.attempt, tt.jitter, got, tt.want)
		}
	}
}
