package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// A step's rate is a token bucket: it starts full, each lease takes a token, it refills continuously up to its
// size, a new rate starts a new bucket, and an invalid rate is logged and leaves the step unlimited.
func TestBucket(t *testing.T) {
	t.Parallel()
	logs := &syncBuffer{}
	e := &Engine{buckets: map[store.StepRef]*bucket{}, log: newLogger(logs)}
	ref := store.StepRef{Pipeline: "p", Step: "a"}
	now := t0
	b := e.bucket(ref, "2/min", now)
	steps := []struct {
		advance time.Duration
		take    int
		want    int // available before taking
	}{
		{0, 2, 2}, {0, 0, 0}, {30 * time.Second, 1, 1}, {10 * time.Second, 0, 0}, {5 * time.Minute, 0, 2},
	}
	for i, s := range steps {
		now = now.Add(s.advance)
		if got := b.available(now); got != s.want {
			t.Fatalf("step %d: %d available, want %d", i, got, s.want)
		}
		for range s.take {
			b.take()
		}
	}
	if e.bucket(ref, "2/min", now) != b {
		t.Fatal("the same rate made a new bucket")
	}
	if nb := e.bucket(ref, "1/h", now); nb == b || nb.available(now) != 1 {
		t.Fatal("a new rate kept the old bucket")
	}
	if e.bucket(ref, "", now) != nil || len(e.buckets) != 0 {
		t.Fatal("an empty rate kept a bucket")
	}
	if e.bucket(ref, "often", now) != nil || !strings.Contains(logs.String(), "invalid step rate") {
		t.Fatalf("invalid rate: logs %s", logs.String())
	}
}
