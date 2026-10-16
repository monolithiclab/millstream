package store

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestDeliveryStateTerminal(t *testing.T) {
	t.Parallel()
	tests := map[DeliveryState]bool{
		DeliveryPending: false, DeliveryLeased: false, DeliveryAwaitingReview: false,
		DeliveryDone: true, DeliveryFiltered: true, DeliveryDead: true, DeliveryCancelled: true,
	}
	for state, want := range tests {
		if got := state.Terminal(); got != want {
			t.Errorf("%s.Terminal() = %v, want %v", state, got, want)
		}
	}
}

func TestLeaseRef(t *testing.T) {
	t.Parallel()
	l := Lease{Pipeline: "p", DeliveryID: "dl_1", FencingToken: 4, RunID: "r"}
	if got, want := l.Ref(), (LeaseRef{Pipeline: "p", DeliveryID: "dl_1", FencingToken: 4}); got != want {
		t.Errorf("Ref() = %+v, want %+v", got, want)
	}
}

// Retention passes add up; More stays the caller's to read from each pass.
func TestRetentionResultAdd(t *testing.T) {
	t.Parallel()
	r := RetentionResult{Items: 1, Events: 2, Transcripts: 3, Rollups: 4}
	r.Add(RetentionResult{Items: 10, Events: 20, Transcripts: 30, Rollups: 40, More: true})
	if want := (RetentionResult{Items: 11, Events: 22, Transcripts: 33, Rollups: 44}); r != want {
		t.Errorf("Add = %+v, want %+v", r, want)
	}
}

func TestRevisionDecode(t *testing.T) {
	t.Parallel()
	r := Revision{ID: 3, PipelineJSON: []byte(`{"Name":"listings","Vars":{"maxPrice":400000}}`)}
	p, err := r.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "listings" || p.Vars["maxPrice"] != int64(400000) {
		t.Errorf("decoded %+v", p)
	}
	if _, err := (Revision{ID: 4, PipelineJSON: []byte("{")}).Decode(); err == nil || errors.Unwrap(err) == nil {
		t.Errorf("Decode of bad JSON = %v", err)
	}
}

// ValidArtifact refuses a name with a path, a SHA-256 that is not lowercase hex, and a missing size or type.
func TestValidArtifact(t *testing.T) {
	sum := strings.Repeat("ab", 32)
	for _, tt := range []struct {
		a    ArtifactRef
		want string
	}{
		{ArtifactRef{Name: "page.html", SHA256: sum, Size: 3, Type: "text/html"}, ""},
		{ArtifactRef{Name: "../page.html", SHA256: sum, Size: 3, Type: "text/html"}, "without a path separator"},
		{ArtifactRef{Name: "..", SHA256: sum, Size: 3, Type: "text/html"}, "without a path separator"},
		{ArtifactRef{Name: "p", SHA256: strings.ToUpper(sum), Size: 3, Type: "text/html"}, "not a hex SHA-256"},
		{ArtifactRef{Name: "p", SHA256: "ab", Size: 3, Type: "text/html"}, "not a hex SHA-256"},
		{ArtifactRef{Name: "p", SHA256: sum, Size: -1, Type: "text/html"}, "no size or type"},
		{ArtifactRef{Name: "p", SHA256: sum}, "no size or type"},
	} {
		err := ValidArtifact(tt.a)
		if tt.want == "" && err != nil || tt.want != "" && (!errors.Is(err, ErrInvalid) ||
			!strings.Contains(err.Error(), tt.want)) {
			t.Errorf("ValidArtifact(%+v) = %v, want %q", tt.a, err, tt.want)
		}
	}
}

// ValidMetrics refuses more than MaxAttemptMetrics, a metric without a name and one without a finite value.
func TestValidMetrics(t *testing.T) {
	if err := ValidMetrics([]Metric{{Name: "cost", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	for _, ms := range [][]Metric{
		make([]Metric, MaxAttemptMetrics+1), {{Value: 1}}, {{Name: "n", Value: math.NaN()}},
		{{Name: "n", Value: math.Inf(1)}},
	} {
		if err := ValidMetrics(ms); !errors.Is(err, ErrInvalid) {
			t.Errorf("ValidMetrics(%d metrics) = %v", len(ms), err)
		}
	}
}

// A delivery's wait, work and latency as of now: a due pending delivery waits on, a leased one works on, and a
// terminal one's latency ends when it finished.
func TestDeliveryTimes(t *testing.T) {
	t0 := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	now := t0.Add(time.Minute)
	pending := Delivery{State: DeliveryPending, NextAttemptAt: t0.Add(20 * time.Second), Wait: time.Second,
		CreatedAt: t0}
	if pending.WaitAt(now) != 41*time.Second || pending.WorkAt(now) != 0 || pending.Latency(now) != time.Minute {
		t.Errorf("pending: wait %v, work %v, latency %v", pending.WaitAt(now), pending.WorkAt(now), pending.Latency(now))
	}
	leased := Delivery{State: DeliveryLeased, LeasedAt: t0.Add(30 * time.Second), Wait: time.Second,
		Work: time.Second, CreatedAt: t0}
	if leased.WaitAt(now) != time.Second || leased.WorkAt(now) != 31*time.Second {
		t.Errorf("leased: wait %v, work %v", leased.WaitAt(now), leased.WorkAt(now))
	}
	done := Delivery{State: DeliveryDone, CreatedAt: t0, FinishedAt: t0.Add(10 * time.Second)}
	if done.Latency(now) != 10*time.Second {
		t.Errorf("done: latency %v", done.Latency(now))
	}
	if !(PipelineState{Disabled: true}).Holds() || (PipelineState{}).Holds() {
		t.Error("Holds")
	}
	if (LeaseRef{Pipeline: "p", DeliveryID: "d"}).Key() != (DeliveryKey{"p", "d"}) {
		t.Error("Key")
	}
}
