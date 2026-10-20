package insights

import (
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// The rest of a review step's delivery latency is time waiting on people, not backoff; another step's rest is
// backoff.
func TestCriticalPathReview(t *testing.T) {
	t0 := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	score := store.Delivery{ID: ids.Delivery("score", "it_0", 0), Step: "score", ItemID: "it_0", State: store.DeliveryDone,
		CreatedAt: t0, FinishedAt: t0.Add(10 * time.Second), Wait: time.Second, Work: 4 * time.Second}
	review := store.Delivery{ID: "dl_review", Step: "check", ItemID: "it_1", State: store.DeliveryDone,
		CreatedAt: t0.Add(10 * time.Second), FinishedAt: t0.Add(70 * time.Second), Wait: time.Second}
	items := []store.Item{{ID: "it_1", Step: "score", Parents: []string{"it_0"}}}
	p := CriticalPath([]store.Delivery{score, review}, items, map[string]bool{"check": true}, t0.Add(time.Hour))
	if len(p.Steps) != 2 || p.Total != 70*time.Second {
		t.Fatalf("path = %+v", p)
	}
	if s := p.Steps[0]; s.Step != "score" || s.Backoff != 5*time.Second || s.Review != 0 {
		t.Errorf("score = %+v, want 5s of backoff", s)
	}
	if s := p.Steps[1]; s.Step != "check" || s.Review != 59*time.Second || s.Backoff != 0 {
		t.Errorf("review = %+v, want 59s of review", s)
	}
}
