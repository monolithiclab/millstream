package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/metrics"
	"github.com/monolithiclab/millstream/internal/store"
)

// /metrics serves the rollups and what the engine measures itself in the Prometheus text format.
func TestPrometheusEndpoint(t *testing.T) {
	f := readFixture(t)
	rec := httptest.NewRecorder()
	f.srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, MetricsPath, nil))
	body := rec.Body.String()
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != prometheusType {
		t.Fatalf("GET /metrics: %d %s\n%s", rec.Code, rec.Header().Get("Content-Type"), body)
	}
	for _, want := range []string{
		"# TYPE millstream_items_in_total counter\n",
		`millstream_items_in_total{pipeline="listings",step="crawl"} 4` + "\n",
		"millstream_review_notifications_failed_total 2\n",
		"# TYPE millstream_writer_batch_size histogram\n",
		`millstream_writer_batch_size_bucket{le="+Inf"} `,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in\n%s", want, body)
		}
	}
	if strings.Contains(body, "millstream_writer_batch_size_count 0\n") {
		t.Error("no writer batch counted")
	}
}

// Leases are counted by pipeline and node, and each eval case shows its latest score.
func TestEngineFamilies(t *testing.T) {
	leases := leasesHeld([]store.Delivery{
		{Pipeline: "p", LeaseOwner: "node-b"}, {Pipeline: "p", LeaseOwner: "node-a"}, {Pipeline: "p", LeaseOwner: "node-b"},
	})
	evals := evalScores([]store.EvalResult{ // newest first
		{Action: "lab/score", Case: "one", Score: 0.75}, {Action: "lab/score", Case: "two", Score: 1},
		{Action: "lab/score", Case: "one", Score: 0.5},
	})
	batches := batchHistogram(store.BatchSizes{Bounds: []int{1, 2}, Counts: []int64{3, 4, 5}, Sum: 9})
	var buf bytes.Buffer
	if err := metrics.Exposition(&buf, nil, []metrics.Family{leases, evals, batches}); err != nil {
		t.Fatal(err)
	}
	want := `# TYPE millstream_eval_score gauge
millstream_eval_score{action="lab/score",case="one"} 0.75
millstream_eval_score{action="lab/score",case="two"} 1
# TYPE millstream_leases_held gauge
millstream_leases_held{node="node-a",pipeline="p"} 1
millstream_leases_held{node="node-b",pipeline="p"} 2
# TYPE millstream_writer_batch_size histogram
millstream_writer_batch_size_bucket{le="1"} 3
millstream_writer_batch_size_bucket{le="2"} 4
millstream_writer_batch_size_bucket{le="+Inf"} 5
millstream_writer_batch_size_sum 9
millstream_writer_batch_size_count 5
`
	if buf.String() != want {
		t.Errorf("exposition =\n%s\nwant\n%s", buf.String(), want)
	}
}
