package api

import (
	"bytes"
	"cmp"
	"net/http"
	"slices"
	"strconv"

	"github.com/monolithiclab/millstream/internal/metrics"
	"github.com/monolithiclab/millstream/internal/store"
)

// prometheusType is the content type of the Prometheus text exposition.
const prometheusType = "text/plain; version=0.0.4; charset=utf-8"

// batchSizer is a store that counts its writer's batch sizes (the SQL stores).
type batchSizer interface {
	WriterBatches() store.BatchSizes
}

// prometheus serves /metrics (spec section 10.2): every rollup series' running totals, gauges only while current,
// and the metrics the engine measures itself: leases held by node, the writer's batch sizes, failed review
// notifications and each eval case's latest score.
func (s *Server) prometheus(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	totals, err := s.opts.Store.MetricTotals(ctx)
	if err != nil {
		return err
	}
	leased, err := s.opts.Store.Deliveries(ctx, store.DeliveryFilter{State: store.DeliveryLeased})
	if err != nil {
		return err
	}
	evals, err := s.opts.Store.EvalResults(ctx, "")
	if err != nil {
		return err
	}
	families := []metrics.Family{leasesHeld(leased), evalScores(evals)}
	if s.opts.NotificationFailures != nil {
		families = append(families, metrics.Family{Name: metrics.NotificationsFailed, Kind: metrics.Counter,
			Samples: []metrics.Sample{{Value: float64(s.opts.NotificationFailures())}}})
	}
	if b, ok := s.opts.Store.(batchSizer); ok {
		families = append(families, batchHistogram(b.WriterBatches()))
	}
	var buf bytes.Buffer
	if err := metrics.Exposition(&buf, metrics.Current(totals, s.opts.Clock.Now()), families, s.log); err != nil {
		return err
	}
	w.Header().Set("Content-Type", prometheusType)
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(buf.Bytes()) // a client gone mid-body has nothing to be told
	return nil
}

// leasesHeld counts the leased deliveries by pipeline and owner.
func leasesHeld(leased []store.Delivery) metrics.Family {
	type key struct{ pipeline, node string }
	held := map[key]int{}
	for _, d := range leased {
		held[key{d.Pipeline, d.LeaseOwner}]++
	}
	f := metrics.Family{Name: metrics.LeasesHeld, Kind: metrics.Gauge}
	for k, n := range held {
		f.Samples = append(f.Samples, metrics.Sample{Labels: map[string]string{"pipeline": k.pipeline, "node": k.node},
			Value: float64(n)})
	}
	sortSamples(f.Samples, "pipeline", "node")
	return f
}

// evalScores keeps each action's case's latest score; results come newest first.
func evalScores(results []store.EvalResult) metrics.Family {
	f := metrics.Family{Name: metrics.EvalScore, Kind: metrics.Gauge}
	seen := map[[2]string]bool{}
	for _, r := range results {
		k := [2]string{r.Action, r.Case}
		if seen[k] {
			continue
		}
		seen[k] = true
		f.Samples = append(f.Samples, metrics.Sample{Labels: map[string]string{"action": r.Action, "case": r.Case},
			Value: r.Score})
	}
	sortSamples(f.Samples, "action", "case")
	return f
}

// sortSamples orders samples by the labels named, in turn.
func sortSamples(ss []metrics.Sample, labels ...string) {
	slices.SortFunc(ss, func(a, b metrics.Sample) int {
		for _, l := range labels {
			if c := cmp.Compare(a.Labels[l], b.Labels[l]); c != 0 {
				return c
			}
		}
		return 0
	})
}

// batchHistogram is the writer's batch sizes as a histogram family.
func batchHistogram(b store.BatchSizes) metrics.Family {
	f := metrics.Family{Name: metrics.WriterBatchSize, Kind: metrics.Histogram}
	for i, n := range b.Counts {
		le := "+Inf"
		if i < len(b.Bounds) {
			le = strconv.Itoa(b.Bounds[i])
		}
		f.Samples = append(f.Samples, metrics.Sample{Suffix: "_bucket", Labels: map[string]string{"le": le},
			Value: float64(n)})
	}
	total := int64(0)
	if len(b.Counts) > 0 {
		total = b.Counts[len(b.Counts)-1]
	}
	f.Samples = append(f.Samples, metrics.Sample{Suffix: "_sum", Value: float64(b.Sum)},
		metrics.Sample{Suffix: "_count", Value: float64(total)})
	return f
}
