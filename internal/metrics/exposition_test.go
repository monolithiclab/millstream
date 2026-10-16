package metrics

import (
	"bytes"
	"maps"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// The exposition of rollups and engine families together parses as the Prometheus text format 0.0.4 (what promtool
// check metrics reads): an engine family hides an action's metric exposed under its name.
func TestExpositionFormat(t *testing.T) {
	t.Parallel()
	m0 := time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)
	s := map[string]string{"step": "score"}
	totals := Totals([]store.MetricRollup{
		{Pipeline: "p", Name: ItemsIn, Labels: s, Minute: m0, Count: 2, Sum: 2},
		{Pipeline: "p", Name: QueueDepth, Labels: s, Minute: m0, Count: 2, Sum: 6},
		{Pipeline: "p", Name: DeliveryLatency, Labels: s, Minute: m0, Count: 3, Sum: 4.5},
		{Pipeline: "p", Name: DeliveryLatency + BucketSuffix, Labels: map[string]string{"step": "score", "le": "0.5"},
			Minute: m0, Count: 1, Sum: 1},
		{Pipeline: "p", Name: DeliveryLatency + BucketSuffix, Labels: map[string]string{"step": "score", "le": "+Inf"},
			Minute: m0, Count: 3, Sum: 3},
		{Pipeline: "p", Name: "eval-score", Labels: s, Minute: m0, Count: 1, Sum: 7}, // sanitises to eval_score
		{Pipeline: "p", Name: "tokens_in", Labels: map[string]string{"model": "a\"b\\c\nd", "step": "score"},
			Minute: m0, Count: 1, Sum: 100},
	})
	engine := []Family{
		{Name: EvalScore, Kind: Gauge, Samples: []Sample{{Labels: map[string]string{"action": "a", "case": "c"}, Value: 0.5}}},
		{Name: NotificationsFailed, Kind: Counter, Samples: []Sample{{Value: 3}}},
		{Name: WriterBatchSize, Kind: Histogram, Samples: []Sample{
			{Suffix: "_bucket", Labels: map[string]string{"le": "1"}, Value: 2},
			{Suffix: "_bucket", Labels: map[string]string{"le": "+Inf"}, Value: 3},
			{Suffix: "_sum", Value: 4}, {Suffix: "_count", Value: 3},
		}},
	}
	var buf bytes.Buffer
	if err := Exposition(&buf, totals, engine); err != nil {
		t.Fatal(err)
	}
	families := parseExposition(t, buf.String())
	if got := slices.Sorted(maps.Keys(families)); !slices.Equal(got, []string{
		"millstream_delivery_latency_seconds", "millstream_eval_score", "millstream_items_in_total",
		"millstream_queue_depth", "millstream_review_notifications_failed_total", "millstream_tokens_in",
		"millstream_writer_batch_size",
	}) {
		t.Errorf("families %v", got)
	}
	if strings.Contains(buf.String(), `step="score"} 7`) {
		t.Errorf("the action's eval-score shows beside the engine's:\n%s", buf.String())
	}
}

var (
	sampleLine = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})? (\S+)( -?\d+)?$`)
	labelPair  = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\\n]|\\[\\"n])*)"(,|$)`)
)

// parseExposition checks text against the Prometheus text format 0.0.4 and returns its families' types, by name:
// each sample follows its family's one TYPE line, with valid names, labels and values; a histogram's buckets rise
// to +Inf, which equals its count.
func parseExposition(t *testing.T, text string) map[string]string {
	t.Helper()
	types := map[string]string{}
	family := ""
	buckets := map[string][]float64{} // a histogram's bucket counts by label set without le
	bounds := map[string][]float64{}
	counts := map[string]float64{}
	if !strings.HasSuffix(text, "\n") {
		t.Fatal("the exposition does not end with a newline")
	}
	for i, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		if rest, ok := strings.CutPrefix(line, "# TYPE "); ok {
			name, typ, _ := strings.Cut(rest, " ")
			if _, seen := types[name]; seen || !slices.Contains([]string{"counter", "gauge", "histogram", "summary",
				"untyped"}, typ) {
				t.Fatalf("line %d: TYPE %q repeated or unknown", i+1, line)
			}
			types[name], family = typ, name
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		m := sampleLine.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("line %d: not a sample: %q", i+1, line)
		}
		name, labels := m[1], map[string]string{}
		for rest := m[3]; rest != ""; {
			p := labelPair.FindStringSubmatch(rest)
			if p == nil {
				t.Fatalf("line %d: bad labels %q", i+1, m[3])
			}
			if _, dup := labels[p[1]]; dup {
				t.Fatalf("line %d: label %s repeated", i+1, p[1])
			}
			labels[p[1]] = p[2]
			rest = rest[len(p[0]):]
		}
		v, err := strconv.ParseFloat(m[4], 64)
		if err != nil {
			t.Fatalf("line %d: value %q", i+1, m[4])
		}
		suffix, ok := strings.CutPrefix(name, family)
		allowed := map[string][]string{"histogram": {"_bucket", "_sum", "_count"}, "summary": {"", "_sum", "_count"}}
		if !ok || family == "" || !slices.Contains(cmpOr(allowed[types[family]], []string{""}), suffix) {
			t.Fatalf("line %d: sample %s outside its family (%s %s)", i+1, name, family, types[family])
		}
		if types[family] == "counter" && !strings.HasSuffix(name, "_total") {
			t.Errorf("line %d: counter %s without _total", i+1, name)
		}
		if types[family] != "histogram" {
			continue
		}
		le := labels["le"]
		delete(labels, "le")
		key := family + labelText(labels)
		switch suffix {
		case "_bucket":
			b, err := strconv.ParseFloat(le, 64)
			if err != nil {
				t.Fatalf("line %d: le %q", i+1, le)
			}
			if n := len(bounds[key]); n > 0 && (b <= bounds[key][n-1] || v < buckets[key][n-1]) {
				t.Fatalf("line %d: buckets of %s do not rise", i+1, key)
			}
			bounds[key], buckets[key] = append(bounds[key], b), append(buckets[key], v)
		case "_count":
			counts[key] = v
		}
	}
	for key, bs := range bounds {
		if !math.IsInf(bs[len(bs)-1], 1) || buckets[key][len(bs)-1] != counts[key] {
			t.Errorf("histogram %s: last bucket %v of %v, count %v", key, bs[len(bs)-1], buckets[key], counts[key])
		}
	}
	return types
}

func cmpOr(a, b []string) []string {
	if a != nil {
		return a
	}
	return b
}

func labelText(labels map[string]string) string {
	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		b.WriteString(k + "=" + labels[k] + ",")
	}
	return b.String()
}
