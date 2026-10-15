package metrics

import (
	"bufio"
	"cmp"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Namespace prefixes every metric name in the Prometheus exposition.
const Namespace = "millstream_"

// series is one metric and label set, aggregated over the rollups' minutes.
type series struct {
	name   string
	labels map[string]string // the pipeline included
	count  int64
	sum    float64
	last   time.Time // a gauge's latest minute, whose mean it shows
	lastV  float64
}

// Prometheus writes rollups in the Prometheus text exposition format (version 0.0.4), aggregated over their
// minutes: a counter as <name>_total, the sum of its points; a gauge as its mean over its latest minute; a
// distribution as a summary without quantiles (<name>_sum, <name>_count); a histogram with its cumulative buckets.
// Every series is labelled with its pipeline. Names and label names are sanitised to [a-zA-Z0-9_], and the output
// is sorted, so equal rollups render equally.
func Prometheus(w io.Writer, rollups []store.MetricRollup) error {
	agg := map[string]*series{}
	for _, r := range rollups {
		labels := maps.Clone(r.Labels)
		if labels == nil {
			labels = map[string]string{}
		}
		labels["pipeline"] = r.Pipeline
		key := r.Name + "\x00" + labelString(labels)
		s := agg[key]
		if s == nil {
			s = &series{name: r.Name, labels: labels}
			agg[key] = s
		}
		s.count += r.Count
		s.sum += r.Sum
		if r.Minute.After(s.last) || s.last.IsZero() {
			s.last, s.lastV = r.Minute, r.Sum/float64(max(r.Count, 1))
		}
	}
	families := map[string][]*series{} // by family name: a histogram's buckets join its family
	for _, s := range agg {
		family, _ := strings.CutSuffix(s.name, BucketSuffix)
		if family != s.name && kinds[family] != Histogram {
			family = s.name
		}
		families[family] = append(families[family], s)
	}
	bw := bufio.NewWriter(w)
	for _, family := range slices.Sorted(maps.Keys(families)) {
		ss := families[family]
		slices.SortFunc(ss, func(a, b *series) int {
			return cmp.Or(cmp.Compare(a.name, b.name), compareLabels(a.labels, b.labels))
		})
		writeFamily(bw, family, ss)
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("write metrics: %w", err)
	}
	return nil
}

func writeFamily(w *bufio.Writer, family string, ss []*series) {
	name := Namespace + sanitize(family)
	switch KindOf(family) {
	case Counter:
		fmt.Fprintf(w, "# TYPE %s_total counter\n", name)
		for _, s := range ss {
			fmt.Fprintf(w, "%s_total%s %s\n", name, labelString(s.labels), number(s.sum))
		}
	case Gauge:
		fmt.Fprintf(w, "# TYPE %s gauge\n", name)
		for _, s := range ss {
			fmt.Fprintf(w, "%s%s %s\n", name, labelString(s.labels), number(s.lastV))
		}
	case Histogram:
		fmt.Fprintf(w, "# TYPE %s histogram\n", name)
		for _, s := range ss {
			if s.name != family { // a bucket: its count is the number of values at or under its bound
				fmt.Fprintf(w, "%s_bucket%s %d\n", name, labelString(s.labels), s.count)
			}
		}
		writeSumCount(w, name, ss, family)
	default:
		fmt.Fprintf(w, "# TYPE %s summary\n", name)
		writeSumCount(w, name, ss, family)
	}
}

func writeSumCount(w *bufio.Writer, name string, ss []*series, family string) {
	for _, s := range ss {
		if s.name == family {
			fmt.Fprintf(w, "%s_sum%s %s\n", name, labelString(s.labels), number(s.sum))
			fmt.Fprintf(w, "%s_count%s %d\n", name, labelString(s.labels), s.count)
		}
	}
}

// labelString renders labels as {k="v",…}, sorted by name, values escaped.
func labelString(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range slices.Sorted(maps.Keys(labels)) {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(sanitize(k))
		b.WriteString(`="`)
		b.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(labels[k]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// compareLabels orders label sets by their rendering, a histogram's buckets by their numeric bound.
func compareLabels(a, b map[string]string) int {
	without := func(m map[string]string) string {
		m = maps.Clone(m)
		delete(m, "le")
		return labelString(m)
	}
	return cmp.Or(cmp.Compare(without(a), without(b)), cmp.Compare(bound(a["le"]), bound(b["le"])))
}

// bound is a bucket's upper bound; +Inf, and no bound, sort last.
func bound(le string) float64 {
	v, err := strconv.ParseFloat(le, 64)
	if err != nil {
		return math.Inf(1)
	}
	return v
}

// sanitize maps a name to the characters Prometheus allows, [a-zA-Z0-9_], not starting with a digit.
func sanitize(name string) string {
	out := []byte(name)
	for i, c := range out {
		letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
		digit := c >= '0' && c <= '9' && i > 0
		if !letter && !digit {
			out[i] = '_'
		}
	}
	if len(out) == 0 {
		return "_"
	}
	return string(out)
}

func number(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
