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

	"github.com/monolithiclab/millstream/internal/store"
)

// Namespace prefixes every metric name in the Prometheus exposition.
const Namespace = "millstream_"

// Prometheus writes rollups in the Prometheus text exposition format (version 0.0.4), aggregated over their
// minutes: Exposition of their Totals, without engine families.
func Prometheus(w io.Writer, rollups []store.MetricRollup) error {
	return Exposition(w, Totals(rollups), nil)
}

// Totals aggregates rollups into one series per pipeline, metric and label set, as the store's MetricTotals does.
func Totals(rollups []store.MetricRollup) []store.MetricTotal {
	agg := map[string]*store.MetricTotal{}
	var keys []string
	for _, r := range rollups {
		key := r.Pipeline + "\x00" + r.Name + "\x00" + labelString(r.Labels)
		t := agg[key]
		if t == nil {
			t = &store.MetricTotal{Pipeline: r.Pipeline, Name: r.Name, Labels: r.Labels}
			agg[key] = t
			keys = append(keys, key)
		}
		t.Count += r.Count
		t.Sum += r.Sum
		if t.Last.IsZero() || r.Minute.After(t.Last) {
			t.Last, t.LastCount, t.LastSum = r.Minute, r.Count, r.Sum
		}
	}
	out := make([]store.MetricTotal, 0, len(keys))
	for _, k := range keys {
		out = append(out, *agg[k])
	}
	return out
}

// Family is a metric the engine measures itself rather than rolls up from events (spec section 10.2): leases held,
// writer batch sizes, review notification failures, eval scores. Its samples carry their own labels and, for a
// histogram, their series suffix (_bucket, _sum, _count).
type Family struct {
	// Name is the metric's name, without the namespace and, for a counter, without _total.
	Name    string
	Kind    Kind
	Samples []Sample
}

// Sample is one series of a Family.
type Sample struct {
	Suffix string
	Labels map[string]string
	Value  float64
}

// Exposition writes the rollups' series and the engine's families in the Prometheus text exposition format
// (version 0.0.4): a counter as <name>_total, the sum of its minutes; a gauge as its mean over its latest minute; a
// distribution as a summary without quantiles (<name>_sum, <name>_count); a histogram with its cumulative buckets.
// Every rollup series is labelled with its pipeline. Names and label names are sanitised to [a-zA-Z0-9_], and the
// output is sorted by family, so equal inputs render equally. An engine family hides a rollup family exposed under
// the same name (an action's metric, whose name sanitises to it).
func Exposition(w io.Writer, totals []store.MetricTotal, engine []Family) error {
	families := map[string][]store.MetricTotal{} // by family name: a histogram's buckets join its family
	for _, t := range totals {
		labels := maps.Clone(t.Labels)
		if labels == nil {
			labels = map[string]string{}
		}
		labels["pipeline"] = t.Pipeline
		t.Labels = labels
		family, _ := strings.CutSuffix(t.Name, BucketSuffix)
		if family != t.Name && kinds[family] != Histogram {
			family = t.Name
		}
		families[family] = append(families[family], t)
	}
	blocks := map[string]string{} // by exposed name
	for family, ts := range families {
		slices.SortFunc(ts, func(a, b store.MetricTotal) int {
			return cmp.Or(cmp.Compare(a.Name, b.Name), compareLabels(a.Labels, b.Labels))
		})
		name, block := rollupFamily(family, ts)
		blocks[name] = block
	}
	for _, f := range engine {
		name, block := engineFamily(f)
		blocks[name] = block
	}
	bw := bufio.NewWriter(w)
	for _, name := range slices.Sorted(maps.Keys(blocks)) {
		_, _ = bw.WriteString(blocks[name])
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("write metrics: %w", err)
	}
	return nil
}

// exposedName is a family's name in the exposition and its TYPE.
func exposedName(family string, kind Kind) (string, string) {
	name := Namespace + sanitize(family)
	switch kind {
	case Counter:
		return name + "_total", "counter"
	case Gauge:
		return name, "gauge"
	case Histogram:
		return name, "histogram"
	}
	return name, "summary"
}

func rollupFamily(family string, ts []store.MetricTotal) (string, string) {
	kind := KindOf(family)
	name, typ := exposedName(family, kind)
	var b strings.Builder
	fmt.Fprintf(&b, "# TYPE %s %s\n", name, typ)
	for _, t := range ts {
		switch {
		case kind == Counter:
			fmt.Fprintf(&b, "%s%s %s\n", name, labelString(t.Labels), number(t.Sum))
		case kind == Gauge:
			fmt.Fprintf(&b, "%s%s %s\n", name, labelString(t.Labels), number(t.LastSum/float64(max(t.LastCount, 1))))
		case kind == Histogram && t.Name != family: // a bucket: its count is the number of values at or under its bound
			fmt.Fprintf(&b, "%s_bucket%s %d\n", name, labelString(t.Labels), t.Count)
		}
	}
	if kind == Histogram || kind == Distribution {
		for _, t := range ts {
			if t.Name == family {
				fmt.Fprintf(&b, "%s_sum%s %s\n", name, labelString(t.Labels), number(t.Sum))
				fmt.Fprintf(&b, "%s_count%s %d\n", name, labelString(t.Labels), t.Count)
			}
		}
	}
	return name, b.String()
}

func engineFamily(f Family) (string, string) {
	name, typ := exposedName(f.Name, f.Kind)
	var b strings.Builder
	fmt.Fprintf(&b, "# TYPE %s %s\n", name, typ)
	for _, s := range f.Samples {
		fmt.Fprintf(&b, "%s%s%s %s\n", name, s.Suffix, labelString(s.Labels), number(s.Value))
	}
	return name, b.String()
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
