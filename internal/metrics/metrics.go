// Package metrics names Millstream's metrics (spec section 10.2) and derives them from the event log: every metric
// is a projection of events (D10), rolled up per minute by the store's rollup unit, so the metrics, the run
// reports and the live view cannot disagree. Points derives the measurements one event carries; the gauges (queue
// depth, oldest pending age) are sampled by the rollup unit from the deliveries instead. Prometheus renders rollups
// in the Prometheus text exposition format, for the engine's /metrics endpoint.
//
// Every engine metric is labelled with its step where it has one. An action's own metric messages keep their name
// and labels, with the step added; one whose name an engine metric already uses is renamed action_<name>.
package metrics

import (
	"encoding/json"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Kind is how a metric aggregates.
type Kind int

// Metric kinds. A counter's points are each 1 (or a count); a gauge is sampled; a distribution keeps the count,
// sum, minimum and maximum of its values; a histogram is a distribution with cumulative buckets (HistogramBuckets).
const (
	Counter Kind = iota
	Gauge
	Distribution
	Histogram
)

// Engine metrics (spec section 10.2), by name.
const (
	// ItemsIn counts the deliveries a step leased for the first time {step}.
	ItemsIn = "items_in"
	// ItemsOut counts the items a step published {step, stream}; a trigger's items count under the trigger stream.
	ItemsOut = "items_out"
	// ItemsFiltered counts the deliveries a builtin filtered {step, reason}.
	ItemsFiltered = "items_filtered"
	// AttemptsFailed counts the failed attempts, retried or not {step, class}.
	AttemptsFailed = "attempts_failed"
	// Retries counts the failed attempts scheduled for a retry {step, class}.
	Retries = "retries"
	// ItemsDead counts the deliveries that ended dead {step, class}.
	ItemsDead = "items_dead"
	// ItemsUnrouted counts the outputs no route matched {step}.
	ItemsUnrouted = "items_unrouted"
	// NondeterministicOutputs counts the outputs a retry published with another payload {step, stream}.
	NondeterministicOutputs = "nondeterministic_outputs"
	// LeaseExpiries counts the leases that expired before their owner reported {step}.
	LeaseExpiries = "lease_expiries"
	// Unschedulable counts the attempts no runtime could take (UnschedulableClasses) {step, class}.
	Unschedulable = "deliveries_unschedulable"
	// QueueDepth samples the due pending deliveries of a step {step}.
	QueueDepth = "queue_depth"
	// OldestPending samples how long a step's oldest due delivery has waited, in seconds {step}.
	OldestPending = "oldest_pending_seconds"
	// DeliveryLatency is a terminal delivery's latency, creation to terminal, in seconds {step}.
	DeliveryLatency = "delivery_latency_seconds"
	// DeliveryWait is an attempt's queue wait, due to leased, in seconds {step}.
	DeliveryWait = "delivery_wait_seconds"
	// DeliveryWork is an attempt's work time, leased to reported, in seconds {step}.
	DeliveryWork = "delivery_work_seconds"
	// DeliveryCost is an attempt's cost in US dollars {step}.
	DeliveryCost = "delivery_cost_usd"
	// RunDuration is a settled run's duration in seconds {state}.
	RunDuration = "run_duration_seconds"
	// RunCost is a settled run's cost in US dollars {state}.
	RunCost = "run_cost_usd"
)

// UnschedulableClasses are the error classes of a delivery no runtime can take (spec sections 5.10 and 11.1): an
// action kind or runtime this deployment lacks (engine.ClassUnsupported) and an agent step without a sandbox
// (engine.ClassSandbox).
var UnschedulableClasses = map[string]bool{"unsupported": true, "sandbox": true}

// kinds are the engine metrics' kinds.
var kinds = map[string]Kind{
	ItemsIn: Counter, ItemsOut: Counter, ItemsFiltered: Counter, AttemptsFailed: Counter, Retries: Counter,
	ItemsDead: Counter, ItemsUnrouted: Counter, NondeterministicOutputs: Counter, LeaseExpiries: Counter,
	Unschedulable: Counter, QueueDepth: Gauge, OldestPending: Gauge, DeliveryLatency: Histogram,
	DeliveryWait: Distribution, DeliveryWork: Distribution, DeliveryCost: Distribution, RunDuration: Distribution,
	RunCost: Distribution,
}

// KindOf returns a metric's kind: an engine metric's own, Counter for a histogram's bucket series, and
// Distribution for an action's metrics.
func KindOf(name string) Kind {
	if k, ok := kinds[name]; ok {
		return k
	}
	if base, ok := strings.CutSuffix(name, BucketSuffix); ok && kinds[base] == Histogram {
		return Counter
	}
	return Distribution
}

// BucketSuffix ends the name of a histogram's bucket series, labelled with its upper bound "le".
const BucketSuffix = "_bucket"

// HistogramBuckets are the upper bounds, in seconds, of the delivery latency histogram; +Inf follows them.
var HistogramBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 1800}

// Point is one measurement of a metric.
type Point struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// eventData is the part of an event's data the projection reads (store/sqlite records it).
type eventData struct {
	Step       string         `json:"step"`
	Stream     string         `json:"stream"`
	Attempt    int            `json:"attempt"`
	Class      string         `json:"class"`
	Reason     string         `json:"reason"`
	Kind       string         `json:"kind"`
	OriginStep string         `json:"originStep"`
	State      string         `json:"state"`
	WaitMS     *int64         `json:"waitMs"`
	WorkMS     *int64         `json:"workMs"`
	LatencyMS  *int64         `json:"latencyMs"`
	DurationMS *int64         `json:"durationMs"`
	CostUSD    float64        `json:"costUsd"`
	Metrics    []store.Metric `json:"metrics"`
}

// Points returns the measurements an event carries. An event it cannot decode carries none.
func Points(ev store.Event) []Point {
	var d eventData
	if json.Unmarshal(ev.Data, &d) != nil {
		return nil
	}
	step := map[string]string{"step": d.Step}
	with := func(k, v string) map[string]string { return map[string]string{"step": d.Step, k: v} }
	var ps []Point
	add := func(name string, labels map[string]string, v float64) {
		ps = append(ps, Point{Name: name, Labels: labels, Value: v})
	}
	switch ev.Kind {
	case store.EventDeliveryLeased:
		if d.Attempt == 1 {
			add(ItemsIn, step, 1)
		}
		if d.WaitMS != nil {
			add(DeliveryWait, step, seconds(*d.WaitMS))
		}
	case store.EventItemPublished:
		add(ItemsOut, with("stream", d.Stream), 1)
	case store.EventNondeterministicOutput:
		add(NondeterministicOutputs, with("stream", d.Stream), 1)
	case store.EventDeliveryFiltered:
		add(ItemsFiltered, with("reason", d.Reason), 1)
	case store.EventDeliveryRetry, store.EventDeliveryDead:
		add(AttemptsFailed, with("class", d.Class), 1)
		if UnschedulableClasses[d.Class] {
			add(Unschedulable, with("class", d.Class), 1)
		}
		if ev.Kind == store.EventDeliveryRetry {
			add(Retries, with("class", d.Class), 1)
		} else {
			add(ItemsDead, with("class", d.Class), 1)
		}
	case store.EventLeaseExpired:
		add(LeaseExpiries, step, 1)
	case store.EventDeadLetter:
		if d.Kind == string(store.DeadLetterUnrouted) {
			add(ItemsUnrouted, map[string]string{"step": d.OriginStep}, 1)
		}
	case store.EventRunSettled:
		state := map[string]string{"state": d.State}
		if d.DurationMS != nil {
			add(RunDuration, state, seconds(*d.DurationMS))
		}
		add(RunCost, state, d.CostUSD)
		return ps
	}
	if d.WorkMS != nil { // the end of an attempt: done, retry, dead, filtered, cancelled
		add(DeliveryWork, step, seconds(*d.WorkMS))
		if d.CostUSD != 0 {
			add(DeliveryCost, step, d.CostUSD)
		}
		for _, m := range d.Metrics {
			labels := maps.Clone(m.Labels)
			if labels == nil {
				labels = map[string]string{}
			}
			labels["step"] = d.Step
			add(ActionName(m.Name), labels, m.Value)
		}
	}
	if d.LatencyMS != nil {
		ps = append(ps, latency(d.Step, seconds(*d.LatencyMS))...)
	}
	return ps
}

// ActionName is the name an action's metric is stored under: its own, or action_<name> when an engine metric (or
// one of its series) uses it.
func ActionName(name string) string {
	base, _ := strings.CutSuffix(name, BucketSuffix)
	if _, engine := kinds[base]; engine {
		return "action_" + name
	}
	return name
}

// latency returns a delivery latency's points: the distribution and every cumulative bucket holding it.
func latency(step string, v float64) []Point {
	ps := []Point{{Name: DeliveryLatency, Labels: map[string]string{"step": step}, Value: v}}
	for _, le := range HistogramBuckets {
		if v <= le {
			ps = append(ps, bucket(step, strconv.FormatFloat(le, 'g', -1, 64)))
		}
	}
	return append(ps, bucket(step, "+Inf"))
}

func bucket(step, le string) Point {
	return Point{Name: DeliveryLatency + BucketSuffix, Labels: map[string]string{"step": step, "le": le}, Value: 1}
}

func seconds(ms int64) float64 { return float64(ms) / 1000 }

// Minute is the minute a measurement at t is rolled up in.
func Minute(t time.Time) time.Time { return t.UTC().Truncate(time.Minute) }
