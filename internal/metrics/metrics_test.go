package metrics

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

var update = flag.Bool("update", false, "rewrite the golden files")

func ev(kind, data string) store.Event {
	return store.Event{Kind: kind, Data: json.RawMessage(data), TS: time.Date(2026, 10, 15, 6, 0, 30, 0, time.UTC)}
}

// render is a point as name{k=v,…}=value, labels sorted.
func render(p Point) string {
	var labels []string
	for k, v := range p.Labels {
		labels = append(labels, k+"="+v)
	}
	slices.Sort(labels)
	return p.Name + "{" + strings.Join(labels, ",") + "}=" + number(p.Value)
}

func TestPoints(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ev   store.Event
		want []string
	}{
		{"first lease", ev(store.EventDeliveryLeased, `{"step":"s","attempt":1,"waitMs":1500}`),
			[]string{"items_in{step=s}=1", "delivery_wait_seconds{step=s}=1.5"}},
		{"retry lease", ev(store.EventDeliveryLeased, `{"step":"s","attempt":2,"waitMs":0}`),
			[]string{"delivery_wait_seconds{step=s}=0"}},
		{"published", ev(store.EventItemPublished, `{"step":"s","stream":"a.b"}`),
			[]string{"items_out{step=s,stream=a.b}=1"}},
		{"nondeterministic", ev(store.EventNondeterministicOutput, `{"step":"s","stream":"a.b"}`),
			[]string{"nondeterministic_outputs{step=s,stream=a.b}=1"}},
		{"filtered", ev(store.EventDeliveryFiltered, `{"step":"s","reason":"duplicate","workMs":2,"latencyMs":40}`),
			[]string{"items_filtered{reason=duplicate,step=s}=1", "delivery_work_seconds{step=s}=0.002",
				"delivery_latency_seconds{step=s}=0.04", "delivery_latency_seconds_bucket{le=0.05,step=s}=1",
				"delivery_latency_seconds_bucket{le=0.1,step=s}=1", "delivery_latency_seconds_bucket{le=0.25,step=s}=1",
				"delivery_latency_seconds_bucket{le=0.5,step=s}=1", "delivery_latency_seconds_bucket{le=1,step=s}=1",
				"delivery_latency_seconds_bucket{le=2.5,step=s}=1", "delivery_latency_seconds_bucket{le=5,step=s}=1",
				"delivery_latency_seconds_bucket{le=10,step=s}=1", "delivery_latency_seconds_bucket{le=30,step=s}=1",
				"delivery_latency_seconds_bucket{le=60,step=s}=1", "delivery_latency_seconds_bucket{le=300,step=s}=1",
				"delivery_latency_seconds_bucket{le=1800,step=s}=1", "delivery_latency_seconds_bucket{le=+Inf,step=s}=1"}},
		{"retry", ev(store.EventDeliveryRetry, `{"step":"s","class":"crash","workMs":1000,"costUsd":0.5}`),
			[]string{"attempts_failed{class=crash,step=s}=1", "retries{class=crash,step=s}=1",
				"delivery_work_seconds{step=s}=1", "delivery_cost_usd{step=s}=0.5"}},
		{"dead unschedulable", ev(store.EventDeliveryDead, `{"step":"s","class":"sandbox","workMs":0,"latencyMs":4000}`),
			[]string{"attempts_failed{class=sandbox,step=s}=1", "deliveries_unschedulable{class=sandbox,step=s}=1",
				"items_dead{class=sandbox,step=s}=1", "delivery_work_seconds{step=s}=0",
				"delivery_latency_seconds{step=s}=4", "delivery_latency_seconds_bucket{le=5,step=s}=1",
				"delivery_latency_seconds_bucket{le=10,step=s}=1", "delivery_latency_seconds_bucket{le=30,step=s}=1",
				"delivery_latency_seconds_bucket{le=60,step=s}=1", "delivery_latency_seconds_bucket{le=300,step=s}=1",
				"delivery_latency_seconds_bucket{le=1800,step=s}=1", "delivery_latency_seconds_bucket{le=+Inf,step=s}=1"}},
		{"done with action metrics", ev(store.EventDeliveryDone, `{"step":"s","workMs":10,"metrics":[
			{"name":"tokens_in","value":120,"labels":{"model":"m","step":"spoofed"}},
			{"name":"queue_depth","value":3}]}`),
			[]string{"delivery_work_seconds{step=s}=0.01", "tokens_in{model=m,step=s}=120",
				"action_queue_depth{step=s}=3"}},
		{"lease expired", ev(store.EventLeaseExpired, `{"step":"s"}`), []string{"lease_expiries{step=s}=1"}},
		{"unrouted", ev(store.EventDeadLetter, `{"kind":"unrouted","originStep":"s"}`),
			[]string{"items_unrouted{step=s}=1"}},
		{"dead letter of a dead delivery", ev(store.EventDeadLetter, `{"kind":"dead","originStep":"s"}`), nil},
		{"settled", ev(store.EventRunSettled, `{"state":"succeeded","durationMs":61000,"costUsd":1.25}`),
			[]string{"run_duration_seconds{state=succeeded}=61", "run_cost_usd{state=succeeded}=1.25"}},
		{"review decided", ev(store.EventReviewDecided,
			`{"step":"g","decision":"reject","reason":"overpriced","reviewer":"person","timeToDecisionMs":90000}`),
			[]string{"review_decisions{decision=reject,reason=overpriced,step=g}=1",
				"review_time_to_decision_seconds{step=g}=90"}},
		{"auto-approved", ev(store.EventReviewDecided, `{"step":"g","decision":"approve","reviewer":"rule"}`),
			[]string{"review_decisions{decision=autoApprove,step=g}=1"}},
		{"timed out", ev(store.EventReviewDecided,
			`{"step":"g","decision":"reject","reason":"timeout","reviewer":"timeout","timeToDecisionMs":5}`),
			[]string{"review_decisions{decision=reject,reason=timeout,step=g}=1"}},
		{"parked", ev(store.EventReviewParked, `{"step":"g","workMs":3}`), []string{"delivery_work_seconds{step=g}=0.003"}},
		{"sample decided", ev(store.EventSampleDecided, `{"step":"g","decision":"agree"}`),
			[]string{"review_samples{agreement=agree,step=g}=1"}},
		{"sample expired", ev(store.EventSampleExpired, `{"step":"g"}`),
			[]string{"review_samples{agreement=unreviewed,step=g}=1"}},
		{"handed to the fallback", ev(store.EventDeliveryFallback, `{"step":"s","cause":"abstain","workMs":5}`),
			[]string{"fallback_handoffs{cause=abstain,step=s}=1", "delivery_work_seconds{step=s}=0.005"}},
		{"done by the fallback", ev(store.EventDeliveryDone,
			`{"step":"s","producedBy":"fallback","fallback":true,"workMs":0,"costUsd":0.25}`),
			[]string{"fallback_deliveries{producedBy=fallback,step=s}=1", "delivery_work_seconds{step=s}=0",
				"delivery_cost_usd{step=s}=0.25", "fallback_cost_usd{step=s,use=delivery}=0.25"}},
		{"audit agreed", ev(store.EventAuditEnded, `{"step":"s","state":"done","agreement":1,"costUsd":0.5}`),
			[]string{"audits{agreement=agree,step=s}=1", "fallback_cost_usd{step=s,use=audit}=0.5"}},
		{"audit disagreed", ev(store.EventAuditEnded, `{"step":"s","state":"done","agreement":0}`),
			[]string{"audits{agreement=disagree,step=s}=1"}},
		{"audit failed", ev(store.EventAuditEnded, `{"step":"s","state":"failed","class":"leaseExpired"}`),
			[]string{"audits{agreement=failed,step=s}=1"}},
		{"other", ev(store.EventRunStarted, `{}`), nil},
		{"malformed", ev(store.EventItemPublished, `[`), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			for _, p := range Points(tt.ev) {
				got = append(got, render(p))
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("Points =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(tt.want, "\n"))
			}
		})
	}
}

func TestKindOf(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]Kind{
		ItemsOut: Counter, QueueDepth: Gauge, DeliveryWork: Distribution, DeliveryLatency: Histogram,
		DeliveryLatency + BucketSuffix: Counter, "tokens_in": Distribution, "x_bucket": Distribution,
	} {
		if got := KindOf(name); got != want {
			t.Errorf("KindOf(%s) = %v, want %v", name, got, want)
		}
	}
	if got := ActionName("delivery_latency_seconds_bucket"); got != "action_delivery_latency_seconds_bucket" {
		t.Errorf("ActionName = %s", got)
	}
	if got := ActionName(EvalScore); got != "action_eval_score" {
		t.Errorf("ActionName(%s) = %s", EvalScore, got)
	}
}

// The exposition aggregates the minutes of each series: counters summed, gauges at their latest minute's mean,
// distributions as summaries, the latency histogram with its buckets; names and label values are made safe.
func TestPrometheus(t *testing.T) {
	t.Parallel()
	m0 := time.Date(2026, 10, 15, 6, 0, 0, 0, time.UTC)
	m1 := m0.Add(time.Minute)
	s := map[string]string{"step": "score"}
	rollups := []store.MetricRollup{
		{Pipeline: "p", Name: ItemsOut, Labels: map[string]string{"step": "score", "stream": "a.b"}, Minute: m0,
			Count: 2, Sum: 2, Min: 1, Max: 1},
		{Pipeline: "p", Name: ItemsOut, Labels: map[string]string{"step": "score", "stream": "a.b"}, Minute: m1,
			Count: 3, Sum: 3, Min: 1, Max: 1},
		{Pipeline: "p", Name: QueueDepth, Labels: s, Minute: m1, Count: 2, Sum: 6, Min: 2, Max: 4},
		{Pipeline: "p", Name: QueueDepth, Labels: s, Minute: m0, Count: 1, Sum: 9, Min: 9, Max: 9},
		{Pipeline: "p", Name: DeliveryLatency, Labels: s, Minute: m0, Count: 3, Sum: 4.5, Min: 0.5, Max: 3},
		{Pipeline: "p", Name: DeliveryLatency + BucketSuffix, Labels: map[string]string{"step": "score", "le": "1"},
			Minute: m0, Count: 1, Sum: 1, Min: 1, Max: 1},
		{Pipeline: "p", Name: DeliveryLatency + BucketSuffix, Labels: map[string]string{"step": "score", "le": "+Inf"},
			Minute: m0, Count: 3, Sum: 3, Min: 1, Max: 1},
		{Pipeline: "p", Name: "tokens-in", Labels: map[string]string{"model": `a "quoted"\name`, "step": "score"},
			Minute: m0, Count: 2, Sum: 300, Min: 100, Max: 200},
		{Pipeline: "q", Name: DeliveryWork, Minute: m0, Count: 1, Sum: 0.25, Min: 0.25, Max: 0.25},
	}
	var buf bytes.Buffer
	if err := Prometheus(&buf, rollups, nil); err != nil {
		t.Fatal(err)
	}
	golden := filepath.Join("testdata", "prometheus.txt")
	if *update {
		if err := os.WriteFile(golden, buf.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if buf.String() != string(want) {
		t.Errorf("exposition =\n%s\nwant\n%s", buf.String(), want)
	}
	parseExposition(t, buf.String())
	if sanitize("9lives") != "_lives" || sanitize("") != "_" {
		t.Errorf("sanitize: %q, %q", sanitize("9lives"), sanitize(""))
	}
}
