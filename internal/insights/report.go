package insights

import (
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// compareRuns is how many earlier runs a report compares with (spec section 11.1).
const compareRuns = 10

// Report is a run's report (spec section 11.1). Durations are in seconds. It comes straight from the log store, so
// it is available whether insights are on or off.
type Report struct {
	Pipeline        string         `json:"pipeline"`
	Run             string         `json:"run"`
	Kind            store.RunKind  `json:"kind"`
	State           store.RunState `json:"state"`
	StartedAt       time.Time      `json:"startedAt"`
	SettledAt       *time.Time     `json:"settledAt"`
	DurationSeconds float64        `json:"durationSeconds"`
	CostUSD         float64        `json:"costUsd"`
	// Items counts the items the run published, per stream.
	Items        map[string]int   `json:"items"`
	CriticalPath ReportPath       `json:"criticalPath"`
	Steps        []StepReport     `json:"steps"`
	Failures     []FailureCluster `json:"failures"`
	// Comparison is against the median of the last runs of the same kind settled before this one started; nil
	// when there are none.
	Comparison *Comparison `json:"comparison"`
	// ETA is when a running run is expected to settle, from the median duration of the compared runs; nil for a
	// settled run or without history.
	ETA *time.Time `json:"eta"`
}

// ReportPath is the critical path in a report.
type ReportPath struct {
	TotalSeconds float64          `json:"totalSeconds"`
	Steps        []ReportPathStep `json:"steps"`
}

// ReportPathStep is one delivery on the critical path.
type ReportPathStep struct {
	Step           string  `json:"step"`
	Delivery       string  `json:"delivery"`
	LatencySeconds float64 `json:"latencySeconds"`
	WaitSeconds    float64 `json:"waitSeconds"`
	WorkSeconds    float64 `json:"workSeconds"`
	BackoffSeconds float64 `json:"backoffSeconds"`
	Share          float64 `json:"share"`
}

// StepReport is one step's figures in a run, the slowest first (by p95 latency).
type StepReport struct {
	Step           string  `json:"step"`
	Deliveries     int     `json:"deliveries"`
	FailedAttempts int     `json:"failedAttempts"`
	P50Seconds     float64 `json:"p50Seconds"`
	P95Seconds     float64 `json:"p95Seconds"`
	MaxSeconds     float64 `json:"maxSeconds"`
	WaitSeconds    float64 `json:"meanWaitSeconds"`
	WorkSeconds    float64 `json:"meanWorkSeconds"`
	CostUSD        float64 `json:"costUsd"`
}

// FailureCluster is a run's failed attempts with one step, class and normalised message.
type FailureCluster struct {
	Step    string `json:"step"`
	Class   string `json:"class"`
	Message string `json:"message"`
	Example string `json:"example"`
	Count   int    `json:"count"`
	// Recovered counts the deliveries of the cluster that ended done.
	Recovered int `json:"recovered"`
}

// Comparison sets the run against the median of earlier runs.
type Comparison struct {
	Runs           int    `json:"runs"`
	Duration       Figure `json:"durationSeconds"`
	Cost           Figure `json:"costUsd"`
	Items          Figure `json:"items"`
	FailedAttempts Figure `json:"failedAttempts"`
}

// Figure is a run's value with the median of the compared runs.
type Figure struct {
	Value  float64 `json:"value"`
	Median float64 `json:"median"`
}

// BuildReport reads a run's report at now.
func BuildReport(ctx context.Context, st store.Store, pipelineName, runID string, now time.Time) (*Report, error) {
	run, err := st.Run(ctx, pipelineName, runID)
	if err != nil {
		return nil, err
	}
	past, err := pastRuns(ctx, st, run)
	if err != nil {
		return nil, err
	}
	since := run.StartedAt
	for _, p := range past {
		since = minTime(since, p.StartedAt)
	}
	attempts, published, err := readAttempts(ctx, st, pipelineName, since)
	if err != nil {
		return nil, err
	}
	ds, err := st.Deliveries(ctx, store.DeliveryFilter{Pipeline: pipelineName, RunID: runID})
	if err != nil {
		return nil, err
	}
	items, err := st.Items(ctx, store.ItemFilter{Pipeline: pipelineName, RunID: runID})
	if err != nil {
		return nil, err
	}
	r := &Report{
		Pipeline: pipelineName, Run: runID, Kind: run.Kind, State: run.State, StartedAt: run.StartedAt,
		DurationSeconds: seconds(runDuration(run, now)), CostUSD: round6(run.CostUSD), Items: map[string]int{},
		Steps: []StepReport{}, Failures: []FailureCluster{},
	}
	if !run.SettledAt.IsZero() {
		r.SettledAt = &run.SettledAt
	}
	for _, p := range published {
		if p.RunID == runID {
			r.Items[p.Stream]++
		}
	}
	r.CriticalPath = reportPath(CriticalPath(ds, items, now))
	runAttempts := slices.DeleteFunc(slices.Clone(attempts), func(a Attempt) bool { return a.RunID != runID })
	r.Steps = stepReports(ds, runAttempts, now)
	r.Failures = runFailures(ds, runAttempts)
	r.Comparison, r.ETA = compare(run, r, past, attempts, published, now)
	return r, nil
}

// pastRuns returns the last runs of the pipeline of the run's kind settled before it started, newest first.
func pastRuns(ctx context.Context, st store.Store, run store.Run) ([]store.Run, error) {
	runs, err := st.Runs(ctx, store.RunFilter{Pipeline: run.Pipeline, Limit: 200})
	if err != nil {
		return nil, err
	}
	var out []store.Run
	for _, p := range runs {
		if p.ID != run.ID && p.Kind == run.Kind && !p.SettledAt.IsZero() && !p.SettledAt.After(run.StartedAt) &&
			len(out) < compareRuns {
			out = append(out, p)
		}
	}
	return out, nil
}

func runDuration(r store.Run, now time.Time) time.Duration {
	if r.SettledAt.IsZero() {
		return max(now.Sub(r.StartedAt), 0)
	}
	return r.SettledAt.Sub(r.StartedAt)
}

func reportPath(p Path) ReportPath {
	out := ReportPath{TotalSeconds: seconds(p.Total), Steps: []ReportPathStep{}}
	for _, s := range p.Steps {
		out.Steps = append(out.Steps, ReportPathStep{
			Step: s.Step, Delivery: s.Delivery, LatencySeconds: seconds(s.Latency), WaitSeconds: seconds(s.Wait),
			WorkSeconds: seconds(s.Work), BackoffSeconds: seconds(s.Backoff), Share: round(ratio(s.Latency, p.Total)),
		})
	}
	return out
}

// stepReports gives each step's latency figures, failed attempts and cost, the slowest first.
func stepReports(ds []store.Delivery, attempts []Attempt, now time.Time) []StepReport {
	type acc struct {
		latencies  []time.Duration
		wait, work time.Duration
		cost       float64
		failed     int
	}
	steps := map[string]*acc{}
	get := func(step string) *acc {
		a := steps[step]
		if a == nil {
			a = &acc{}
			steps[step] = a
		}
		return a
	}
	for _, d := range ds {
		a := get(d.Step)
		a.latencies = append(a.latencies, d.Latency(now))
		a.wait += d.WaitAt(now)
		a.work += d.WorkAt(now)
		a.cost += d.CostUSD
	}
	for _, at := range attempts {
		if at.Failed() {
			get(at.Step).failed++
		}
	}
	out := []StepReport{}
	for _, name := range slices.Sorted(maps.Keys(steps)) {
		a := steps[name]
		sr := StepReport{Step: name, Deliveries: len(a.latencies), FailedAttempts: a.failed, CostUSD: round6(a.cost)}
		if n := len(a.latencies); n > 0 {
			sr.P50Seconds = seconds(percentile(a.latencies, 0.5))
			sr.P95Seconds = seconds(percentile(a.latencies, 0.95))
			sr.MaxSeconds = seconds(percentile(a.latencies, 1))
			sr.WaitSeconds = seconds(a.wait / time.Duration(n))
			sr.WorkSeconds = seconds(a.work / time.Duration(n))
		}
		out = append(out, sr)
	}
	slices.SortStableFunc(out, func(a, b StepReport) int {
		switch {
		case a.P95Seconds > b.P95Seconds:
			return -1
		case a.P95Seconds < b.P95Seconds:
			return 1
		}
		return 0
	})
	return out
}

// runFailures groups a run's failed attempts by step, class and normalised message.
func runFailures(ds []store.Delivery, attempts []Attempt) []FailureCluster {
	states := map[string]store.DeliveryState{}
	for _, d := range ds {
		states[d.ID] = d.State
	}
	byKey := map[string]*FailureCluster{}
	recovered := map[string]map[string]bool{}
	for _, a := range attempts {
		if !a.Failed() {
			continue
		}
		msg := Normalise(a.Error)
		k := a.Step + "\x00" + a.Class + "\x00" + msg
		c := byKey[k]
		if c == nil {
			c = &FailureCluster{Step: a.Step, Class: a.Class, Message: msg, Example: a.Error}
			byKey[k], recovered[k] = c, map[string]bool{}
		}
		c.Count++
		if states[a.Delivery] == store.DeliveryDone {
			recovered[k][a.Delivery] = true
		}
	}
	out := []FailureCluster{}
	for _, k := range slices.Sorted(maps.Keys(byKey)) {
		c := byKey[k]
		c.Recovered = len(recovered[k])
		out = append(out, *c)
	}
	slices.SortStableFunc(out, func(a, b FailureCluster) int { return b.Count - a.Count })
	return out
}

// compare sets the run against the median of the past runs, and gives a running run its ETA.
func compare(run store.Run, r *Report, past []store.Run, attempts []Attempt, published []published,
	now time.Time,
) (*Comparison, *time.Time) {
	if len(past) == 0 {
		return nil, nil
	}
	items, failed := map[string]float64{}, map[string]float64{}
	for _, p := range published {
		items[p.RunID]++
	}
	for _, a := range attempts {
		if a.Failed() {
			failed[a.RunID]++
		}
	}
	var durations, costs, counts, failures []float64
	for _, p := range past {
		durations = append(durations, runDuration(p, now).Seconds())
		costs = append(costs, p.CostUSD)
		counts = append(counts, items[p.ID])
		failures = append(failures, failed[p.ID])
	}
	c := &Comparison{
		Runs:           len(past),
		Duration:       Figure{Value: r.DurationSeconds, Median: round(median(durations))},
		Cost:           Figure{Value: r.CostUSD, Median: round6(median(costs))},
		Items:          Figure{Value: items[run.ID], Median: median(counts)},
		FailedAttempts: Figure{Value: failed[run.ID], Median: median(failures)},
	}
	if !run.SettledAt.IsZero() {
		return c, nil
	}
	eta := run.StartedAt.Add(time.Duration(median(durations) * float64(time.Second))).UTC()
	return c, &eta
}

func median(vs []float64) float64 {
	s := slices.Clone(vs)
	slices.Sort(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// WriteText writes the report for people: the run, its critical path, its slowest steps, its failures by cluster
// and the comparison with earlier runs.
func (r *Report) WriteText(w io.Writer, now time.Time) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "Run %s of %s (%s): %s\n", r.Run, r.Pipeline, r.Kind, r.State)
	settled := "not settled"
	if r.SettledAt != nil {
		settled = "settled " + r.SettledAt.UTC().Format(time.RFC3339)
	}
	fmt.Fprintf(tw, "Started %s, %s, %s, cost $%s\n", r.StartedAt.UTC().Format(time.RFC3339), settled,
		dur(r.DurationSeconds), money(r.CostUSD))
	if r.ETA != nil {
		if r.ETA.After(now) {
			fmt.Fprintf(tw, "ETA %s (in %s)\n", r.ETA.Format(time.RFC3339), r.ETA.Sub(now).Round(time.Second))
		} else {
			fmt.Fprintf(tw, "ETA %s: overdue by %s\n", r.ETA.Format(time.RFC3339), now.Sub(*r.ETA).Round(time.Second))
		}
	}
	streams := slices.Sorted(maps.Keys(r.Items))
	parts := make([]string, len(streams))
	for i, s := range streams {
		parts[i] = s + " " + strconv.Itoa(r.Items[s])
	}
	fmt.Fprintf(tw, "Items: %s\n", orNone(strings.Join(parts, ", ")))

	fmt.Fprintf(tw, "\nCritical path (%s):\n", dur(r.CriticalPath.TotalSeconds))
	fmt.Fprintln(tw, "  STEP\tDELIVERY\tLATENCY\tWAIT\tWORK\tBACKOFF\tSHARE")
	for _, s := range r.CriticalPath.Steps {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\t%s\n", s.Step, s.Delivery, dur(s.LatencySeconds),
			dur(s.WaitSeconds), dur(s.WorkSeconds), dur(s.BackoffSeconds), pct(s.Share))
	}

	fmt.Fprintln(tw, "\nSlowest steps:")
	fmt.Fprintln(tw, "  STEP\tDELIVERIES\tFAILED\tP50\tP95\tMAX\tWAIT\tWORK\tCOST")
	for _, s := range r.Steps {
		fmt.Fprintf(tw, "  %s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t$%s\n", s.Step, s.Deliveries, s.FailedAttempts,
			dur(s.P50Seconds), dur(s.P95Seconds), dur(s.MaxSeconds), dur(s.WaitSeconds), dur(s.WorkSeconds),
			money(s.CostUSD))
	}

	fmt.Fprintln(tw, "\nFailures by cluster:")
	if len(r.Failures) == 0 {
		fmt.Fprintln(tw, "  none")
	} else {
		fmt.Fprintln(tw, "  STEP\tCLASS\tCOUNT\tRECOVERED\tMESSAGE")
		for _, f := range r.Failures {
			fmt.Fprintf(tw, "  %s\t%s\t%d\t%d\t%s\n", f.Step, f.Class, f.Count, f.Recovered, f.Message)
		}
	}

	if c := r.Comparison; c != nil {
		fmt.Fprintf(tw, "\nCompared with the median of the last %d runs:\n", c.Runs)
		fmt.Fprintln(tw, "  FIGURE\tTHIS RUN\tMEDIAN\tCHANGE")
		fmt.Fprintf(tw, "  duration\t%s\t%s\t%s\n", dur(c.Duration.Value), dur(c.Duration.Median), change(c.Duration))
		fmt.Fprintf(tw, "  cost\t$%s\t$%s\t%s\n", money(c.Cost.Value), money(c.Cost.Median), change(c.Cost))
		fmt.Fprintf(tw, "  items\t%s\t%s\t%s\n", num(c.Items.Value), num(c.Items.Median), change(c.Items))
		fmt.Fprintf(tw, "  failed attempts\t%s\t%s\t%s\n", num(c.FailedAttempts.Value), num(c.FailedAttempts.Median),
			change(c.FailedAttempts))
	} else {
		fmt.Fprintln(tw, "\nNo earlier run to compare with.")
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

func dur(s float64) string {
	return (time.Duration(s * float64(time.Second))).Round(time.Millisecond).String()
}

func money(v float64) string { return strconv.FormatFloat(v, 'f', 4, 64) }

func num(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

func pct(v float64) string { return strconv.FormatFloat(v*100, 'f', 0, 64) + "%" }

// change is the run's value against the median, as a signed percentage; "-" when the median is zero.
func change(f Figure) string {
	if f.Median == 0 {
		return "-"
	}
	return fmt.Sprintf("%+.0f%%", (f.Value-f.Median)/f.Median*100)
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
