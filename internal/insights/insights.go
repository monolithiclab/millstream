// Package insights is the insights projector and the run reports (spec section 11.1): deterministic analysis of the
// log store, no LLM involved. The projector reads a pipeline's deliveries (their timings), the events that end
// attempts (their errors, costs and the actions' metrics), its review decisions and samples, and the critical paths
// of its recent runs, the lineage of the items its filters and reviewers discarded or decided, and the fallback
// audits that ended, and finds the kinds this build computes: slowStep, failureCluster, cost, reviewPattern, waste,
// distillable, unschedulable and drift (from audits). Each insight's id is a fingerprint
// of what it is about, so the store updates one row for a recurring problem and resolves it once the projector no
// longer finds it.
//
// Windows: an insight covers the last 24 hours (Window); regressions compare it with the 7 days before (Baseline).
package insights

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/metrics"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Windows and thresholds.
const (
	// Window is the period an insight covers, up to the projector's instant.
	Window = 24 * time.Hour
	// Baseline is the period before the window that regressions compare with.
	Baseline = 7 * 24 * time.Hour
	// MaxSamples is the most sample item ids an insight's evidence lists.
	MaxSamples = 20
	// pathRuns is how many of the runs settled in the window slowStep reads the critical paths of, newest first.
	pathRuns = 10
	// pathShare is the share of the critical path a step must exceed to be a slowStep.
	pathShare = 0.5
	// regressionFactor and minRegression: a p95 latency at least this many times the baseline's, and at least this
	// much longer, is a regression; each side needs minSamples deliveries.
	regressionFactor = 1.5
	minRegression    = time.Second
	minSamples       = 5
	// schemaFailureShare is the share of an agent step's attempts failing schema validation from which its cost
	// insight is a warning.
	schemaFailureShare = 0.1
)

// Severities.
const (
	SeverityInfo    = pipeline.SeverityInfo
	SeverityWarning = pipeline.SeverityWarning
	SeverityError   = pipeline.SeverityError
)

// Kinds are the insight kinds this build computes.
var Kinds = []string{pipeline.InsightSlowStep, pipeline.InsightFailureCluster, pipeline.InsightCost,
	pipeline.InsightReviewPattern, pipeline.InsightWaste, pipeline.InsightDistillable, pipeline.InsightUnschedulable,
	pipeline.InsightDrift}

// Data is what the projector reads of one pipeline.
type Data struct {
	Config
	Pipeline string
	Now      time.Time
	// Deliveries were created in the window or the baseline before it.
	Deliveries []store.Delivery
	// Attempts ended in the window or the baseline, in order.
	Attempts []Attempt
	// Paths are the critical paths of the runs settled in the window, at most 10, newest first.
	Paths []Path
	// Decisions are the review decisions and Samples the review samples' outcomes recorded in the window or the
	// baseline, in order.
	Decisions []Decision
	Samples   []SampleOutcome
	// Audits are the fallback audits that ended in the window or the baseline, in order.
	Audits []AuditOutcome
	// Items are the items the lineage walks need, by id (loadLineage).
	Items map[string]store.Item
	// payloads caches the items' decoded payloads (value).
	payloads map[string]any
}

// PendingStep is a step whose pending deliveries no registered node can take: Count of them, waiting for a node
// that offers what Needs describes.
type PendingStep struct {
	Step  string
	Count int64
	Needs string
}

// ClassNoNode is the class of an unschedulable insight about pending deliveries no registered node can take, beside
// the classes of the attempts no runtime could take (metrics.UnschedulableClasses).
const ClassNoNode = "noNode"

func (d *Data) windowStart() time.Time { return d.Now.Add(-Window) }

// Load reads what the projector needs of a pipeline at now.
func Load(ctx context.Context, st store.Store, pipelineName string, now time.Time, cfg Config) (*Data, error) {
	cfg.DistillMinDeliveries = cmp.Or(cfg.DistillMinDeliveries, DefaultDistillMinDeliveries)
	cfg.FilterMaxFalseDrop = cmp.Or(cfg.FilterMaxFalseDrop, DefaultFilterMaxFalseDrop)
	d := &Data{Config: cfg, Pipeline: pipelineName, Now: now}
	since := now.Add(-Window - Baseline)
	var err error
	if d.Deliveries, err = st.Deliveries(ctx, store.DeliveryFilter{Pipeline: pipelineName, Since: since}); err != nil {
		return nil, fmt.Errorf("insights of %s: %w", pipelineName, err)
	}
	if d.Attempts, _, err = readAttempts(ctx, st, pipelineName, since); err != nil {
		return nil, fmt.Errorf("insights of %s: %w", pipelineName, err)
	}
	if d.Decisions, d.Samples, err = readReviews(ctx, st, pipelineName, since); err != nil {
		return nil, fmt.Errorf("insights of %s: %w", pipelineName, err)
	}
	if d.Audits, err = readAudits(ctx, st, pipelineName, since); err != nil {
		return nil, fmt.Errorf("insights of %s: %w", pipelineName, err)
	}
	runs, err := st.Runs(ctx, store.RunFilter{Pipeline: pipelineName, Limit: 200})
	if err != nil {
		return nil, fmt.Errorf("insights of %s: %w", pipelineName, err)
	}
	runs = slices.DeleteFunc(runs, func(r store.Run) bool {
		return r.SettledAt.IsZero() || r.SettledAt.Before(d.windowStart()) || r.SettledAt.After(now)
	})
	slices.SortStableFunc(runs, func(a, b store.Run) int { return b.SettledAt.Compare(a.SettledAt) })
	for _, r := range runs[:min(len(runs), pathRuns)] {
		p, err := runPath(ctx, st, r, now)
		if err != nil {
			return nil, fmt.Errorf("insights of %s: %w", pipelineName, err)
		}
		d.Paths = append(d.Paths, p)
	}
	if err := d.loadLineage(ctx, st); err != nil {
		return nil, fmt.Errorf("insights of %s: %w", pipelineName, err)
	}
	return d, nil
}

// runPath reads a run's deliveries and items and returns its critical path.
func runPath(ctx context.Context, st store.Store, r store.Run, now time.Time) (Path, error) {
	ds, err := st.Deliveries(ctx, store.DeliveryFilter{Pipeline: r.Pipeline, RunID: r.ID})
	if err != nil {
		return Path{}, err
	}
	items, err := st.Items(ctx, store.ItemFilter{Pipeline: r.Pipeline, RunID: r.ID})
	if err != nil {
		return Path{}, err
	}
	return CriticalPath(ds, items, now), nil
}

// Project runs the projector for a pipeline at now and records what it finds for the kinds its insights key leaves
// on. With insights off it records nothing, and the insights already recorded keep their state.
func Project(ctx context.Context, st store.Store, p *pipeline.Pipeline, now time.Time, cfg Config) error {
	keys, err := p.InsightsConfig()
	if err != nil {
		return fmt.Errorf("insights of %s: %w", p.Name, err)
	}
	kinds := keys.Kinds(Kinds)
	if len(kinds) == 0 {
		return nil
	}
	if cfg.Steps == nil {
		cfg.Steps = StepsOf(p, nil)
	}
	d, err := Load(ctx, st, p.Name, now, cfg)
	if err != nil {
		return err
	}
	return st.RecordInsights(ctx, store.InsightReport{Pipeline: p.Name, Kinds: kinds, Insights: Compute(d, kinds)})
}

// Compute returns the insights of the kinds named found in d, by kind then scope.
func Compute(d *Data, kinds []string) []store.Insight {
	var out []store.Insight
	for _, k := range kinds {
		var found []store.Insight
		switch k {
		case pipeline.InsightSlowStep:
			found = slowSteps(d)
		case pipeline.InsightFailureCluster:
			found = failureClusters(d)
		case pipeline.InsightCost:
			found = costs(d)
		case pipeline.InsightReviewPattern:
			found = reviewPatterns(d)
		case pipeline.InsightWaste:
			found = wastes(d)
		case pipeline.InsightDistillable:
			found = distillables(d)
		case pipeline.InsightUnschedulable:
			found = unschedulable(d)
		case pipeline.InsightDrift:
			found = drifts(d)
		}
		out = append(out, found...)
	}
	return out
}

// insight builds an insight of d's window.
func (d *Data) insight(kind, scope, severity string, evidence map[string]any, id ...string) store.Insight {
	body, err := json.Marshal(evidence)
	if err != nil { // evidence holds finite numbers, strings, times and lists of them: this does not happen
		body = []byte(`{"error":` + strconv.Quote("encode evidence: "+err.Error()) + `}`)
	}
	return store.Insight{
		Pipeline: d.Pipeline, ID: Fingerprint(kind, d.Pipeline, id...), Kind: kind, Scope: scope,
		WindowStart: d.windowStart(), WindowEnd: d.Now, Severity: severity, Evidence: body,
	}
}

// slowSteps finds the step holding more than half of the recent runs' critical paths, with how their time splits
// into queue wait, work and backoff, and the steps whose p95 latency regressed against the baseline.
func slowSteps(d *Data) []store.Insight {
	type share struct{ latency, wait, work, backoff time.Duration }
	onPath := map[string]*share{}
	var total time.Duration
	samples := map[string][]string{}
	for _, p := range d.Paths {
		total += p.Total
		for _, s := range p.Steps {
			sh := onPath[s.Step]
			if sh == nil {
				sh = &share{}
				onPath[s.Step] = sh
			}
			sh.latency += s.Latency
			sh.wait += s.Wait
			sh.work += s.Work
			sh.backoff += s.Backoff
			samples[s.Step] = appendSample(samples[s.Step], s.Item)
		}
	}
	window, baseline := map[string][]time.Duration{}, map[string][]time.Duration{}
	for _, dl := range d.Deliveries {
		switch {
		case dl.FinishedAt.IsZero() || dl.FinishedAt.After(d.Now):
		case !dl.FinishedAt.Before(d.windowStart()):
			window[dl.Step] = append(window[dl.Step], dl.Latency(d.Now))
		case !dl.FinishedAt.Before(d.windowStart().Add(-Baseline)):
			baseline[dl.Step] = append(baseline[dl.Step], dl.Latency(d.Now))
		}
	}
	steps := slices.Sorted(maps.Keys(onPath))
	for s := range window {
		if _, ok := onPath[s]; !ok {
			steps = append(steps, s)
		}
	}
	slices.Sort(steps)
	var out []store.Insight
	for _, step := range steps {
		ev := map[string]any{"runs": len(d.Paths)}
		bottleneck := false
		if sh := onPath[step]; sh != nil && total > 0 && sh.latency > 0 {
			frac := ratio(sh.latency, total)
			ev["criticalPathShare"] = round(frac)
			ev["queueWaitShare"] = round(ratio(sh.wait, sh.latency))
			ev["workShare"] = round(ratio(sh.work, sh.latency))
			ev["backoffShare"] = round(ratio(sh.backoff, sh.latency))
			ev["slowestPart"] = slowestPart(sh.wait, sh.work, sh.backoff)
			bottleneck = len(onPath) > 1 && frac > pathShare
		}
		regression := false
		if w := window[step]; len(w) > 0 {
			p95 := Percentile(w, 0.95)
			ev["deliveries"], ev["p95Seconds"] = len(w), seconds(p95)
			if b := baseline[step]; len(b) >= minSamples {
				base := Percentile(b, 0.95)
				ev["baselineP95Seconds"] = seconds(base)
				regression = len(w) >= minSamples && float64(p95) >= regressionFactor*float64(base) &&
					p95-base >= minRegression
			}
		}
		if !bottleneck && !regression {
			continue
		}
		ev["regression"] = regression
		ev["samples"] = nonNil(samples[step])
		severity := SeverityInfo
		if regression {
			severity = SeverityWarning
		}
		out = append(out, d.insight(pipeline.InsightSlowStep, step, severity, ev, step))
	}
	return out
}

// slowestPart names the largest part of a step's time on the critical path: queue (raise concurrency), work (the
// action is slow) or backoff (retries).
func slowestPart(wait, work, backoff time.Duration) string {
	switch {
	case wait >= work && wait >= backoff:
		return "queue"
	case work >= backoff:
		return "work"
	}
	return "backoff"
}

// cluster is a group of failed attempts.
type cluster struct {
	step, class, message, example string
	count                         int // in the window
	first, last                   time.Time
	deliveries                    []string // in the window, distinct
	samples                       []string
}

// clusters groups the failed attempts that keep pass by key, counting those in the window and seeing all of them
// for first and last seen. Only groups with attempts in the window are returned, by key.
func (d *Data) clusters(keep func(Attempt) bool, key func(Attempt) []string) []*cluster {
	items := map[string]string{}
	for _, dl := range d.Deliveries {
		items[dl.ID] = dl.ItemID
	}
	byKey := map[string]*cluster{}
	for _, a := range d.Attempts {
		if !a.Failed() || !keep(a) {
			continue
		}
		parts := key(a)
		k := fmt.Sprint(parts)
		c := byKey[k]
		if c == nil {
			c = &cluster{step: a.Step, class: a.Class, message: Normalise(a.Error), example: a.Error, first: a.TS}
			byKey[k] = c
		}
		c.last = a.TS
		if a.TS.Before(d.windowStart()) || a.TS.After(d.Now) {
			continue
		}
		c.count++
		if !slices.Contains(c.deliveries, a.Delivery) {
			c.deliveries = append(c.deliveries, a.Delivery)
			if it := items[a.Delivery]; it != "" {
				c.samples = appendSample(c.samples, it)
			}
		}
	}
	var out []*cluster
	for _, k := range slices.Sorted(maps.Keys(byKey)) {
		if byKey[k].count > 0 {
			out = append(out, byKey[k])
		}
	}
	return out
}

// failureClusters groups the failed attempts of the window by step, class and normalised message.
func failureClusters(d *Data) []store.Insight {
	attempts := map[string]int{} // per step, in the window
	for _, a := range d.Attempts {
		if !a.TS.Before(d.windowStart()) && !a.TS.After(d.Now) && a.Outcome != store.EventDeliveryCancelled {
			attempts[a.Step]++
		}
	}
	states := map[string]store.DeliveryState{}
	for _, dl := range d.Deliveries {
		states[dl.ID] = dl.State
	}
	keep := func(a Attempt) bool { return !metrics.UnschedulableClasses[a.Class] }
	key := func(a Attempt) []string { return []string{a.Step, a.Class, Normalise(a.Error)} }
	var out []store.Insight
	for _, c := range d.clusters(keep, key) {
		terminal, recovered, dead := 0, 0, 0
		for _, id := range c.deliveries {
			switch states[id] {
			case store.DeliveryDone:
				terminal++
				recovered++
			case store.DeliveryDead:
				terminal++
				dead++
			case store.DeliveryFiltered, store.DeliveryCancelled:
				terminal++
			}
		}
		ev := map[string]any{
			"class": c.class, "message": c.message, "example": c.example, "count": c.count,
			"rate": round(float64(c.count) / float64(max(attempts[c.step], 1))), "firstSeen": c.first, "lastSeen": c.last,
			"deliveries": len(c.deliveries), "dead": dead, "samples": nonNil(c.samples), "retrySuccessShare": nil,
		}
		severity := SeverityWarning
		if terminal > 0 {
			share := float64(recovered) / float64(terminal)
			ev["retrySuccessShare"] = round(share)
			if share < 0.5 { // mostly deterministic: retries do not fix it
				severity = SeverityError
			}
		}
		out = append(out, d.insight(pipeline.InsightFailureCluster, c.step, severity, ev, c.step, c.class, c.message))
	}
	return out
}

// unschedulable groups the attempts no runtime could take, by step and class, and adds the steps whose pending
// deliveries no registered node can take (class noNode).
func unschedulable(d *Data) []store.Insight {
	keep := func(a Attempt) bool { return metrics.UnschedulableClasses[a.Class] }
	key := func(a Attempt) []string { return []string{a.Step, a.Class} }
	var out []store.Insight
	for _, c := range d.clusters(keep, key) {
		ev := map[string]any{
			"class": c.class, "example": c.example, "count": c.count, "deliveries": len(c.deliveries),
			"firstSeen": c.first, "lastSeen": c.last, "samples": nonNil(c.samples),
		}
		out = append(out, d.insight(pipeline.InsightUnschedulable, c.step, SeverityError, ev, c.step, c.class))
	}
	for _, p := range d.Pending {
		ev := map[string]any{"class": ClassNoNode, "example": "no registered node offers " + p.Needs,
			"count": p.Count, "deliveries": p.Count, "needs": p.Needs}
		out = append(out, d.insight(pipeline.InsightUnschedulable, p.Step, SeverityError, ev, p.Step, ClassNoNode))
	}
	return out
}

// spend is what a step's attempts cost over a period, by model.
type spend struct {
	cost       float64
	outputs    int
	deliveries map[string]bool // with harness metrics
	turns      float64
	models     map[string]*modelSpend
	samples    []string
}

type modelSpend struct {
	attempts                                 int
	cost, tokensIn, tokensOut, turns, schema float64
}

func (s *spend) model(name string) *modelSpend {
	m := s.models[name]
	if m == nil {
		m = &modelSpend{}
		s.models[name] = m
	}
	return m
}

// costs reports the spend of every step that spent in the window: cost per output and per item the pipeline's
// review steps approved in the window, turns per delivery and schema-validation failures, by model. It is a warning when the cost per output regressed against the baseline or
// schema validation fails often.
func costs(d *Data) []store.Insight {
	items := map[string]string{}
	for _, dl := range d.Deliveries {
		items[dl.ID] = dl.ItemID
	}
	window, baseline := map[string]*spend{}, map[string]*spend{}
	for _, a := range d.Attempts {
		var period map[string]*spend
		switch {
		case a.TS.After(d.Now) || a.TS.Before(d.windowStart().Add(-Baseline)):
			continue
		case a.TS.Before(d.windowStart()):
			period = baseline
		default:
			period = window
		}
		s := period[a.Step]
		if s == nil {
			s = &spend{deliveries: map[string]bool{}, models: map[string]*modelSpend{}}
			period[a.Step] = s
		}
		s.cost += a.CostUSD
		if a.Outcome == store.EventDeliveryDone {
			s.outputs += a.Outputs
		}
		model, harness := "", false
		for _, m := range a.Metrics {
			if v, ok := m.Labels["model"]; ok {
				model = v
			}
			harness = harness || m.Name == agent.MetricTurns
		}
		if !harness && a.CostUSD == 0 {
			continue
		}
		ms := s.model(model)
		ms.attempts++
		ms.cost += a.CostUSD
		for _, m := range a.Metrics {
			switch m.Name {
			case agent.MetricTokensIn:
				ms.tokensIn += m.Value
			case agent.MetricTokensOut:
				ms.tokensOut += m.Value
			case agent.MetricTurns:
				ms.turns += m.Value
				s.turns += m.Value
				s.deliveries[a.Delivery] = true
			case agent.MetricSchemaFailures:
				ms.schema += m.Value
			}
		}
		if it := items[a.Delivery]; it != "" {
			s.samples = appendSample(s.samples, it)
		}
	}
	approved := d.approvals()
	var out []store.Insight
	for _, step := range slices.Sorted(maps.Keys(window)) {
		s := window[step]
		if len(s.models) == 0 {
			continue // no spend and no harness
		}
		ev := map[string]any{"costUsd": round6(s.cost), "outputs": s.outputs, "costPerOutput": nil,
			"costPerApprovedItem": nil, "baselineCostPerOutput": nil, "turnsPerDelivery": nil,
			"samples": nonNil(s.samples)}
		if approved > 0 {
			ev["costPerApprovedItem"] = round6(s.cost / float64(approved))
		}
		severity := SeverityInfo
		var perOutput float64
		if s.outputs > 0 {
			perOutput = s.cost / float64(s.outputs)
			ev["costPerOutput"] = round6(perOutput)
		}
		if b := baseline[step]; b != nil && b.outputs >= minSamples {
			base := b.cost / float64(b.outputs)
			ev["baselineCostPerOutput"] = round6(base)
			if s.outputs > 0 && base > 0 && perOutput >= regressionFactor*base {
				severity = SeverityWarning
			}
		}
		if len(s.deliveries) > 0 {
			ev["turnsPerDelivery"] = round(s.turns / float64(len(s.deliveries)))
		}
		byModel := map[string]any{}
		var attempts int
		var schemaFailures float64
		for name, m := range s.models {
			byModel[name] = map[string]any{"attempts": m.attempts, "costUsd": round6(m.cost), "tokensIn": m.tokensIn,
				"tokensOut": m.tokensOut, "turns": m.turns, "schemaFailures": m.schema}
			attempts += m.attempts
			schemaFailures += m.schema
		}
		ev["byModel"], ev["schemaFailures"] = byModel, schemaFailures
		if attempts > 0 && schemaFailures/float64(attempts) >= schemaFailureShare {
			severity = SeverityWarning
		}
		out = append(out, d.insight(pipeline.InsightCost, step, severity, ev, step))
	}
	return out
}

// Percentile returns the p-th percentile of ds (nearest rank); ds must not be empty.
func Percentile[T cmp.Ordered](ds []T, p float64) T {
	s := slices.Clone(ds)
	slices.Sort(s)
	rank := int(math.Ceil(p*float64(len(s)))) - 1
	return s[max(min(rank, len(s)-1), 0)]
}

func appendSample(samples []string, id string) []string {
	if len(samples) >= MaxSamples || slices.Contains(samples, id) {
		return samples
	}
	return append(samples, id)
}

// nonNil returns s, or an empty list for nil, so evidence lists encode as [] rather than null.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func ratio(a, b time.Duration) float64 {
	if b <= 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func seconds(d time.Duration) float64 { return round(d.Seconds()) }

// round keeps 3 decimals, round6 6 (dollars): evidence stays readable and stable.
func round(v float64) float64  { return math.Round(v*1e3) / 1e3 }
func round6(v float64) float64 { return math.Round(v*1e6) / 1e6 }

// Summary is a one-line account of an insight's evidence, for listings.
func Summary(in store.Insight) string {
	var ev map[string]any
	if err := json.Unmarshal(in.Evidence, &ev); err != nil {
		return "evidence unreadable: " + err.Error()
	}
	f := func(k string) string {
		switch v := ev[k].(type) {
		case nil:
			return "-"
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		default:
			return fmt.Sprint(v)
		}
	}
	p := func(k string) string {
		v, ok := ev[k].(float64)
		if !ok {
			return "-"
		}
		return strconv.FormatFloat(v*100, 'f', 0, 64) + "%"
	}
	switch in.Kind {
	case pipeline.InsightSlowStep:
		return fmt.Sprintf("%s of the critical path (queue %s, work %s, backoff %s); p95 %ss, baseline %ss",
			p("criticalPathShare"), p("queueWaitShare"), p("workShare"), p("backoffShare"), f("p95Seconds"),
			f("baselineP95Seconds"))
	case pipeline.InsightFailureCluster:
		return fmt.Sprintf("%s failures (%s of attempts), %s recovered on retry: %s: %s", f("count"), p("rate"),
			p("retrySuccessShare"), f("class"), f("message"))
	case pipeline.InsightCost:
		return fmt.Sprintf("$%s for %s outputs ($%s each, $%s per approved item), %s turns per delivery, %s schema "+
			"failures", f("costUsd"), f("outputs"), f("costPerOutput"), f("costPerApprovedItem"), f("turnsPerDelivery"),
			f("schemaFailures"))
	case pipeline.InsightUnschedulable:
		if f("class") == ClassNoNode {
			return fmt.Sprintf("%s deliveries waiting: %s", f("count"), f("example"))
		}
		return fmt.Sprintf("%s attempts no runtime can take: %s: %s", f("count"), f("class"), f("example"))
	case pipeline.InsightDrift:
		return fmt.Sprintf("the fallback disagreed with the primary on %s of %s audits (%s disagreed, %s failed)",
			p("disagreementShare"), f("audits"), f("disagreed"), f("failed"))
	case pipeline.InsightWaste:
		return fmt.Sprintf("%s items discarded downstream, $%s of $%s wasted (%s)", f("discardedItems"),
			f("wastedCostUsd"), f("costUsd"), p("wasteShare"))
	case pipeline.InsightDistillable:
		return fmt.Sprintf("candidate #%s for code: %s deliveries, %s bounded output, p95 %s turns, reviewer "+
			"agreement %s; saves about $%s a month", f("rank"), f("deliveries"), p("boundedShare"), f("turnsP95"),
			p("reviewerAgreement"), f("monthlySavingUsd"))
	case pipeline.InsightReviewPattern:
		return fmt.Sprintf("%s rejected, %s edited, %s auto-approved, sampled agreement %s, intervention %s, p50 "+
			"decision %ss", p("rejectionRate"), p("editRate"), p("autoApprovalRate"), p("sampledAgreement"),
			p("interventionRate"), f("timeToDecisionP50Seconds"))
	}
	return string(in.Evidence)
}
