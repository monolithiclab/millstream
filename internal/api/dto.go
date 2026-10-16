package api

import (
	"encoding/json"
	"maps"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// The resources the API answers with: the OpenAPI document's component schemas, one type each. Optional times
// are pointers, absent when zero.

// Page is one page of a list: Next, when set, is the cursor of the next page.
type Page[T any] struct {
	Items []T    `json:"items"`
	Next  string `json:"next,omitempty"`
}

// Pipeline is a pipeline with its active revision's graph and its state.
type Pipeline struct {
	Name            string      `json:"name"`
	Description     string      `json:"description,omitempty"`
	State           string      `json:"state"`
	Revision        int64       `json:"revision"`
	Steps           []Step      `json:"steps"`
	Triggers        []Trigger   `json:"triggers"`
	Streams         []string    `json:"streams"`
	LastRun         *RunSummary `json:"lastRun,omitempty"`
	OpenDeadLetters int         `json:"openDeadLetters"`
	OpenReviews     int         `json:"openReviews"`
}

// Step is a step of a revision, with its pause state when it is the active revision's.
type Step struct {
	Name        string   `json:"name"`
	Action      string   `json:"action"`
	Builtin     bool     `json:"builtin"`
	In          []string `json:"in"`
	Out         []string `json:"out"`
	DLQ         string   `json:"dlq"`
	Concurrency int      `json:"concurrency"`
	Paused      bool     `json:"paused"`
	PauseCause  string   `json:"pauseCause,omitempty"`
}

// Trigger is a trigger of a revision: cron, manual or webhook.
type Trigger struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Schedule string `json:"schedule,omitempty"`
	Stream   string `json:"stream"`
}

// Revision is an applied revision; Graph is set when one revision is asked for.
type Revision struct {
	ID          int64     `json:"id"`
	Pipeline    string    `json:"pipeline"`
	PackageHash string    `json:"packageHash"`
	AppliedAt   time.Time `json:"appliedAt"`
	Active      bool      `json:"active"`
	Graph       *Graph    `json:"graph,omitempty"`
}

// Graph is a revision's steps, triggers and streams.
type Graph struct {
	Description string    `json:"description,omitempty"`
	Steps       []Step    `json:"steps"`
	Triggers    []Trigger `json:"triggers"`
	Streams     []string  `json:"streams"`
}

// Stream is a stream of the active revision with the steps writing and reading it and its item count.
type Stream struct {
	Name      string   `json:"name"`
	Producers []string `json:"producers"`
	Consumers []string `json:"consumers"`
	Items     int64    `json:"items"`
}

// RunSummary is a run as a pipeline lists its last one.
type RunSummary struct {
	ID        string     `json:"id"`
	State     string     `json:"state"`
	StartedAt time.Time  `json:"startedAt"`
	SettledAt *time.Time `json:"settledAt,omitempty"`
}

// Run is a run record; Items and OpenDeadLetters are set when one run is asked for.
type Run struct {
	ID              string            `json:"id"`
	Pipeline        string            `json:"pipeline"`
	Revision        int64             `json:"revision"`
	Kind            string            `json:"kind"`
	Trigger         string            `json:"trigger,omitempty"`
	TriggerKey      string            `json:"triggerKey"`
	TriggerSource   string            `json:"triggerSource"`
	ScheduledAt     *time.Time        `json:"scheduledAt,omitempty"`
	Generation      int64             `json:"generation"`
	State           string            `json:"state"`
	StartedAt       time.Time         `json:"startedAt"`
	SettledAt       *time.Time        `json:"settledAt,omitempty"`
	CostUSD         float64           `json:"costUsd"`
	Vars            map[string]any    `json:"vars"`
	VarSources      map[string]string `json:"varSources"`
	Items           map[string]int64  `json:"items,omitempty"`
	OpenDeadLetters *int              `json:"openDeadLetters,omitempty"`
}

// Item is an item envelope with its payload.
type Item struct {
	ID         string              `json:"id"`
	Pipeline   string              `json:"pipeline"`
	RunID      string              `json:"runId"`
	Stream     string              `json:"stream"`
	Step       string              `json:"step"`
	Key        string              `json:"key"`
	Parents    []string            `json:"parents"`
	Generation int64               `json:"generation"`
	TS         time.Time           `json:"ts"`
	Payload    json.RawMessage     `json:"payload"`
	Artifacts  []store.ArtifactRef `json:"artifacts"`
}

// LineageNode is an item of a lineage, without its payload.
type LineageNode struct {
	ID      string    `json:"id"`
	RunID   string    `json:"runId"`
	Stream  string    `json:"stream"`
	Step    string    `json:"step"`
	Key     string    `json:"key"`
	Parents []string  `json:"parents"`
	TS      time.Time `json:"ts"`
}

// Lineage is an item's ancestors (up) and descendants (down), nearest first; Truncated says a bound cut it.
type Lineage struct {
	Item      string        `json:"item"`
	Up        []LineageNode `json:"up"`
	Down      []LineageNode `json:"down"`
	Truncated bool          `json:"truncated"`
}

// Delivery is a delivery record; Attempts is set when one delivery is asked for.
type Delivery struct {
	ID             string     `json:"id"`
	Pipeline       string     `json:"pipeline"`
	RunID          string     `json:"runId"`
	Step           string     `json:"step"`
	ItemID         string     `json:"itemId"`
	Generation     int64      `json:"generation"`
	State          string     `json:"state"`
	Attempt        int        `json:"attempt"`
	LeaseOwner     string     `json:"leaseOwner,omitempty"`
	LeaseExpiresAt *time.Time `json:"leaseExpiresAt,omitempty"`
	NextAttemptAt  *time.Time `json:"nextAttemptAt,omitempty"`
	ErrorClass     string     `json:"errorClass,omitempty"`
	ErrorMessage   string     `json:"errorMessage,omitempty"`
	FilterReason   string     `json:"filterReason,omitempty"`
	CostUSD        float64    `json:"costUsd"`
	CreatedAt      time.Time  `json:"createdAt"`
	LeasedAt       *time.Time `json:"leasedAt,omitempty"`
	FinishedAt     *time.Time `json:"finishedAt,omitempty"`
	WaitMs         int64      `json:"waitMs"`
	WorkMs         int64      `json:"workMs"`
	Attempts       []Attempt  `json:"attempts,omitempty"`
}

// Attempt is one attempt of a delivery, read from its events, with the artifacts it kept (an agent's transcript).
type Attempt struct {
	Attempt    int                 `json:"attempt"`
	Owner      string              `json:"owner,omitempty"`
	LeasedAt   *time.Time          `json:"leasedAt,omitempty"`
	EndedAt    *time.Time          `json:"endedAt,omitempty"`
	Outcome    string              `json:"outcome"`
	ErrorClass string              `json:"errorClass,omitempty"`
	Error      string              `json:"error,omitempty"`
	CostUSD    float64             `json:"costUsd"`
	WorkMs     int64               `json:"workMs"`
	Artifacts  []store.ArtifactRef `json:"artifacts"`
}

// DeadLetterGroup is the dead letters of one step that share an error fingerprint.
type DeadLetterGroup struct {
	Fingerprint string       `json:"fingerprint"`
	Step        string       `json:"step"`
	Kind        string       `json:"kind"`
	Error       string       `json:"error"`
	Count       int          `json:"count"`
	FirstSeen   time.Time    `json:"firstSeen"`
	LastSeen    time.Time    `json:"lastSeen"`
	DeadLetters []DeadLetter `json:"deadLetters"`
}

// DeadLetter is a dead-letter record.
type DeadLetter struct {
	ID             string    `json:"id"`
	RunID          string    `json:"runId"`
	OriginDelivery string    `json:"originDelivery"`
	Stream         string    `json:"stream"`
	State          string    `json:"state"`
	Error          string    `json:"error"`
	CreatedAt      time.Time `json:"createdAt"`
}

// MetricSeries is one metric's minutes for one group, the label sets of the group added up.
type MetricSeries struct {
	Name   string        `json:"name"`
	Group  string        `json:"group"`
	Points []MetricPoint `json:"points"`
}

// MetricPoint is one minute of a series.
type MetricPoint struct {
	T     time.Time `json:"t"`
	Count int64     `json:"count"`
	Sum   float64   `json:"sum"`
	Min   float64   `json:"min"`
	Max   float64   `json:"max"`
}

// Review is a review with its item.
type Review struct {
	Pipeline   string          `json:"pipeline"`
	DeliveryID string          `json:"deliveryId"`
	RunID      string          `json:"runId"`
	Step       string          `json:"step"`
	Inbox      string          `json:"inbox"`
	State      string          `json:"state"`
	Version    int64           `json:"version"`
	ParkedAt   time.Time       `json:"parkedAt"`
	Deadline   *time.Time      `json:"deadline,omitempty"`
	Editable   []string        `json:"editable"`
	Reasons    []string        `json:"reasons"`
	Display    string          `json:"display"`
	Decision   string          `json:"decision,omitempty"`
	Reviewer   string          `json:"reviewer,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	Text       string          `json:"text,omitempty"`
	Patch      json.RawMessage `json:"patch,omitempty"`
	DecidedAt  *time.Time      `json:"decidedAt,omitempty"`
	Item       Item            `json:"item"`
}

// Sample is a review sample with its item.
type Sample struct {
	Pipeline   string     `json:"pipeline"`
	DeliveryID string     `json:"deliveryId"`
	RunID      string     `json:"runId"`
	Step       string     `json:"step"`
	Inbox      string     `json:"inbox"`
	State      string     `json:"state"`
	Display    string     `json:"display"`
	CreatedAt  time.Time  `json:"createdAt"`
	Deadline   *time.Time `json:"deadline,omitempty"`
	Decision   string     `json:"decision,omitempty"`
	Reviewer   string     `json:"reviewer,omitempty"`
	Reason     string     `json:"reason,omitempty"`
	DecidedAt  *time.Time `json:"decidedAt,omitempty"`
	Item       Item       `json:"item"`
}

// Insight is an insight with a one-line summary of its evidence.
type Insight struct {
	Pipeline    string          `json:"pipeline"`
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Scope       string          `json:"scope"`
	Severity    string          `json:"severity"`
	State       string          `json:"state"`
	Summary     string          `json:"summary"`
	WindowStart time.Time       `json:"windowStart"`
	WindowEnd   time.Time       `json:"windowEnd"`
	OpenedAt    time.Time       `json:"openedAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
	Evidence    json.RawMessage `json:"evidence"`
}

// StepLessons is a step's lessons file in the active revision.
type StepLessons struct {
	Step     string                   `json:"step"`
	Rules    []pipeline.LessonRule    `json:"rules"`
	Examples []pipeline.LessonExample `json:"examples"`
}

// Nodes is the engine and the nodes it serves.
type Nodes struct {
	Engine EngineInfo `json:"engine"`
	Nodes  []Node     `json:"nodes"`
}

// EngineInfo is the engine serving the API: the active one, in M3 the only one.
type EngineInfo struct {
	State   string `json:"state"`
	Version string `json:"version"`
}

// Node is a node the engine serves; Leases is how many deliveries it holds now.
type Node struct {
	ID       string            `json:"id"`
	Embedded bool              `json:"embedded"`
	Version  string            `json:"version"`
	Slots    int               `json:"slots"`
	Leases   int               `json:"leases"`
	Labels   map[string]string `json:"labels"`
	Runtimes []string          `json:"runtimes"`
}

// Count is how many records an action changed.
type Count struct {
	Count int `json:"count"`
}

// timePtr is t, or nil when zero.
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// orEmpty is s, or an empty slice for nil: lists are [] in JSON, never null.
func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func toRun(r store.Run) Run {
	return Run{
		ID: r.ID, Pipeline: r.Pipeline, Revision: int64(r.Revision), Kind: string(r.Kind), Trigger: r.Trigger,
		TriggerKey: r.TriggerKey, TriggerSource: string(r.TriggerSource), ScheduledAt: timePtr(r.ScheduledAt),
		Generation: r.Generation, State: string(r.State), StartedAt: r.StartedAt, SettledAt: timePtr(r.SettledAt),
		CostUSD: r.CostUSD, Vars: nonNilMap(r.Vars), VarSources: nonNilMap(r.VarSources),
	}
}

// nonNilMap is m, or an empty map for nil: objects are {} in JSON, never null.
func nonNilMap[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}
	return m
}

func toRevision(rev store.Revision, active store.RevisionID) Revision {
	return Revision{ID: int64(rev.ID), Pipeline: rev.Pipeline, PackageHash: rev.PackageHash, AppliedAt: rev.AppliedAt,
		Active: rev.ID == active}
}

func toItem(it store.Item) Item {
	payload := it.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	return Item{
		ID: it.ID, Pipeline: it.Pipeline, RunID: it.RunID, Stream: it.Stream, Step: it.Step, Key: it.Key,
		Parents: orEmpty(it.Parents), Generation: it.Generation, TS: it.TS, Payload: payload,
		Artifacts: orEmpty(it.Artifacts),
	}
}

func toLineageNode(it store.Item) LineageNode {
	return LineageNode{ID: it.ID, RunID: it.RunID, Stream: it.Stream, Step: it.Step, Key: it.Key,
		Parents: orEmpty(it.Parents), TS: it.TS}
}

func toDelivery(d store.Delivery) Delivery {
	out := Delivery{
		ID: d.ID, Pipeline: d.Pipeline, RunID: d.RunID, Step: d.Step, ItemID: d.ItemID, Generation: d.Generation,
		State: string(d.State), Attempt: d.Attempt, LeaseOwner: d.LeaseOwner, ErrorClass: d.ErrorClass,
		ErrorMessage: d.ErrorMessage, FilterReason: d.FilterReason, CostUSD: d.CostUSD, CreatedAt: d.CreatedAt,
		LeasedAt: timePtr(d.LeasedAt), FinishedAt: timePtr(d.FinishedAt), WaitMs: d.Wait.Milliseconds(),
		WorkMs: d.Work.Milliseconds(),
	}
	if d.State == store.DeliveryLeased {
		out.LeaseExpiresAt = timePtr(d.LeaseExpiresAt)
	}
	if d.State == store.DeliveryPending {
		out.NextAttemptAt = timePtr(d.NextAttemptAt)
	}
	return out
}

func toReview(r store.Review) Review {
	out := Review{
		Pipeline: r.Pipeline, DeliveryID: r.DeliveryID, RunID: r.RunID, Step: r.Step, Inbox: r.Spec.Inbox,
		State: string(r.State), Version: r.Version, ParkedAt: r.ParkedAt, Deadline: timePtr(r.Deadline),
		Editable: orEmpty(r.Spec.Editable), Reasons: orEmpty(r.Spec.Reasons), Display: r.Display,
		Decision: r.Decision, Reviewer: r.Reviewer, Reason: r.Reason, Text: r.Text, DecidedAt: timePtr(r.DecidedAt),
		Item: toItem(r.Item),
	}
	if r.State != store.ReviewOpen && len(r.Patch) > 0 {
		out.Patch = r.Patch
	}
	return out
}

func toSample(s store.ReviewSample) Sample {
	return Sample{
		Pipeline: s.Pipeline, DeliveryID: s.DeliveryID, RunID: s.RunID, Step: s.Step, Inbox: s.Inbox,
		State: string(s.State), Display: s.Display, CreatedAt: s.CreatedAt, Deadline: timePtr(s.Deadline),
		Decision: s.Decision, Reviewer: s.Reviewer, Reason: s.Reason, DecidedAt: timePtr(s.DecidedAt),
		Item: toItem(s.Item),
	}
}

// graph describes a revision's pipeline; paused, when not nil, is its pause state (the active revision's).
func graph(p *pipeline.Pipeline, ps *store.PipelineState) Graph {
	g := Graph{Description: p.Description, Steps: []Step{}, Triggers: []Trigger{}}
	streams := map[string]bool{}
	for _, name := range slices.Sorted(maps.Keys(p.Triggers)) {
		t := p.Triggers[name]
		tr := Trigger{Name: name, Stream: pipeline.TriggerStreamPrefix + name}
		switch {
		case t.Cron != nil:
			tr.Kind, tr.Schedule = "cron", t.Cron.Expr
		case t.Webhook != nil:
			tr.Kind = "webhook"
		default:
			tr.Kind = "manual"
		}
		streams[tr.Stream] = true
		g.Triggers = append(g.Triggers, tr)
	}
	for _, name := range slices.Sorted(maps.Keys(p.Steps)) {
		st := p.Steps[name]
		step := Step{Name: name, Action: st.Action, Builtin: pipeline.IsBuiltin(st.Action), In: orEmpty(st.In),
			Out: orEmpty(st.Out.Targets()), DLQ: st.DLQ, Concurrency: max(st.Concurrency, 1)}
		if ps != nil {
			step.Paused = ps.Paused || slices.Contains(ps.PausedSteps, name)
			step.PauseCause = ps.PauseCauses[name]
		}
		for _, s := range step.In {
			streams[s] = true
		}
		for _, s := range step.Out {
			streams[s] = true
		}
		if st.DLQ != "" {
			streams[st.DLQ] = true
		}
		g.Steps = append(g.Steps, step)
	}
	g.Streams = slices.Sorted(maps.Keys(streams))
	return g
}
