package store

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// RevisionID identifies an applied revision.
type RevisionID int64

// DeliveryState is a delivery's state (spec section 5.3). A retry waiting for its backoff is pending with a
// NextAttemptAt in the future.
type DeliveryState string

// Delivery states. Filtered belongs to the builtins (filter, dedupe, a review's rejection); AwaitingReview to review.
const (
	DeliveryPending        DeliveryState = "pending"
	DeliveryLeased         DeliveryState = "leased"
	DeliveryDone           DeliveryState = "done"
	DeliveryFiltered       DeliveryState = "filtered"
	DeliveryAwaitingReview DeliveryState = "awaitingReview"
	DeliveryDead           DeliveryState = "dead"
	DeliveryCancelled      DeliveryState = "cancelled"
)

// Terminal reports whether a delivery in state s is finished for its run's settlement (spec section 5.4).
func (s DeliveryState) Terminal() bool {
	switch s {
	case DeliveryDone, DeliveryFiltered, DeliveryDead, DeliveryCancelled:
		return true
	}
	return false
}

// RunState is a run's state (spec section 5.4).
type RunState string

// Run states. A run is waitingReview while only parked reviews remain (spec section 5.4).
const (
	RunRunning             RunState = "running"
	RunWaitingReview       RunState = "waitingReview"
	RunSucceeded           RunState = "succeeded"
	RunCompletedWithErrors RunState = "completedWithErrors"
	RunCancelled           RunState = "cancelled"
)

// RunKind says what opened a run.
type RunKind string

// Run kinds (spec section 9.1).
const (
	RunKindTrigger RunKind = "trigger"
	RunKindReplay  RunKind = "replay"
	// RunKindImport is a run an imported stream opened for a producer run (spec section 4.12).
	RunKindImport RunKind = "import"
)

// TriggerSource says how a trigger fired.
type TriggerSource string

// Trigger sources. A cron trigger fired by hand is a manual firing: it does not move where catch-up resumes.
const (
	SourceCron   TriggerSource = "cron"
	SourceManual TriggerSource = "manual"
	// SourceImport is an import run's: its trigger is the imported stream, its key the producer run.
	SourceImport TriggerSource = "import"
	// SourceWebhook is a webhook trigger's request (spec section 4.6), keyed by its request id.
	SourceWebhook TriggerSource = "webhook"
	// SourceInsight is an insight trigger's (spec section 4.6), keyed by the insight and the event that opened,
	// reopened or escalated it.
	SourceInsight TriggerSource = "insight"
)

// DeadLetterKind is why a dead letter exists (spec section 5.6).
type DeadLetterKind string

// Dead-letter kinds.
const (
	DeadLetterDead     DeadLetterKind = "dead"
	DeadLetterUnrouted DeadLetterKind = "unrouted"
)

// DeadLetterState is a dead letter's state.
type DeadLetterState string

// Dead-letter states.
const (
	DeadLetterOpen      DeadLetterState = "open"
	DeadLetterReplayed  DeadLetterState = "replayed"
	DeadLetterDiscarded DeadLetterState = "discarded"
)

// Error classes the store itself assigns. The engine passes its own (retryable, nonRetryable, protocol, timeout,
// crash…) to Fail.
const (
	// ClassKeyRule: several outputs, not all keyed or with a key repeated (spec section 5.2). Never retried.
	ClassKeyRule = "keyRule"
	// ClassRoute: a route's when failed to evaluate. Deterministic, so never retried (spec section 4.3).
	ClassRoute = "route"
	// ClassMissingStep: a delivery reported done for a step its run's revision does not have, so its outputs have
	// nowhere to go. Never retried.
	ClassMissingStep = "missingStep"
	// ClassLeaseExpired: the lease expired before its owner reported (spec section 5.3). It consumes the attempt.
	ClassLeaseExpired = "leaseExpired"
	// ClassAbstain: the action abstained (spec section 6.3). A Fail of this class is the Abstain unit's: the
	// delivery goes to the step's fallback action, or, on a step without one, dead-letters as a non-retryable error.
	ClassAbstain = "abstain"
)

// What produced an item of a step with a fallback (spec section 11.5), recorded in its envelope: the step's own
// action, or its fallback after the primary abstained or failed.
const (
	ProducedByPrimary  = "primary"
	ProducedByFallback = "fallback"
)

// Why a delivery went to its step's fallback action (spec section 11.5), as fallback.when spells it.
const (
	FallbackOnAbstain = "abstain"
	FallbackOnError   = "error"
)

// Reasons a builtin filters a delivery, recorded on it (Delivery.FilterReason) and in its deliveryFiltered event.
const (
	// FilterReasonExpr: the filter builtin's expr was false for the item.
	FilterReasonExpr = "exprFalse"
	// FilterReasonDuplicate: the dedupe builtin already let the item's key through (spec section 5.11).
	FilterReasonDuplicate = "duplicate"
	// FilterReasonRejected: a reviewer rejected the item without a reason code; with one, the code is the reason.
	FilterReasonRejected = "rejected"
	// FilterReasonTimeout: the review timed out and onTimeout rejected it.
	FilterReasonTimeout = "timeout"
)

// MaxErrorMessage is the most bytes of an error message kept on a delivery (spec section 5.5).
const MaxErrorMessage = 4096

// Event kinds the store records.
const (
	EventRevisionApplied        = "revisionApplied"
	EventRunStarted             = "runStarted"
	EventItemPublished          = "itemPublished"
	EventNondeterministicOutput = "nondeterministicOutput"
	EventDeliveryLeased         = "deliveryLeased"
	EventDeliveryDone           = "deliveryDone"
	EventDeliveryRetry          = "deliveryRetry"
	EventDeliveryDead           = "deliveryDead"
	EventDeliveryCancelled      = "deliveryCancelled"
	EventDeliveryFiltered       = "deliveryFiltered"
	EventLeaseExpired           = "leaseExpired"
	EventDeadLetter             = "deadLetter"
	EventDeadLetterReplayed     = "deadLetterReplayed"
	EventDeadLetterDiscarded    = "deadLetterDiscarded"
	EventRevisionMigrated       = "revisionMigrated"
	EventRunCancelled           = "runCancelled"
	EventRunReopened            = "runReopened"
	EventRunSettled             = "runSettled"
	EventPaused                 = "paused"
	EventResumed                = "resumed"
	EventDisabled               = "disabled"
	EventRolledBack             = "rolledBack"
	EventTriggerTokenSet        = "triggerTokenSet"
	EventFiringSkipped          = "firingSkipped"
	EventBudgetReached          = "budgetReached"
	EventInsightOpened          = "insightOpened"
	EventInsightResolved        = "insightResolved"
	EventInsightEscalated       = "insightEscalated"
	EventReviewParked           = "reviewParked"
	EventReviewDecided          = "reviewDecided"
	EventReviewEscalated        = "reviewEscalated"
	EventReviewSampled          = "reviewSampled"
	EventSampleDecided          = "sampleDecided"
	EventSampleExpired          = "sampleExpired"
	EventItemsExpired           = "itemsExpired"
	EventPurgeStarted           = "purgeStarted"
	EventPurgeCompleted         = "purgeCompleted"
	// EventDeliveryFallback: an attempt abstained or failed non-retryably, and the delivery went to its step's
	// fallback action with its retry budget restarted (spec section 11.5).
	EventDeliveryFallback = "deliveryFallback"
	// EventAuditEnded: a fallback audit ended, with its agreement or its error, and its cost (spec section 11.5).
	EventAuditEnded = "auditEnded"
)

// Budget scopes (spec section 7.5): budget.runUsd bounds the spend of one run, budget.dayUsd the spend of the
// pipeline over a UTC day of the store clock.
const (
	BudgetRun = "run"
	BudgetDay = "day"
)

// PauseCauseBudget prefixes the cause of a step paused by a budget: budget.run or budget.day. A pause a user asked
// for has no cause.
const PauseCauseBudget = "budget."

// PackageRecord is a package to apply: its content hash, an opaque bundle of its files, and its pipeline as
// pipeline.Parse returns it, with step defaults (dlq, retry) filled in. The store keeps the pipeline as JSON with
// the revision.
type PackageRecord struct {
	Hash     string
	Bundle   []byte
	Pipeline *pipeline.Pipeline
	// Message says why the revision was applied (apply --message); it is kept with the revision.
	Message string
	// Vars and VarSources are the variables of the runs the pipeline's imported streams open, resolved by whoever
	// applies the package as a firing would resolve them (spec section 4.12): such a run opens inside a producer's
	// Done, where no environment is read. They are kept with the revision, so a rollback to it brings them back;
	// applying the active package again replaces them.
	Vars       map[string]any
	VarSources map[string]string
}

// Revision is an applied revision.
type Revision struct {
	ID          RevisionID
	Pipeline    string
	PackageHash string
	AppliedAt   time.Time
	Message     string
	// PipelineJSON is the pipeline as stored; Decode parses it.
	PipelineJSON []byte
}

// Decode parses the revision's pipeline.
func (r Revision) Decode() (*pipeline.Pipeline, error) {
	p, err := pipeline.UnmarshalPipeline(r.PipelineJSON)
	if err != nil {
		return nil, fmt.Errorf("revision %d: %w", r.ID, err)
	}
	return p, nil
}

// PipelineState is a pipeline's active revision and state: active, paused (its triggers fire, its deliveries wait)
// or disabled (neither: spec section 13).
type PipelineState struct {
	Name           string
	ActiveRevision RevisionID
	Paused         bool
	Disabled       bool
	PausedSteps    []string
	// PauseCauses says why a paused step was paused when a user did not ask for it (budget.run, budget.day), by
	// step; nil when no step is.
	PauseCauses map[string]string
}

// Holds reports whether the pipeline's deliveries wait: it is paused or disabled.
func (ps PipelineState) Holds() bool { return ps.Paused || ps.Disabled }

// Firing is one trigger firing (spec section 4.6).
type Firing struct {
	Pipeline string
	Trigger  string
	// Key is the scheduled time of a cron firing (ids.CronKey) or the request id of a manual one.
	Key   string
	RunID string
	// Source defaults to SourceManual. A cron firing carries its ScheduledAt.
	Source      TriggerSource
	ScheduledAt time.Time
	// Kind defaults to RunKindTrigger.
	Kind RunKind
	// Revision pins the run; zero means the pipeline's active revision.
	Revision   RevisionID
	Generation int64
	// Vars are the run's resolved variables and VarSources where each came from (flag, env, default).
	Vars       map[string]any
	VarSources map[string]string
	// Payload is the trigger item's payload; nil means {}.
	Payload json.RawMessage
}

// StepRef names a step of a pipeline.
type StepRef struct {
	Pipeline string
	Step     string
}

// LeaseRequest asks for deliveries to lease.
type LeaseRequest struct {
	Owner string
	Max   int
	TTL   time.Duration
	// Exclude lists steps the engine holds back (rate limits); a StepRef with an empty Step excludes the whole
	// pipeline. Paused pipelines and steps are skipped from the store's own pause state.
	Exclude []StepRef
	// Slots caps the deliveries leased per step in this call (free concurrency). A nil map means no cap; with a
	// non-nil map, a step that is absent is not capped.
	Slots map[StepRef]int
	// PipelineSlots caps the deliveries leased per pipeline in this call, across its steps (free limits.concurrency),
	// with the same nil and absent rules as Slots.
	PipelineSlots map[string]int
	// Reserve names the agent steps of each revision, with the reservation each delivery of them takes when leased:
	// the maxCostUsd of the action in its run's revision, zero when it sets none (spec section 5.9). A delivery of a
	// step in Reserve is leased only while its run's spend and reservations plus its own stay within budget.runUsd,
	// and the pipeline's for the day within budget.dayUsd; the first delivery a budget stops pauses every agent step
	// of its pipeline named here.
	Reserve map[AgentStep]float64
}

// AgentStep is an agent step of one revision of a pipeline: what a budget reservation is keyed by, since another
// revision may set another maxCostUsd. Fallback names the step's fallback action (spec section 11.5), which runs the
// deliveries handed to it and the step's audits.
type AgentStep struct {
	StepRef
	Revision RevisionID
	Fallback bool
}

// Lease is a leased delivery with what a node needs to run it.
type Lease struct {
	DeliveryID   string
	Pipeline     string
	RunID        string
	Revision     RevisionID
	Step         string
	Generation   int64
	Attempt      int
	FencingToken int64
	ExpiresAt    time.Time
	Item         Item
	// Fallback says the step's fallback action runs the delivery (spec section 11.5): its primary abstained or
	// failed, or the lease is an audit.
	Fallback bool
	// Audit says the lease is a fallback audit, not a delivery: DeliveryID is the audit's id, RunID the run of the
	// delivery it audits, and its end goes to EndAudit (spec section 11.5).
	Audit bool
}

// Ref returns the reference that renews or completes the lease.
func (l Lease) Ref() LeaseRef {
	return LeaseRef{Pipeline: l.Pipeline, DeliveryID: l.DeliveryID, Step: l.Step, FencingToken: l.FencingToken}
}

// LeaseRef identifies one lease: a delivery and the fencing token it was leased with. Step is informative: the
// units find the delivery by pipeline and id.
type LeaseRef struct {
	Pipeline     string
	DeliveryID   string
	Step         string
	FencingToken int64
}

// DeliveryKey names a delivery: its id is unique within its pipeline only (two pipelines with the same step, fired
// under the same key, have deliveries with the same id).
type DeliveryKey struct{ Pipeline, ID string }

// Key returns the key of the leased delivery.
func (r LeaseRef) Key() DeliveryKey { return DeliveryKey{r.Pipeline, r.DeliveryID} }

// Output is one output of a delivery: its payload and the artifacts already moved to the artifact store.
type Output struct {
	Key       string
	Payload   json.RawMessage
	Artifacts []ArtifactRef
}

// ArtifactRef is an artifact an item carries (spec sections 5.2 and 9.2): a file in the pipeline's
// content-addressed artifact store, under the name the action gave it.
type ArtifactRef struct {
	Name string `json:"name"`
	// SHA256 is the hex SHA-256 of the content: its address in the store.
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	// Type is the content's media type, sniffed from its first bytes.
	Type string `json:"type"`
}

// ValidArtifact checks an artifact reference: a name that is a plain file name (it becomes in/<name> in the
// working directory of the actions reading the item), a SHA-256 in lowercase hex, a size and a type. Its error wraps
// ErrInvalid.
func ValidArtifact(a ArtifactRef) error {
	switch {
	case !ValidArtifactName(a.Name):
		return fmt.Errorf("artifact name %q: want a file name without a path separator: %w", a.Name, ErrInvalid)
	case !ValidSHA256(a.SHA256):
		return fmt.Errorf("artifact %s: %q is not a hex SHA-256: %w", a.Name, a.SHA256, ErrInvalid)
	case a.Size < 0 || a.Type == "":
		return fmt.Errorf("artifact %s: no size or type: %w", a.Name, ErrInvalid)
	}
	return nil
}

// ValidArtifactName reports whether name can name an artifact: a non-empty file name of at most 255 bytes, not
// "." or "..", without a slash, a backslash or a NUL byte.
func ValidArtifactName(name string) bool {
	return name != "" && len(name) <= 255 && name != "." && name != ".." && !strings.ContainsAny(name, "/\\\x00")
}

// ValidSHA256 reports whether s is a SHA-256 in lowercase hex.
func ValidSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Artifact is an artifact store entry: its content and how many items reference it.
type Artifact struct {
	Pipeline string
	SHA256   string
	Size     int64
	Type     string
	Refcount int64
}

// Metric is a measurement an action reported for an attempt with a metric message (spec section 6.3). The Done and
// Fail units record an attempt's metrics in its event, where the rollup projector reads them (spec section 10.2).
type Metric struct {
	Name   string            `json:"name"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels,omitempty"`
}

// MaxAttemptMetrics is the most metrics an attempt records; a node drops the others.
const MaxAttemptMetrics = 256

// ValidMetrics checks an attempt's metrics: at most MaxAttemptMetrics, each with a name and a finite value. Its
// error wraps ErrInvalid.
func ValidMetrics(ms []Metric) error {
	if len(ms) > MaxAttemptMetrics {
		return fmt.Errorf("%d metrics, over %d: %w", len(ms), MaxAttemptMetrics, ErrInvalid)
	}
	for _, m := range ms {
		if m.Name == "" || math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
			return fmt.Errorf("metric %q: want a name and a finite value: %w", m.Name, ErrInvalid)
		}
	}
	return nil
}

// DoneRequest completes a leased delivery with its outputs. Artifacts are the attempt's own (spec section 6.3),
// already in the artifact store, and Metrics the ones the action reported.
type DoneRequest struct {
	LeaseRef
	// Owner, when set, must be the lease's owner: a remote node's report (spec section 15.7).
	Owner     string
	Attempt   int
	Outputs   []Output
	CostUSD   float64
	Artifacts []ArtifactRef
	Metrics   []Metric
}

// FailRequest fails a leased delivery. Artifacts are the failed attempt's own, kept as on done.
type FailRequest struct {
	LeaseRef
	// Owner, when set, must be the lease's owner, as DoneRequest's.
	Owner     string
	Attempt   int
	Class     string
	Message   string
	Retryable bool
	CostUSD   float64
	// Unreported means the attempt ended without reporting its cost (the runtime killed or lost the action): its
	// budget reservation is charged in full, as for an expired lease, when it exceeds CostUSD.
	Unreported bool
	Artifacts  []ArtifactRef
	Metrics    []Metric
}

// AttemptArtifact is an artifact an attempt of a delivery kept for itself, such as an agent's transcript.
type AttemptArtifact struct {
	Attempt int
	ArtifactRef
}

// FilterRequest filters a leased delivery: a builtin decided not to pass it on.
type FilterRequest struct {
	LeaseRef
	Attempt int
	Reason  string
}

// DedupeRequest applies the Dedupe unit to a leased delivery of a dedupe step: Key is the item's key as the step's
// config.key computed it, and Window how long a key let through holds back the next item with the same key (zero:
// for ever).
type DedupeRequest struct {
	LeaseRef
	Attempt int
	Key     string
	Window  time.Duration
}

// ReplayStart starts a replay run (spec section 5.7): of a step over items of its pipeline, or, with Stream, of the
// steps reading an imported stream over the producer's items on it (a back-fill, spec section 4.12).
type ReplayStart struct {
	Pipeline string
	Step     string
	// Stream, instead of Step, names an imported stream of the pipeline: Items are the producer's items on it, each
	// copied to the stream (or found there) and delivered to every step reading it.
	Stream string
	// RunID is the replay run's id and Key the request id it derives from (ids.ReplayRun).
	RunID string
	Key   string
	// Revision pins the run; zero means the pipeline's active revision.
	Revision RevisionID
	// Vars are the run's resolved variables and VarSources where each came from.
	Vars       map[string]any
	VarSources map[string]string
	// Items are the selected input items, each on one of the step's input streams (with Stream, the producer's items
	// on its stream).
	Items []string
}

// Run is a run record.
type Run struct {
	// Seq orders runs started at the same instant: with StartedAt, a paging cursor (RunFilter.After).
	Seq           int64
	ID            string
	Pipeline      string
	Revision      RevisionID
	Trigger       string
	TriggerKey    string
	TriggerSource TriggerSource
	// ScheduledAt is a cron firing's scheduled time, zero for other runs.
	ScheduledAt time.Time
	Kind        RunKind
	Generation  int64
	Vars        map[string]any
	VarSources  map[string]string
	State       RunState
	StartedAt   time.Time
	// SettledAt is zero while the run is not settled.
	SettledAt time.Time
	CostUSD   float64
	// Producer is the producer pipeline of an import run, empty for other runs; its TriggerKey is the producer run.
	Producer string
}

// Item is an item envelope with its payload (spec section 5.2).
type Item struct {
	// Seq orders items by insertion: a paging cursor (ItemFilter.After).
	Seq        int64
	ID         string
	Pipeline   string
	RunID      string
	Stream     string
	Step       string
	Key        string
	Parents    []string
	Generation int64
	TS         time.Time
	Payload    json.RawMessage
	Artifacts  []ArtifactRef
	// ParentPipeline is the pipeline holding the item's parents when it is another one: a copy published on an
	// imported stream (spec section 4.12). Empty for every other item.
	ParentPipeline string
	// ProducedBy is ProducedByPrimary or ProducedByFallback for an output of a step with a fallback (spec section
	// 11.5), empty for every other item.
	ProducedBy string
	// Tombstone says why the item no longer holds its payload: empty while it does. A tombstoned item keeps its
	// envelope and lineage; its payload reads as null and it carries no artifacts (spec section 9.3).
	Tombstone Tombstone
}

// NodeState is a remote node's state (spec section 5.10).
type NodeState string

// Node states.
const (
	// NodeActive: the node pulls deliveries.
	NodeActive NodeState = "active"
	// NodeDraining: the engine leases nothing more to the node, which finishes what it holds; a node that left is
	// draining too. Registering again from a new process makes it active.
	NodeDraining NodeState = "draining"
	// NodeRevoked: the id is refused from then on (spec section 15.7).
	NodeRevoked NodeState = "revoked"
)

// Node is a remote node as the store records it: what it registered with, its state and when the engine last
// heard from it (registration and heartbeats, by the store clock).
type Node struct {
	ID       string
	Version  string
	Labels   map[string]string
	Runtimes []string
	Slots    int
	// Nonce is drawn by each node process: a node registering again from the process that registered last (after a
	// failover) stays draining when it was; another process makes it active.
	Nonce string
	State NodeState
	// RegisteredAt is zero for an id revoked before it ever registered.
	RegisteredAt  time.Time
	LastHeartbeat time.Time
}

// Delivery is a delivery record (spec section 5.3).
type Delivery struct {
	ID             string
	Pipeline       string
	RunID          string
	Step           string
	ItemID         string
	Generation     int64
	State          DeliveryState
	Attempt        int
	LeaseOwner     string
	FencingToken   int64
	LeaseExpiresAt time.Time
	NextAttemptAt  time.Time
	ErrorClass     string
	ErrorMessage   string
	// FilterReason is why a builtin filtered the delivery, empty unless it is filtered.
	FilterReason string
	CostUSD      float64
	// ReservedUSD is the budget reservation of a leased agent delivery (spec section 5.9), zero otherwise.
	ReservedUSD float64
	// Fallback says the delivery went to its step's fallback action (spec section 11.5).
	Fallback bool
	// CreatedAt is when the delivery was created (or reset by a dead-letter replay), LeasedAt when its latest
	// attempt was leased (zero before the first), and FinishedAt when it became terminal (zero until then).
	CreatedAt  time.Time
	LeasedAt   time.Time
	FinishedAt time.Time
	// Wait is the time its attempts spent due but not leased (queue wait, backoff excluded), and Work the time they
	// spent leased: together, with the backoffs, its latency (spec section 11.1).
	Wait time.Duration
	Work time.Duration
}

// WaitAt is the delivery's queue wait as of now: Wait, plus the time a due pending delivery has waited since.
func (d Delivery) WaitAt(now time.Time) time.Duration {
	if d.State == DeliveryPending && !d.NextAttemptAt.After(now) {
		return d.Wait + now.Sub(d.NextAttemptAt)
	}
	return d.Wait
}

// WorkAt is the delivery's work time as of now: Work, plus the time a leased delivery has held its lease since.
func (d Delivery) WorkAt(now time.Time) time.Duration {
	if d.State == DeliveryLeased && !d.LeasedAt.IsZero() {
		return d.Work + max(now.Sub(d.LeasedAt), 0)
	}
	return d.Work
}

// Latency is the delivery's latency: from creation to terminal, or to now while it is not terminal.
func (d Delivery) Latency(now time.Time) time.Duration {
	end := d.FinishedAt
	if end.IsZero() {
		end = now
	}
	return max(end.Sub(d.CreatedAt), 0)
}

// DeadLetter is a dead-letter record; its ID is the dead-letter item's id on Stream.
type DeadLetter struct {
	ID             string
	Pipeline       string
	RunID          string
	OriginDelivery string
	OriginStep     string
	Kind           DeadLetterKind
	State          DeadLetterState
	Stream         string
	Error          string
	CreatedAt      time.Time
}

// Event is one entry of the append-only event log; Seq is assigned in commit order without gaps.
type Event struct {
	Seq      int64
	TS       time.Time
	Pipeline string
	RunID    string
	Kind     string
	Data     json.RawMessage
}

// RunFilter selects runs; empty fields match everything. Runs come newest first (StartedAt, then Seq).
type RunFilter struct {
	Pipeline string
	State    RunState
	// Unsettled keeps the runs not settled yet, whatever their state (running, waitingReview, cancelled while live).
	Unsettled bool
	// Producer and ProducerRun keep the import runs opened for that producer run, in any pipeline (ProducerRun only
	// with Producer).
	Producer    string
	ProducerRun string
	// After keeps the runs that come after this run in that order: the next page. A zero Seq starts at the newest.
	After RunPosition
	Limit int
}

// RunPosition is where a run sits in the order runs come in.
type RunPosition struct {
	StartedAt time.Time
	Seq       int64
}

// ItemFilter selects items; empty fields match everything. Items come in insertion order, those after the cursor
// After (an Item.Seq), at most Limit (all when zero).
type ItemFilter struct {
	Pipeline string
	Stream   string
	RunID    string
	// IDs keeps the items with these ids.
	IDs []string
	// Parents keeps the items that have one of these items among their parents. The parents are in the item's own
	// pipeline, unless ParentPipeline names theirs: then their children are found in every pipeline, in
	// ParentPipeline and as the copies published through imported streams in the others (spec section 4.12).
	Parents        []string
	ParentPipeline string
	// Since keeps the items published at or after it.
	Since time.Time
	After int64
	Limit int
}

// DeliveryFilter selects deliveries; empty fields match everything. Deliveries come in creation order.
type DeliveryFilter struct {
	Pipeline string
	ID       string
	RunID    string
	Step     string
	// ItemID keeps the deliveries of this input item.
	ItemID string
	State  DeliveryState
	// Since keeps the deliveries created at or after it.
	Since time.Time
}

// EventFilter selects a pipeline's events; empty fields match everything. Events come in sequence order.
type EventFilter struct {
	Pipeline string
	RunID    string
	Kinds    []string
	// Since keeps the events recorded at or after it.
	Since time.Time
}

// StepStats is a step's deliveries by state now, and those that finished within a window (StepStats in Store).
type StepStats struct {
	Step           string
	Pending        int64
	Leased         int64
	AwaitingReview int64
	// Finished counts the deliveries that became terminal within the window, Dead those of them that ended dead,
	// and Latencies holds their latencies, in no particular order.
	Finished  int64
	Dead      int64
	Latencies []time.Duration
}

// MetricRollup is one metric's one-minute rollup for one label set (spec section 10.2).
type MetricRollup struct {
	Pipeline string
	Name     string
	// Labels are the label set, by name; nil when it has none.
	Labels map[string]string
	// Minute is the minute the rollup covers, truncated.
	Minute time.Time
	Count  int64
	Sum    float64
	Min    float64
	Max    float64
}

// MetricTotal is one series of the rollups, a pipeline's metric and label set, over every minute ever rolled up,
// those retention deleted included: what the Prometheus exposition shows.
type MetricTotal struct {
	Pipeline string
	Name     string
	// Labels are the label set, by name; nil when it has none.
	Labels map[string]string
	// Count and Sum add up every minute, so they never decrease.
	Count int64
	Sum   float64
	// Last is the latest minute, and LastCount and LastSum its count and sum: a gauge shows their mean.
	Last      time.Time
	LastCount int64
	LastSum   float64
}

// BatchSizes are the sizes of the batches a store's writer committed since it opened (spec section 10.2, writer batch
// size), as a histogram: Counts[i] is the number of batches of at most Bounds[i] units, cumulative, and the last
// count, one past the bounds, the number of batches (+Inf); Sum is their units.
type BatchSizes struct {
	Bounds []int
	Counts []int64
	Sum    int64
}

// MetricFilter selects rollups; empty fields match everything. Rollups come by pipeline, name, labels and minute.
type MetricFilter struct {
	Pipeline string
	Name     string
	// Since keeps the minutes at or after it.
	Since time.Time
}

// InsightState is an insight's state (spec section 11.1).
type InsightState string

// Insight states. The projector opens and resolves insights; a user acknowledges or mutes them, and a muted insight
// stays muted whatever the projector finds.
const (
	InsightOpen         InsightState = "open"
	InsightAcknowledged InsightState = "acknowledged"
	InsightResolved     InsightState = "resolved"
	InsightMuted        InsightState = "muted"
)

// Insight is a finding of the insights projector (spec section 11.1). ID is its fingerprint: a recurring problem
// updates its row.
type Insight struct {
	Pipeline string
	ID       string
	Kind     string
	// Scope is the step it concerns, or "pipeline".
	Scope       string
	WindowStart time.Time
	WindowEnd   time.Time
	// Severity is info, warning or error.
	Severity string
	// Evidence is a JSON object: the figures and up to 20 sample item ids.
	Evidence json.RawMessage
	State    InsightState
	// OpenedAt is when the projector first found it, UpdatedAt when it last changed.
	OpenedAt  time.Time
	UpdatedAt time.Time
}

// InsightReport is what one pass of the insights projector found for a pipeline: Insights for the Kinds it
// evaluated. An insight of those kinds not found again is resolved; kinds not evaluated are left as they are.
type InsightReport struct {
	Pipeline string
	Kinds    []string
	Insights []Insight
}

// InsightFilter selects insights; empty fields match everything. Insights come by pipeline, then most recently
// updated first.
type InsightFilter struct {
	Pipeline string
	State    InsightState
}

// DeadLetterFilter selects dead letters; empty fields match everything.
type DeadLetterFilter struct {
	Pipeline string
	RunID    string
	State    DeadLetterState
}

// AuditState is a fallback audit's state (spec section 11.5).
type AuditState string

// Audit states.
const (
	// AuditPending: sampled when the delivery it audits was done, waiting for a lease.
	AuditPending AuditState = "pending"
	// AuditLeased: the fallback action runs it.
	AuditLeased AuditState = "leased"
	// AuditDone: the fallback answered; its outputs and agreement are recorded.
	AuditDone AuditState = "done"
	// AuditFailed: the fallback failed, abstained, lost its lease, or could not be dispatched. Never retried.
	AuditFailed AuditState = "failed"
	// AuditCancelled: its item was erased or expired, or a budget left no room for its reservation.
	AuditCancelled AuditState = "cancelled"
)

// Audit is a fallback audit (spec section 11.5): the step's fallback action run on an item its primary covered, a
// record beside the run, never part of it. Primary and Fallback are the outputs of each, a JSON array of
// {"key", "payload"} objects (Fallback is null until the audit is done); Agreement is 1 when they are the same
// outputs (by key, then canonical payload) and 0 otherwise, set once done.
type Audit struct {
	ID         string
	Pipeline   string
	DeliveryID string
	RunID      string
	Step       string
	ItemID     string
	State      AuditState
	// LeaseOwner and FencingToken are those of its latest lease.
	LeaseOwner   string
	FencingToken int64
	Primary      json.RawMessage
	Fallback     json.RawMessage
	Agreement    *float64
	CostUSD      float64
	// Error is the class and message of a failed or cancelled audit.
	Error      string
	CreatedAt  time.Time
	FinishedAt time.Time
}

// AuditFilter selects audits; empty fields match everything. Audits come in creation order.
type AuditFilter struct {
	Pipeline   string
	ID         string
	ItemID     string
	DeliveryID string
	RunID      string
	State      AuditState
}

// AuditEnd ends a leased audit: the fallback's outputs, or why it failed (Class set), with its cost. Owner, when set,
// must be the lease's owner, as DoneRequest's.
type AuditEnd struct {
	LeaseRef
	Owner      string
	Outputs    []Output
	Class      string
	Message    string
	CostUSD    float64
	Unreported bool
	Metrics    []Metric
}

// EvalResult is the score of one eval case of an action (spec section 7.4), which the M4 Prometheus endpoint
// exposes as the eval_score{action,case} metric.
type EvalResult struct {
	// Action is the action's name and ActionDigest the content hash of its directory when it was scored.
	Action       string
	ActionDigest string
	Case         string
	// Score is from 0 to 1.
	Score float64
	// At is when the store recorded it.
	At time.Time
}

// ReviewState is a review's state (spec section 8).
type ReviewState string

// Review states. An open review's delivery is awaitingReview; decided and timedOut ones ended it (approved or
// rejected, by a person, the autoApprove rule or onTimeout); a cancelled one's run was cancelled.
const (
	ReviewOpen      ReviewState = "open"
	ReviewDecided   ReviewState = "decided"
	ReviewTimedOut  ReviewState = "timedOut"
	ReviewCancelled ReviewState = "cancelled"
)

// Review decisions.
const (
	DecisionApprove = "approve"
	DecisionReject  = "reject"
)

// SampleState is a review sample's state (spec section 8): open until a person decides it, or unreviewed once its
// step's timeout passed without a decision.
type SampleState string

// Review sample states.
const (
	SampleOpen       SampleState = "open"
	SampleDecided    SampleState = "decided"
	SampleUnreviewed SampleState = "unreviewed"
)

// Review sample decisions: whether the person agrees with the rule that approved the item.
const (
	SampleAgree    = "agree"
	SampleDisagree = "disagree"
)

// ReviewSpec is what a parked review keeps of its step's config (spec section 8), so that deciding it or timing it
// out needs neither the revision nor its package.
type ReviewSpec struct {
	Inbox string `json:"inbox"`
	// Timeout is how long the item may wait, zero for ever; a review sample expires after it too.
	Timeout time.Duration `json:"timeoutNs"`
	// OnTimeout is reject, approve or escalate (to EscalateTo); an escalated review that times out again is
	// rejected.
	OnTimeout  string   `json:"onTimeout,omitempty"`
	EscalateTo string   `json:"escalateTo,omitempty"`
	Editable   []string `json:"editable"`
	Reasons    []string `json:"reasons"`
	// Notify is the step's webhook, told when the review is escalated as when it parked; NotifySecret, set
	// instead, names the pipeline secret holding it, resolved when a notification is sent and never stored.
	Notify       string `json:"notify,omitempty"`
	NotifySecret string `json:"notifySecret,omitempty"`
	// Schemas are the output schemas of what reaches the step: an edited payload must satisfy one of them. Empty
	// when no producer declares one.
	Schemas []json.RawMessage `json:"schemas"`
}

// ParkResult is what the Park unit did: the delivery's state, and the deadline a parked review got (zero without a
// timeout, or when the delivery did not park).
type ParkResult struct {
	State    DeliveryState
	Deadline time.Time
}

// ParkRequest applies the Park unit to a leased delivery of a review step: it parks in Spec.Inbox, or, when
// AutoApprove is set, passes at once and, with Sample, becomes a review sample. Display is the item rendered with the
// step's display template.
type ParkRequest struct {
	LeaseRef
	Attempt     int
	Spec        ReviewSpec
	Display     string
	AutoApprove bool
	Sample      bool
}

// ReviewDecisionRequest decides a parked review. Version is the review's version the reviewer saw: a review
// escalated since, or decided, is a conflict. An approval applies Patch (a JSON Patch, within the step's editable
// pointers) to the item's payload; a rejection needs a Reason among the step's reason codes when it has any.
type ReviewDecisionRequest struct {
	Pipeline   string
	DeliveryID string
	Version    int64
	Decision   string
	Patch      json.RawMessage
	Reviewer   string
	Reason     string
	Text       string
}

// SampleDecisionRequest decides a review sample: agree or disagree with the rule.
type SampleDecisionRequest struct {
	Pipeline   string
	DeliveryID string
	Decision   string
	Reviewer   string
	Reason     string
}

// Review is a review of a delivery of a review step (spec section 8), with its item.
type Review struct {
	// Seq orders reviews by when they were first parked: the inbox's order and paging cursor.
	Seq        int64
	Pipeline   string
	DeliveryID string
	RunID      string
	Step       string
	State      ReviewState
	// Version changes when the review is escalated or parked again: a decision names the version it saw.
	Version int64
	// Spec is the step's review config the review was parked with; its Inbox is where the review waits now,
	// escalated or not.
	Spec     ReviewSpec
	Display  string
	ParkedAt time.Time
	// Deadline is when onTimeout applies, zero without a timeout.
	Deadline time.Time
	// Decision is approve or reject once decided or timed out; Reviewer a person, rule or timeout; Patch the edit's
	// diff as a JSON Patch ([] without an edit).
	Decision  string
	Reviewer  string
	Reason    string
	Text      string
	Patch     json.RawMessage
	DecidedAt time.Time
	Item      Item
}

// ReviewSample is a review sample: an auto-approved item given to a person to measure the rule (spec section 8).
type ReviewSample struct {
	Seq        int64
	Pipeline   string
	DeliveryID string
	RunID      string
	Step       string
	Inbox      string
	State      SampleState
	Display    string
	CreatedAt  time.Time
	// Deadline is when an undecided sample becomes unreviewed, zero for never.
	Deadline  time.Time
	Decision  string
	Reviewer  string
	Reason    string
	DecidedAt time.Time
	Item      Item
}

// TimedOutReview is a review the Review timeout unit ended or escalated: Action is approve, reject or escalate, and
// an escalated review's Inbox and Deadline are its new ones. A sample that expired has Action unreviewed. A review
// that could not time out has Err set and nothing else but its keys: it stays open, tried again at the next tick.
type TimedOutReview struct {
	Err        error
	Pipeline   string
	DeliveryID string
	RunID      string
	Step       string
	Revision   RevisionID
	Inbox      string
	Action     string
	Deadline   time.Time
	ItemKey    string
	Display    string
	// Notify is the step's webhook, or NotifySecret the secret holding it; both empty for none.
	Notify       string
	NotifySecret string
}

// ReviewFilter selects reviews; empty fields match everything, but State defaults to open. Reviews come in Seq order,
// those after the cursor After, at most Limit (all when zero).
type ReviewFilter struct {
	Pipeline string
	Inbox    string
	Step     string
	State    ReviewState
	After    int64
	Limit    int
}

// SampleFilter selects review samples like ReviewFilter; State defaults to open.
type SampleFilter struct {
	Pipeline string
	Inbox    string
	Step     string
	State    SampleState
	After    int64
	Limit    int
}

// Tombstone is why an item no longer holds its payload (spec section 9.3). Its envelope (id, run, stream, step,
// parents, generation, time) stays, so counts and lineage remain correct; its payload is null and its artifacts are
// gone. An erased item's key is gone too.
type Tombstone string

// Tombstones.
const (
	// TombstoneExpired: its stream's retention passed (Retain).
	TombstoneExpired Tombstone = "expired"
	// TombstoneErased: a purge erased it (StartPurge fences it, ErasePurge erases it).
	TombstoneErased Tombstone = "erased"
)

// RetentionBatch is the most records of each kind one Retain pass removes.
const RetentionBatch = 500

// StreamRetention is a stream's retention (spec section 4.2, streams.<name>.retention): its items' payloads and
// artifacts are kept for Age after they were published.
type StreamRetention struct {
	Pipeline string
	Stream   string
	Age      time.Duration
}

// RetentionRequest is one pass of the retention unit (spec section 9.3): the streams with a retention, and how long
// events, transcripts (the artifacts attempts keep) and metric rollups are kept, zero for ever. Ages count back from
// the store clock.
type RetentionRequest struct {
	Streams     []StreamRetention
	Events      time.Duration
	Transcripts time.Duration
	Rollups     time.Duration
	// Limit bounds the records of each kind the pass removes; zero means RetentionBatch.
	Limit int
}

// RetentionResult is what a Retain pass removed. More says a kind reached its limit: another pass has more to do.
type RetentionResult struct {
	Items, Events, Transcripts, Rollups int
	More                                bool
}

// Add adds o's counts to r's.
func (r *RetentionResult) Add(o RetentionResult) {
	r.Items, r.Events, r.Transcripts, r.Rollups = r.Items+o.Items, r.Events+o.Events, r.Transcripts+o.Transcripts,
		r.Rollups+o.Rollups
}

// PurgeBatch is the most items one ErasePurge pass erases.
const PurgeBatch = 200

// PurgeScope is what a purge names (spec section 9.3).
type PurgeScope string

// Purge scopes.
const (
	PurgeKey  PurgeScope = "key"
	PurgeItem PurgeScope = "item"
)

// PurgeState is where a purge stands.
type PurgeState string

// Purge states: erasing until every fenced item is erased, sweeping while the engine deletes the artifact files no
// surviving item references, then done.
const (
	PurgeErasing  PurgeState = "erasing"
	PurgeSweeping PurgeState = "sweeping"
	PurgeDone     PurgeState = "done"
)

// PurgeRequest asks for a purge: of the items with Key, or of the item Item, in Pipeline or, when it is empty, in
// every pipeline; exactly one of Key and Item. ID names it: a request repeated with the same id finds the purge it
// started.
type PurgeRequest struct {
	ID       string
	Key      string
	Item     string
	Pipeline string
}

// Purge is a durable erasure operation (spec section 9.3, the purges table). It holds no erased data: the key or
// item named is not kept.
type Purge struct {
	ID          string
	Scope       PurgeScope
	Pipeline    string
	State       PurgeState
	Items       int64    // the items fenced: those named, their descendants and their copies in other pipelines
	Erased      int64    // how many of them are erased so far
	Pipelines   []string // the pipelines holding them, sorted
	RequestedAt time.Time
	FinishedAt  time.Time
}

// ArtifactKey names an artifact store entry.
type ArtifactKey struct {
	Pipeline string
	SHA256   string
}
