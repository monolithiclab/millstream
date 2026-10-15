package store

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// RevisionID identifies an applied revision.
type RevisionID int64

// DeliveryState is a delivery's state (spec section 5.3). A retry waiting for its backoff is pending with a
// NextAttemptAt in the future.
type DeliveryState string

// Delivery states. Filtered and AwaitingReview belong to builtins and review (later milestones).
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

// Run states. WaitingReview is reserved for M3.
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
)

// TriggerSource says how a trigger fired.
type TriggerSource string

// Trigger sources. A cron trigger fired by hand is a manual firing: it does not move where catch-up resumes.
const (
	SourceCron   TriggerSource = "cron"
	SourceManual TriggerSource = "manual"
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
	EventFiringSkipped          = "firingSkipped"
)

// PackageRecord is a package to apply: its content hash, an opaque bundle of its files, and its pipeline as
// pipeline.Parse returns it, with step defaults (dlq, retry) filled in. The store keeps the pipeline as JSON with
// the revision.
type PackageRecord struct {
	Hash     string
	Bundle   []byte
	Pipeline *pipeline.Pipeline
}

// Revision is an applied revision.
type Revision struct {
	ID          RevisionID
	Pipeline    string
	PackageHash string
	AppliedAt   time.Time
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

// PipelineState is a pipeline's active revision and pause state.
type PipelineState struct {
	Name           string
	ActiveRevision RevisionID
	Paused         bool
	PausedSteps    []string
}

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

// Output is one output of a delivery.
type Output struct {
	Key     string
	Payload json.RawMessage
}

// DoneRequest completes a leased delivery with its outputs.
type DoneRequest struct {
	LeaseRef
	Attempt int
	Outputs []Output
	CostUSD float64
}

// FailRequest fails a leased delivery.
type FailRequest struct {
	LeaseRef
	Attempt   int
	Class     string
	Message   string
	Retryable bool
	CostUSD   float64
}

// Run is a run record.
type Run struct {
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
}

// Item is an item envelope with its payload (spec section 5.2).
type Item struct {
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
	CostUSD        float64
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

// RunFilter selects runs; empty fields match everything. Runs come newest first.
type RunFilter struct {
	Pipeline string
	State    RunState
	Limit    int
}

// ItemFilter selects items; empty fields match everything. Items come in insertion order.
type ItemFilter struct {
	Pipeline string
	Stream   string
	RunID    string
}

// DeliveryFilter selects deliveries; empty fields match everything. Deliveries come in creation order.
type DeliveryFilter struct {
	Pipeline string
	RunID    string
	State    DeliveryState
}

// DeadLetterFilter selects dead letters; empty fields match everything.
type DeadLetterFilter struct {
	Pipeline string
	RunID    string
	State    DeadLetterState
}
