package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// RevisionID identifies an applied revision.
type RevisionID int64

// DeliveryState is a delivery's state (spec section 5.3). A retry waiting for its backoff is pending with a
// NextAttemptAt in the future.
type DeliveryState string

// Delivery states. Filtered belongs to the filter and dedupe builtins; AwaitingReview to review (M3).
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

// Reasons a builtin filters a delivery, recorded on it (Delivery.FilterReason) and in its deliveryFiltered event.
const (
	// FilterReasonExpr: the filter builtin's expr was false for the item.
	FilterReasonExpr = "exprFalse"
	// FilterReasonDuplicate: the dedupe builtin already let the item's key through (spec section 5.11).
	FilterReasonDuplicate = "duplicate"
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
	EventFiringSkipped          = "firingSkipped"
	EventBudgetReached          = "budgetReached"
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
	// PauseCauses says why a paused step was paused when a user did not ask for it (budget.run, budget.day), by
	// step; nil when no step is.
	PauseCauses map[string]string
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
	// Reserve names the agent steps, with the reservation each delivery of them takes when leased: the action's
	// maxCostUsd, zero when it sets none (spec section 5.9). A delivery of a step in Reserve is leased only while its
	// run's spend and reservations plus its own stay within budget.runUsd, and the pipeline's for the day within
	// budget.dayUsd; the first delivery a budget stops pauses every agent step of its pipeline named here.
	Reserve map[StepRef]float64
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

// DoneRequest completes a leased delivery with its outputs. Artifacts are the attempt's own (spec section 6.3),
// already in the artifact store.
type DoneRequest struct {
	LeaseRef
	Attempt   int
	Outputs   []Output
	CostUSD   float64
	Artifacts []ArtifactRef
}

// FailRequest fails a leased delivery. Artifacts are the failed attempt's own, kept as on done.
type FailRequest struct {
	LeaseRef
	Attempt   int
	Class     string
	Message   string
	Retryable bool
	CostUSD   float64
	// Unreported means the attempt ended without reporting its cost (the runtime killed or lost the action): its
	// budget reservation is charged in full, as for an expired lease, when it exceeds CostUSD.
	Unreported bool
	Artifacts  []ArtifactRef
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

// ReplayStart starts a replay run (spec section 5.7).
type ReplayStart struct {
	Pipeline string
	Step     string
	// RunID is the replay run's id and Key the request id it derives from (ids.ReplayRun).
	RunID string
	Key   string
	// Revision pins the run; zero means the pipeline's active revision.
	Revision RevisionID
	// Vars are the run's resolved variables and VarSources where each came from.
	Vars       map[string]any
	VarSources map[string]string
	// Items are the selected input items, each on one of the step's input streams.
	Items []string
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
	Artifacts  []ArtifactRef
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
	// Since keeps the items published at or after it.
	Since time.Time
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

// EvalResult is the score of one eval case of an action (spec section 7.4), the eval_score{action,case} metric.
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
