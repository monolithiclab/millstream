// Package store is Millstream's log store contract (spec sections 5 and 9.1): the records it holds and the atomic
// units of spec section 5.11 that change them. Every write goes through one writer loop that applies each unit
// whole (D23); reads use a separate pool. The pipeline id scopes every record (spec section 4.10), so runs, items,
// deliveries and dead letters are addressed by pipeline and id: two pipelines with a trigger of the same name fire
// runs with the same id without colliding. The SQLite implementation lives in store/sqlite.
package store

import (
	"context"
	"errors"
	"time"
)

// Errors returned, wrapped, by the units and reads.
var (
	// ErrNotFound means the named pipeline, revision, run, delivery or dead letter does not exist.
	ErrNotFound = errors.New("not found")
	// ErrStaleLease means a Done or Fail carried a fencing token or attempt that no longer holds the delivery's
	// lease, or the delivery is no longer leased: the message comes from an owner that lost it (spec section 5.10).
	ErrStaleLease = errors.New("stale lease")
	// ErrConflict means the unit does not apply to the record's current state, such as cancelling a settled run.
	ErrConflict = errors.New("conflict")
	// ErrInvalid means the request itself is malformed, such as an output payload that is not JSON.
	ErrInvalid = errors.New("invalid request")
	// ErrClosed means the store was closed.
	ErrClosed = errors.New("store closed")
)

// RollupBatch is the most events one Rollup pass reads.
const RollupBatch = 5000

// Store is the log store. Units block until the transaction that applied them commits. A unit that fails leaves no
// trace, whatever else committed in the same batch.
type Store interface {
	Close() error

	// ApplyRevision stores a package and makes its revision the pipeline's active one. Applying the package that
	// is already active is a no-op returning its revision.
	ApplyRevision(ctx context.Context, pkg PackageRecord) (RevisionID, error)
	// Fire is the Trigger unit: the run, its trigger item and the deliveries of the steps reading the trigger
	// stream. created is false when the run already existed: the firing already happened.
	Fire(ctx context.Context, f Firing) (created bool, err error)
	// Lease is the Lease unit: it leases up to req.Max pending deliveries that are due, oldest first per step,
	// round-robin across pipelines.
	Lease(ctx context.Context, req LeaseRequest) ([]Lease, error)
	// Renew extends the leases still held by owner with matching fencing tokens and returns the others.
	Renew(ctx context.Context, owner string, refs []LeaseRef, ttl time.Duration) (lost []LeaseRef, err error)
	// Done is the Done unit. It returns the state the delivery ended in: done, dead (key rule or a route that
	// fails to evaluate) or cancelled (its run was cancelled).
	Done(ctx context.Context, d DoneRequest) (DeliveryState, error)
	// Fail is the Fail unit. It returns the state the delivery ended in: pending (retry scheduled), dead or
	// cancelled.
	Fail(ctx context.Context, f FailRequest) (DeliveryState, error)
	// Filter is the Filter unit: a builtin decided not to pass the leased delivery on (spec section 4.7). It
	// returns filtered, or cancelled for a delivery of a cancelled run.
	Filter(ctx context.Context, f FilterRequest) (DeliveryState, error)
	// Dedupe is the Dedupe unit (spec section 5.11): the leased delivery's item passes, published unchanged as by
	// Done, when its key was not let through by the step in the same generation within the window; otherwise the
	// delivery is filtered with FilterReasonDuplicate. It returns the state the delivery ended in, as Done does.
	Dedupe(ctx context.Context, d DedupeRequest) (DeliveryState, error)
	// StartReplay is the Start replay unit (spec section 5.7): a replay run on the revision named, the pipeline's
	// next generation, and a delivery of the step for each selected item. It returns the run's generation, and
	// created false when the run already existed: the request already started it.
	StartReplay(ctx context.Context, r ReplayStart) (generation int64, created bool, err error)
	// Cancel is the Cancel unit.
	Cancel(ctx context.Context, pipeline, runID string) error
	// ReplayDeadLetters is the Dead-letter replay unit for the open dead letters of a run (all of them when ids is
	// empty). migrate moves the run to the pipeline's active revision first. It returns how many were replayed;
	// an unrouted output that still matches no route stays open.
	ReplayDeadLetters(ctx context.Context, pipeline, runID string, ids []string, migrate bool) (int, error)
	// DiscardDeadLetters is the Dead-letter discard unit; it returns how many open dead letters it discarded.
	DiscardDeadLetters(ctx context.Context, pipeline string, ids []string) (int, error)
	// ExpireLeases ends every lease expired by the store clock, as a failed attempt (class leaseExpired): pending
	// after backoff while attempts remain, dead otherwise, cancelled for a cancelled run. It returns the leases it
	// ended, with the fencing token each was held under.
	ExpireLeases(ctx context.Context) ([]LeaseRef, error)
	// SetPaused pauses or resumes a step, or the whole pipeline when step is empty.
	SetPaused(ctx context.Context, pipeline, step string, paused bool) error
	// RecordEval stores the scores of an eval run, all in one unit, at the store clock's instant.
	RecordEval(ctx context.Context, results []EvalResult) error
	// SkipFirings records each scheduled time of the cron trigger that catch-up none passed over (spec section
	// 4.6), with a firingSkipped event, all in one unit. A time already recorded is a no-op.
	SkipFirings(ctx context.Context, pipeline, trigger string, times []time.Time) error
	// Rollup is the rollup projector's unit (spec section 10.2): it rolls the events recorded since its last pass
	// up into one-minute metric rollups, with a sample of each step's queue depth and oldest pending age, and moves
	// its cursor in the same transaction, so a pass that fails leaves no trace and a pass repeated finds nothing
	// new. It returns how many events it read, at most RollupBatch: a full batch means more are waiting.
	Rollup(ctx context.Context) (int, error)
	// RecordInsights applies one pass of the insights projector (spec section 11.1): each insight found is opened
	// or updated by fingerprint, a resolved one found again reopens, and the open or acknowledged insights of the
	// evaluated kinds not found are resolved; muted ones keep their state. It records insightOpened and
	// insightResolved events.
	RecordInsights(ctx context.Context, r InsightReport) error
	// SetInsightState acknowledges or mutes an insight, or reopens one (open).
	SetInsightState(ctx context.Context, pipeline, id string, state InsightState) error

	Runs(ctx context.Context, f RunFilter) ([]Run, error)
	Run(ctx context.Context, pipeline, id string) (Run, error)
	Items(ctx context.Context, f ItemFilter) ([]Item, error)
	Deliveries(ctx context.Context, f DeliveryFilter) ([]Delivery, error)
	DeadLetters(ctx context.Context, f DeadLetterFilter) ([]DeadLetter, error)
	// Events returns up to limit events with a sequence number above afterSeq, in sequence order.
	Events(ctx context.Context, afterSeq int64, limit int) ([]Event, error)
	// PipelineEvents returns the events f selects, in sequence order.
	PipelineEvents(ctx context.Context, f EventFilter) ([]Event, error)
	// MetricRollups returns the rollups f selects.
	MetricRollups(ctx context.Context, f MetricFilter) ([]MetricRollup, error)
	// Insights returns the insights f selects.
	Insights(ctx context.Context, f InsightFilter) ([]Insight, error)
	ActiveRevision(ctx context.Context, pipeline string) (RevisionID, error)
	// OpenRevisions returns the revisions of the runs not yet settled, in ascending order: the revisions whose steps
	// may still have deliveries to dispatch.
	OpenRevisions(ctx context.Context) ([]RevisionID, error)
	Revision(ctx context.Context, id RevisionID) (Revision, error)
	// Artifacts returns the artifact store entries of a pipeline (all pipelines when empty), by pipeline and hash.
	Artifacts(ctx context.Context, pipeline string) ([]Artifact, error)
	// AttemptArtifacts returns the artifacts the attempts of a delivery kept, by attempt then name.
	AttemptArtifacts(ctx context.Context, pipeline, deliveryID string) ([]AttemptArtifact, error)
	PipelineState(ctx context.Context, pipeline string) (PipelineState, error)
	// Pipelines returns every pipeline's state, by name.
	Pipelines(ctx context.Context) ([]PipelineState, error)
	// EvalResults returns the recorded scores of an action, newest first.
	EvalResults(ctx context.Context, action string) ([]EvalResult, error)
	// LastFiring returns the latest scheduled time the cron trigger fired a cron run for or skipped, zero when it
	// never did: where catch-up resumes. Manual firings of the trigger do not count.
	LastFiring(ctx context.Context, pipeline, trigger string) (time.Time, error)
}
