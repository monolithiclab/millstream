// Package store is Millstream's log store contract (spec sections 5 and 9.1): the records it holds and the atomic
// units of spec section 5.11 that change them. Every write goes through one writer loop that applies each unit
// whole (D23); reads use a separate pool. The pipeline id scopes every record (spec section 4.10), so runs, items,
// deliveries and dead letters are addressed by pipeline and id: two pipelines with a trigger of the same name fire
// runs with the same id without colliding. The SQLite (store/sqlite) and Postgres (store/postgres) stores share
// their implementation, store/sqlstore; store/storetest is the conformance suite both pass.
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
	// ErrReadOnly means the store was opened for reading only (a CLI command on Postgres, which never takes the
	// leadership lock): on Postgres the active engine is the only writer, and state changes go through its API.
	ErrReadOnly = errors.New("store opened read-only")
	// ErrWriteLost means the store lost its write connection, and on Postgres the leadership lock it held (spec
	// section 5.9): it can no longer write, and the engine steps down.
	ErrWriteLost = errors.New("write connection lost")
)

// RollupBatch is the most events one Rollup pass reads.
const RollupBatch = 5000

// Store is the log store. Units block until the transaction that applied them commits. A unit that fails leaves no
// trace, whatever else committed in the same batch.
type Store interface {
	Close() error
	// OnCommit sets fn to be called after each batch that recorded events commits, with the sequence number of its
	// last event: what live event streams wake on (spec section 12.1). fn must not block; nil stops the calls.
	OnCommit(fn func(lastSeq int64))

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
	// Park is the Park unit (spec section 5.11): a leased delivery of a review step parks awaitingReview in its
	// inbox, with a deadline when the step has a timeout, holding no lease or slot; or, auto-approved, it passes
	// unchanged as by Done, recorded as approved by rule, and becomes a review sample when req.Sample is set. It
	// returns the delivery's state, awaitingReview, done (or what Done ends in) or cancelled, and a parked review's
	// deadline as stored.
	Park(ctx context.Context, req ParkRequest) (ParkResult, error)
	// DecideReview is the Review decision unit: an open review at the version named is approved, its payload edited
	// and validated and passed on as by Done, or rejected, its delivery filtered with the reason. A review decided,
	// escalated or parked again since is ErrConflict, and one that timed out says when and what onTimeout applied;
	// an edit outside the editable pointers, a payload no output schema accepts or a missing reason code is
	// ErrInvalid.
	DecideReview(ctx context.Context, req ReviewDecisionRequest) (DeliveryState, error)
	// DecideSample is the Sample decision unit: an open review sample records agree or disagree; it never emits.
	DecideSample(ctx context.Context, req SampleDecisionRequest) error
	// ReviewTimeouts is the Review timeout unit: every open review past its deadline by the store clock gets its
	// onTimeout applied once (reject, approve, or escalate to another inbox with a fresh deadline), and every open
	// sample past its deadline becomes unreviewed. It returns what it changed.
	ReviewTimeouts(ctx context.Context) ([]TimedOutReview, error)
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
	// SetPaused pauses or resumes a step, or the whole pipeline when step is empty. Resuming the pipeline also
	// enables a disabled one; pausing a disabled pipeline is ErrConflict.
	SetPaused(ctx context.Context, pipeline, step string, paused bool) error
	// Disable is the Disable unit: the pipeline's triggers stop firing and its deliveries wait until it is resumed
	// (spec section 13). Disabling a disabled pipeline is a no-op.
	Disable(ctx context.Context, pipeline string) error
	// Rollback is the Rollback unit: it reactivates revision to of the pipeline, or, when to is zero, the newest
	// revision older than the active one, and returns it. Runs already started keep their revision (spec section
	// 5.7). A revision of another pipeline is ErrNotFound, the active revision or no older one ErrConflict.
	Rollback(ctx context.Context, pipeline string, to RevisionID) (RevisionID, error)
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
	// LastEventSeq returns the sequence number of the last event committed, zero when there is none.
	LastEventSeq(ctx context.Context) (int64, error)
	// PipelineEvents returns the events f selects, in sequence order.
	PipelineEvents(ctx context.Context, f EventFilter) ([]Event, error)
	// MetricRollups returns the rollups f selects.
	MetricRollups(ctx context.Context, f MetricFilter) ([]MetricRollup, error)
	// Insights returns the insights f selects.
	Insights(ctx context.Context, f InsightFilter) ([]Insight, error)
	// Reviews returns the reviews f selects, with their items.
	Reviews(ctx context.Context, f ReviewFilter) ([]Review, error)
	// Review returns the review of a delivery with its item; an empty pipeline looks in every pipeline, and a
	// delivery id two pipelines share is then ErrConflict.
	Review(ctx context.Context, pipeline, deliveryID string) (Review, error)
	// ReviewSamples returns the review samples f selects, with their items.
	ReviewSamples(ctx context.Context, f SampleFilter) ([]ReviewSample, error)
	// ReviewSample returns the review sample of a delivery, looked up as Review does.
	ReviewSample(ctx context.Context, pipeline, deliveryID string) (ReviewSample, error)
	ActiveRevision(ctx context.Context, pipeline string) (RevisionID, error)
	// OpenRevisions returns the revisions of the runs not yet settled, in ascending order: the revisions whose steps
	// may still have deliveries to dispatch.
	OpenRevisions(ctx context.Context) ([]RevisionID, error)
	Revision(ctx context.Context, id RevisionID) (Revision, error)
	// Revisions returns a pipeline's revisions, newest first.
	Revisions(ctx context.Context, pipeline string) ([]Revision, error)
	// Package returns the bundle of the package applied under hash.
	Package(ctx context.Context, hash string) ([]byte, error)
	// StreamCounts returns how many items each stream of a pipeline holds, by stream: those of one run when runID
	// is set.
	StreamCounts(ctx context.Context, pipeline, runID string) (map[string]int64, error)
	// StepStats returns each step's deliveries by state now, and those that finished since the instant given, by
	// step: the live state of a pipeline (spec section 12.1).
	StepStats(ctx context.Context, pipeline string, since time.Time) ([]StepStats, error)
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
