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
	// SkipFirings records a firingSkipped event for each cron firing key of the trigger that catch-up none passed
	// over (spec section 4.6), all in one unit.
	SkipFirings(ctx context.Context, pipeline, trigger string, keys []string) error

	Runs(ctx context.Context, f RunFilter) ([]Run, error)
	Run(ctx context.Context, pipeline, id string) (Run, error)
	Items(ctx context.Context, f ItemFilter) ([]Item, error)
	Deliveries(ctx context.Context, f DeliveryFilter) ([]Delivery, error)
	DeadLetters(ctx context.Context, f DeadLetterFilter) ([]DeadLetter, error)
	// Events returns up to limit events with a sequence number above afterSeq, in sequence order.
	Events(ctx context.Context, afterSeq int64, limit int) ([]Event, error)
	ActiveRevision(ctx context.Context, pipeline string) (RevisionID, error)
	Revision(ctx context.Context, id RevisionID) (Revision, error)
	PipelineState(ctx context.Context, pipeline string) (PipelineState, error)
	// Pipelines returns every pipeline's state, by name.
	Pipelines(ctx context.Context) ([]PipelineState, error)
	// LastFiring returns the greatest key the trigger fired a run under or skipped, or "" when it never did. Cron
	// keys sort as their times, so for a cron trigger it is the last scheduled time handled.
	LastFiring(ctx context.Context, pipeline, trigger string) (string, error)
}
