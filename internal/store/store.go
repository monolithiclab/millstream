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
	// ErrRevoked means a node id was revoked (millstream nodes revoke): the engine refuses it from then on (spec
	// section 15.7).
	ErrRevoked = errors.New("node revoked")
	// ErrUnavailable means the database cannot serve the request now: out of resources or connections, shutting
	// down or starting, or unreachable (Postgres SQLSTATE classes 53 and 08, 57P01, 57P03). Retrying later may
	// succeed.
	ErrUnavailable = errors.New("store unavailable")
)

// Retryable reports whether err means the store cannot serve now, rather than that the request failed: closed,
// read-only, its write connection lost, the database unavailable, or the caller's context ended. The HTTP API and
// the node protocol answer it 503 unavailable.
func Retryable(err error) bool {
	for _, e := range []error{ErrClosed, ErrReadOnly, ErrWriteLost, ErrUnavailable, context.Canceled,
		context.DeadlineExceeded} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// RollupBatch is the most events one Rollup pass reads.
const RollupBatch = 5000

// Store is the log store. Units block until the transaction that applied them commits. A unit that fails leaves no
// trace, whatever else committed in the same batch.
type Store interface {
	Close() error
	// OnCommit sets fn to be called after each batch that recorded events commits, with the sequence number of its
	// last event: what live event streams wake on (spec section 12.1). fn must not block; nil stops the calls.
	OnCommit(fn func(lastSeq int64))

	// ApplyRevision stores a package and makes its revision the pipeline's active one, on condition that the active
	// revision's package is still pkg.Base when it is set (ErrConflict otherwise), recording proposalApplied for the
	// accepted proposals it carries over their base. Applying the package that is already active is a no-op returning its revision.
	ApplyRevision(ctx context.Context, pkg PackageRecord) (RevisionID, error)
	// Fire is the Trigger unit: the run, its trigger item and the deliveries of the steps reading the trigger
	// stream. created is false when the run already existed: the firing already happened.
	Fire(ctx context.Context, f Firing) (created bool, err error)
	// Lease is the Lease unit: it leases up to req.Max pending deliveries that are due, oldest first per step,
	// round-robin across pipelines.
	Lease(ctx context.Context, req LeaseRequest) ([]Lease, error)
	// Renew extends the leases still held by owner with matching fencing tokens and returns the others. A remote
	// node's renewal records its heartbeat; a revoked node's is ErrRevoked.
	Renew(ctx context.Context, owner string, refs []LeaseRef, ttl time.Duration) (lost []LeaseRef, err error)
	// Done is the Done unit. It returns the state the delivery ended in: done, dead (key rule or a route that
	// fails to evaluate) or cancelled (its run was cancelled).
	Done(ctx context.Context, d DoneRequest) (DeliveryState, error)
	// Fail is the Fail unit, or the Abstain unit for a failure of class ClassAbstain. It returns the state the delivery ended in: pending (retry scheduled), dead or
	// cancelled.
	Fail(ctx context.Context, f FailRequest) (DeliveryState, error)
	// Abstain is the Abstain unit (spec section 5.11): the leased delivery's action abstained (f.Message is its
	// reason; f.Class and f.Retryable are not read). It goes to its step's fallback action, pending at once with its
	// retry budget restarted, or, when the step has no fallback for abstain or the fallback itself abstained, it
	// fails as a non-retryable error of class ClassAbstain. It returns the state the delivery ended in.
	Abstain(ctx context.Context, f FailRequest) (DeliveryState, error)
	// EndAudit ends a leased fallback audit (spec section 11.5): done with the fallback's outputs and their
	// agreement with the primary's, or failed, never retried; its reservation settles and its cost is charged to
	// the pipeline's day. An audit of an item erased meanwhile is cancelled, its outputs dropped.
	EndAudit(ctx context.Context, req AuditEnd) error
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
	// CreateTriggerTokens is the Trigger tokens unit of an apply (spec section 4.6): it stores, by trigger, the
	// SHA-256 (hex) of a new token for each webhook trigger of the pipeline that has none yet, and returns the
	// triggers it stored one for, sorted. A trigger that already has a token keeps it.
	CreateTriggerTokens(ctx context.Context, pipeline string, hashes map[string]string) ([]string, error)
	// RotateTriggerToken is the Trigger tokens unit of a rotation: it replaces the token hash of a webhook trigger of
	// the pipeline's active revision. A pipeline or trigger that does not exist is ErrNotFound, a trigger of another
	// kind ErrInvalid.
	RotateTriggerToken(ctx context.Context, pipeline, trigger, hash string) error
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
	// insightResolved events, and insightEscalated when an open or acknowledged insight's severity rises.
	RecordInsights(ctx context.Context, r InsightReport) error
	// SetInsightState acknowledges or mutes an insight, or reopens one (open).
	SetInsightState(ctx context.Context, pipeline, id string, state InsightState) error
	// RegisterNode records a remote node joining, or joining again from a new process: its version, labels,
	// runtimes and slots, active, heard from now. A revoked id is ErrRevoked.
	RegisterNode(ctx context.Context, n Node) (NodeState, error)
	// SetNodeState drains or revokes a node (spec sections 5.10 and 15.7). With expireLeases, every lease the node
	// holds ends at once, as an expired lease does (its attempt consumed), and those leases are returned. Revoking
	// an id never registered records it, so it is refused from then on; draining one is ErrNotFound. Renew updates
	// a registered node's last heartbeat.
	SetNodeState(ctx context.Context, id string, state NodeState, expireLeases bool) ([]LeaseRef, error)
	// Retain is the retention unit (spec section 9.3): it tombstones the items of each stream older than its
	// retention, except those unfinished work needs (a delivery not terminal, an open dead letter or one whose
	// dead delivery they are the input of, an open review or review sample), dropping their artifact references,
	// and deletes the events older than req.Events that every projection has read (never the last one), the
	// attempt artifacts older than req.Transcripts but those of a live delivery or of an open dead letter's, and
	// the metric rollups older than req.Rollups. Each kind is bounded by req.Limit.
	Retain(ctx context.Context, req RetentionRequest) (RetentionResult, error)
	// StartPurge is the first unit of a purge (spec section 9.3): it records the purge and fences its lineage, the
	// items named, their descendants and, following imports, their copies in other pipelines and theirs. Fenced
	// items are tombstoned erased at once, their pending and parked deliveries cancelled with their reviews, their
	// open review samples closed and their open dead letters discarded; a leased one is cancelled when it reports,
	// its outputs discarded. A request whose id names an existing purge returns it, with created false.
	StartPurge(ctx context.Context, req PurgeRequest) (p Purge, created bool, err error)
	// ErasePurge erases up to PurgeBatch fenced items of an erasing purge: payloads, keys, artifact references,
	// the artifacts their deliveries' attempts kept, error messages, review renderings and edits, dedupe keys. Once
	// none is left, the purge moves to sweeping. A purge in another state is returned as it is.
	ErasePurge(ctx context.Context, id string) (Purge, error)
	// FinishPurge ends a sweeping purge, once the engine deleted the artifact files no surviving item references,
	// with a purgeCompleted event in each pipeline it erased items of. A done purge is returned as it is.
	FinishPurge(ctx context.Context, id string) (Purge, error)
	// EvidenceKey returns the deployment's redaction key (spec section 11.2), made at random by its first call and
	// kept in the store, never exported.
	EvidenceKey(ctx context.Context) ([]byte, error)
	// RecordExport records an evidence bundle exported, with the item keys it holds. An id already recorded is
	// ErrConflict.
	RecordExport(ctx context.Context, e EvidenceExport) error
	// RecordImport records an evidence bundle imported, its file already in the artifact store, and counts a
	// reference to it. A bundle expired by the store clock is ErrConflict; one already imported is left as it is,
	// with created false.
	RecordImport(ctx context.Context, e EvidenceImport) (created bool, err error)
	// ExpireEvidence deletes the evidence imports expired by the store clock, dropping their references, and returns
	// them: the retention job then deletes their files.
	ExpireEvidence(ctx context.Context) ([]EvidenceImport, error)
	// CreateProposal is the creation of a proposal (spec section 11.3), drafting: its base must be the pipeline's
	// active revision (ErrConflict otherwise), and its fingerprint must not be suppressed: a rejected proposal with
	// the same fingerprint suppresses it until an insight it answers reaches twice the severity weight it had at
	// the rejection (ErrConflict). A proposal answering no insight is never suppressed. It records proposalCreated.
	CreateProposal(ctx context.Context, p NewProposal) (Proposal, error)
	// StartValidation moves a drafting, ready or stale proposal to validating, and takes over a validating one last
	// updated before takeOver (its validation presumed lost: zero never takes over); any other state is
	// ErrConflict.
	StartValidation(ctx context.Context, id string, takeOver time.Time) (Proposal, error)
	// ResetValidations moves every validating proposal back to drafting, as an engine does when it starts or takes
	// the lead: no validation outlives the engine that ran it. It returns their ids.
	ResetValidations(ctx context.Context) ([]string, error)
	// RecordValidation records the report of a validating proposal: ready when it passed, else drafting again. A
	// rebase replaces its base and patch; the base must then be the pipeline's active revision. A proposal whose base
	// is no longer the active revision becomes stale instead. It records proposalValidated.
	RecordValidation(ctx context.Context, v Validation) (Proposal, error)
	// AcceptProposal is the Proposal acceptance unit (spec section 5.11): a ready proposal whose patch hash is the
	// one named is accepted only when the pipeline's active revision is still its base, and every other open
	// proposal of the pipeline touching one of its paths or steps becomes stale in the same transaction. When the
	// base moved, the proposal becomes stale, committed, and the call answers it with ErrConflict. A proposal not
	// ready, or whose hash differs, is ErrConflict and left as it is.
	AcceptProposal(ctx context.Context, id, patchSHA256, by string) (Proposal, error)
	// RejectProposal rejects an open or stale proposal with a reason, required (ErrInvalid without), recording the
	// highest severity of its insights for the suppression of its fingerprint.
	RejectProposal(ctx context.Context, id, reason, by string) (Proposal, error)
	// RecordPublished records that an accepted proposal went to its publish action: proposalPublished, with the
	// action's name and where it put the proposal. A proposal that is not accepted is ErrConflict.
	RecordPublished(ctx context.Context, id, publisher, where string) error
	// SetProposalToken replaces the hash (SHA-256, hex) of the proposals-only API token (spec section 11.3).
	SetProposalToken(ctx context.Context, hash string) error
	// DeleteArtifacts removes the artifact store entries named that no item or attempt references any more (their
	// reference count is zero): the artifact sweep, once their files are gone. It returns how many it removed.
	DeleteArtifacts(ctx context.Context, keys []ArtifactKey) (int, error)

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
	// MetricTotals returns every series' running totals over every minute ever rolled up, retained or not, by
	// pipeline, name and labels.
	MetricTotals(ctx context.Context) ([]MetricTotal, error)
	// Audits returns the fallback audits f selects.
	Audits(ctx context.Context, f AuditFilter) ([]Audit, error)
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
	// EvalResults returns the recorded scores of an action, of every action when it is empty, newest first.
	EvalResults(ctx context.Context, action string) ([]EvalResult, error)
	// Nodes returns the remote nodes that registered or were revoked, by id.
	Nodes(ctx context.Context) ([]Node, error)
	// TriggerTokenHash returns the SHA-256 (hex) of a webhook trigger's token, ErrNotFound when it has none.
	TriggerTokenHash(ctx context.Context, pipeline, trigger string) (string, error)
	// EvidenceImports returns the evidence imports of a pipeline (every pipeline when empty), expired or not, oldest
	// first.
	EvidenceImports(ctx context.Context, pipeline string) ([]EvidenceImport, error)
	// EvidenceExports returns the evidence exports of a pipeline (every pipeline when empty), oldest first, with the
	// item keys they still hold.
	EvidenceExports(ctx context.Context, pipeline string) ([]EvidenceExport, error)
	// Proposal returns a proposal.
	Proposal(ctx context.Context, id string) (Proposal, error)
	// Proposals returns the proposals f selects, newest first.
	Proposals(ctx context.Context, f ProposalFilter) ([]Proposal, error)
	// ProposalTokenHash returns the hash of the proposals-only API token, ErrNotFound when none was minted.
	ProposalTokenHash(ctx context.Context) (string, error)
	// Purge returns a purge.
	Purge(ctx context.Context, id string) (Purge, error)
	// Purges returns the purges not yet done, oldest first: what the engine resumes.
	Purges(ctx context.Context) ([]Purge, error)
	// LastFiring returns the latest scheduled time the cron trigger fired a cron run for or skipped, zero when it
	// never did: where catch-up resumes. Manual firings of the trigger do not count.
	LastFiring(ctx context.Context, pipeline, trigger string) (time.Time, error)
}
