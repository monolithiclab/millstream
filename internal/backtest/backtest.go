// Package backtest validates a patch against past items (spec section 11.3, validation step 3): the patch is applied
// to a scratch copy of its base revision's package, the steps it changes are replayed on a stratified sample of past
// items, from local history or imported evidence, in a scratch store, and the report compares the base revision's
// replay ("before") with the patched one's ("after"): agreement with the historical outputs, agreement with the
// reviewers' decisions, failure rate, cost and latency.
//
// A replay fires each sampled item's historical run again in a SQLite Memory store, with the run's own trigger, key
// and variables, so that the items it publishes have their historical ids (D9). A stubber decides every delivery
// before a node runs it:
//
//   - a delivery off the sampled lineages is cut short (filtered, notReplayed): siblings a step published beside a
//     sampled item are not replayed;
//   - an action with effects: external never runs: its delivery takes its recorded outcome when its input is the
//     historical one, and otherwise publishes nothing;
//   - a review step takes its recorded decision when its input is the historical one, and otherwise stops there
//     (nobody reviews a backtest), unless the patch changes it: then its own rules apply (autoApprove);
//   - an unchanged step whose input is the historical one takes its recorded outcome (outputs, filter or failure);
//   - everything else runs: the changed steps, and the unchanged steps reached by items the history does not hold
//     (a topology change admitting items the historical run discarded), whose agent cost the improve budget pays.
//
// Proposed code runs only where spec section 15.3 allows it: the exec actions whose files the patch touches run in
// the container sandbox (Options.Sandbox: built and run with no network, read-only, without credentials), and a
// backtest without one refuses them, since exec actions otherwise run with the node's privileges; container actions
// whose files it touches run without the network; agents run in their sandbox, as configured for the deployment.
package backtest

import (
	"context"
	"fmt"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// DefaultSample is improve.backtest.sample's default (spec section 14).
const DefaultSample = 200

// History sources.
const (
	// SourceLocal is the deployment's own items, deliveries and reviews.
	SourceLocal = "local"
	// SourceEvidence is the imported evidence bundles of the pipeline (spec section 11.2).
	SourceEvidence = "evidence"
)

// Refusals. Each wraps a store error, which the API maps to its status.
var (
	// ErrBudget: improve.budget.dayUsd is spent for the day, or the backtest reached it.
	ErrBudget = fmt.Errorf("improve budget reached: %w", store.ErrConflict)
	// ErrUntrusted: the patch changes exec code and no container sandbox can run it.
	ErrUntrusted = fmt.Errorf("proposed code outside the sandbox: %w", store.ErrInvalid)
	// ErrNoHistory: no past item reaches the steps the patch changes.
	ErrNoHistory = fmt.Errorf("no history to replay: %w", store.ErrInvalid)
	// ErrProduction: a production deployment runs no backtest (spec section 11.2, D31).
	ErrProduction = fmt.Errorf("backtests run in development only: %w", store.ErrConflict)
)

// Runner runs backtests. Proposals validate through it (Task 5), the API and the CLI too.
type Runner interface {
	Run(ctx context.Context, req Request) (*Report, error)
}

// Request is a backtest to run.
type Request struct {
	// Pipeline is the pipeline whose revision the patch applies to.
	Pipeline string `json:"pipeline"`
	// BaseRevision is the package hash of that revision. A patch stating another base is refused.
	BaseRevision string `json:"baseRevision"`
	// Patch is the unified diff (package patch).
	Patch []byte `json:"patch"`
	// Sample is how many past deliveries to sample; zero means improve.backtest.sample, else DefaultSample.
	Sample int `json:"sample,omitempty"`
	// Steps are replayed besides those the patch changes; each must be a step of the patched pipeline.
	Steps []string `json:"steps,omitempty"`
	// Source is SourceLocal or SourceEvidence; empty means local history when it reaches the changed steps, else
	// imported evidence.
	Source string `json:"source,omitempty"`
}

// Report is a backtest's outcome.
type Report struct {
	Pipeline     string `json:"pipeline"`
	BaseRevision string `json:"baseRevision"`
	// PatchHash is the patch's hash (patch.Hash), PatchedRevision the patched package's.
	PatchHash       string `json:"patchHash"`
	PatchedRevision string `json:"patchedRevision"`
	// Source is where the sample came from; Bundles names the evidence bundles read, for SourceEvidence.
	Source  string   `json:"source"`
	Bundles []string `json:"bundles,omitempty"`
	// ChangedSteps are the steps replayed (those the patch changes and Request.Steps), sorted.
	ChangedSteps []string `json:"changedSteps"`
	// Sampled counts the sampled deliveries, Strata them by stratum (done, filtered, dead, approved, rejected,
	// edited), and Runs the historical runs replayed for them.
	Sampled int            `json:"sampled"`
	Strata  map[string]int `json:"strata"`
	Runs    int            `json:"runs"`
	// Before is the base revision's replay, After the patched one's, over the whole sample.
	Before Metrics `json:"before"`
	After  Metrics `json:"after"`
	// Steps compares the two replays per changed step.
	Steps []StepReport `json:"steps"`
	// Divergence lists the revisions the sample was recorded on other than the base: its outcomes may differ for
	// that reason alone.
	Divergence []Divergence `json:"divergence,omitempty"`
	// Warnings are what the reader should know: divergence, deliveries left out of the population and why.
	Warnings []string `json:"warnings,omitempty"`
	// CostUSD is what both replays cost, charged to improve.budget.dayUsd.
	CostUSD    float64   `json:"costUsd"`
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt"`
}

// StepReport is one changed step's replays.
type StepReport struct {
	Step   string  `json:"step"`
	Before Metrics `json:"before"`
	After  Metrics `json:"after"`
}

// Metrics measure one replay, over the sample or one step's deliveries on the sampled lineages.
type Metrics struct {
	// Compared counts the deliveries with a historical counterpart (the same step and input item), and Agreed those
	// that ended alike: the same state and the same outputs. Agreement is Agreed / Compared, 0 when none.
	Compared  int     `json:"compared"`
	Agreed    int     `json:"agreed"`
	Agreement float64 `json:"agreement"`
	// NewlyReached counts the deliveries without a historical counterpart: items the patch admits. NoLongerReached
	// counts the historical deliveries on the sampled lineages the replay never made.
	NewlyReached    int `json:"newlyReached"`
	NoLongerReached int `json:"noLongerReached"`
	// Reviewed counts the decided reviews on the sampled lineages, and ReviewAgreed those the replay agrees with:
	// an approved item still reaching its review, an edited one reaching it with the edited values, a rejected one
	// no longer reaching it. ReviewAgreement is ReviewAgreed / Reviewed, 0 when none.
	Reviewed        int     `json:"reviewed"`
	ReviewAgreed    int     `json:"reviewAgreed"`
	ReviewAgreement float64 `json:"reviewAgreement"`
	// Approved counts the approvals among those reviews, and ApprovalsDropped the approved items the replay no
	// longer brings to their review: what a filter would wrongly drop (improve.filter.maxFalseDrop, spec section
	// 11.6).
	Approved         int `json:"approved"`
	ApprovalsDropped int `json:"approvalsDropped"`
	// Ran counts the deliveries an action or builtin ran (not stubbed), Failed those that ended dead, and
	// FailureRate is Failed / Ran, 0 when none.
	Ran         int     `json:"ran"`
	Failed      int     `json:"failed"`
	FailureRate float64 `json:"failureRate"`
	// CostUSD is what the deliveries that ran cost, and LatencyMs their mean latency (queue wait and work).
	CostUSD   float64 `json:"costUsd"`
	LatencyMs float64 `json:"latencyMs"`
}

// Divergence is a revision the sample was recorded on, other than the base.
type Divergence struct {
	// PackageHash is that revision's package hash.
	PackageHash string `json:"packageHash"`
	// Sampled counts the sampled deliveries recorded on it.
	Sampled int `json:"sampled"`
}
