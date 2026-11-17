package storetest

import (
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// Area is a share of the conformance suite: the SQLite store runs each from a package of its own,
// internal/store/sqlstore/conformance/<area>, so that no package nears the test-time budget's package limit.
type Area string

// The areas.
const (
	// Delivery: the pipeline, run and delivery units: revisions, firings, leases, done and fail, routes, built-in
	// steps, budgets, dead letters and replays, the writer.
	Delivery Area = "delivery"
	// Review: reviews, audits and fallbacks, imports, insights, evals, proposals and the API reads.
	Review Area = "review"
	// Retention: retention, purges and rollups, evidence, nodes, and units facing a missing table.
	Retention Area = "retention"
)

// Test is a conformance test.
type Test struct {
	Name string
	Area Area
	Run  func(t *testing.T, open Opener)
}

// SchemaChanging names the tests whose stores change the schema (they rename a table): a SQLite Memory store they
// used never goes back to the pool (sqlitetest.Memory).
var SchemaChanging = []string{"TestUnitsSurfaceStoreErrors", "TestAuditsSurfaceStoreErrors"}

// Tests lists the conformance tests the build holds, by area: every exported Test function, those of the *_e2e.go
// files only under the e2e tag. TestTestsListed checks the list against the sources.
var Tests = slices.Concat(
	area(Delivery,
		TestDoneRefusesInvalidArtifacts, TestAttemptArtifacts, TestReservationChargedWhenUnreported,
		TestFailCostReplacesReservation, TestFilter, TestBuiltinUnitsOnCancelledRun, TestDedupeWindow,
		TestReplayUnroutedWithoutMigrateStaysOpen, TestReplayUnroutedWithMigrate, TestReplayErrors, TestDiscard,
		TestReplayMigrateToRevisionWithoutStep, TestReplayMigrateRefusedWhileLiveStepMissing, TestOrphanStepEndsDead,
		TestDoneStaleLease, TestDoneInvalidOutput, TestDoneKeyRule, TestDoneRouteEvaluationFails,
		TestDoneRouteDoesNotCompile, TestSkipFiringsAndLastFiring, TestPipelines, TestLease, TestRenew,
		TestApplyRevision, TestApplyRevisionPausedSteps, TestFire, TestFireRevisionOfAnotherPipeline, TestSetPaused,
		TestDisable, TestTriggerTokens, TestRollback, TestRandomUnitSequences, TestStartReplay,
		TestStartReplayRefusals, TestReplayRefusesExpiredPayloads, TestCancelWhileLeased, TestCancelFailAndExpiry,
		TestCancelPendingAndRetrying, TestCancelErrors, TestOpenRevisions, TestCloseDrainsQueuedUnits,
		TestFailpointBeforeCommit, TestFailpointAfterCommit, TestDoneFirstCommit, TestRevisionNumbersSurviveACrash),
	area(Review,
		TestEvalResults, TestInsightLifecycle, TestInsightRequests, TestRecordInsightsFailpoint, TestReviewDecisions,
		TestReviewTimeouts, TestAutoApproveAndSamples, TestReviewTimeoutSkipsABrokenReview, TestClosedStore,
		TestCancelledContext, TestReads, TestAPIReads, TestImportCopiesAndRuns, TestImportPausedConsumerAndCancel,
		TestImportProducerReplay, TestImportBackfill, TestAbstainAndFallback, TestAudits, TestAuditBudget,
		TestFallbackEdges, TestAuditsSurfaceStoreErrors, TestProposalLifecycle, TestProposalMovedBase,
		TestProposalSuppression, TestProposalOpenFingerprint, TestProposalPublishing, TestProposalUnitsFailpoints),
	area(Retention,
		TestUnitsSurfaceStoreErrors, TestNodes, TestRollupMath, TestDoneRefusesInvalidMetrics,
		TestRetainKeepsWhatUnfinishedWorkNeeds, TestRetainEventsTranscriptsRollups, TestPurgeResumesAfterCrash,
		TestEvidenceExports, TestEvidenceImports),
	e2eTests,
)

// area lists tests of an area, each named after its function.
func area(a Area, fns ...func(t *testing.T, open Opener)) []Test {
	tests := make([]Test, len(fns))
	for i, fn := range fns {
		name := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
		tests[i] = Test{Name: name[strings.LastIndex(name, ".")+1:], Area: a, Run: fn}
	}
	return tests
}

// RunArea runs the tests of an area against the stores open opens, each as a subtest named after its function:
// scripts/test-times measures each subtest of a test named TestConformance as a test of its own.
func RunArea(t *testing.T, a Area, open Opener) {
	for _, tc := range Tests {
		if tc.Area == a {
			t.Run(tc.Name, func(t *testing.T) { tc.Run(t, open) })
		}
	}
}
