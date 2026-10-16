package sqlstore_test

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// The conformance suite (store/storetest), one top-level test per function, so that test-times measures each.
// TestConformanceListed keeps the list in step with storetest.Tests.

func TestDoneRefusesInvalidArtifacts(t *testing.T) {
	storetest.TestDoneRefusesInvalidArtifacts(t, memoryStore)
}

func TestAttemptArtifacts(t *testing.T) {
	storetest.TestAttemptArtifacts(t, memoryStore)
}

func TestRunBudgetConcurrentLeases(t *testing.T) {
	storetest.TestRunBudgetConcurrentLeases(t, memoryStore)
}

func TestReservationChargedWhenUnreported(t *testing.T) {
	storetest.TestReservationChargedWhenUnreported(t, memoryStore)
}

func TestFailCostReplacesReservation(t *testing.T) {
	storetest.TestFailCostReplacesReservation(t, memoryStore)
}

func TestBudgetLeavesUserPauses(t *testing.T) {
	storetest.TestBudgetLeavesUserPauses(t, memoryStore)
}

func TestFilter(t *testing.T) {
	storetest.TestFilter(t, memoryStore)
}

func TestBuiltinUnitsOnCancelledRun(t *testing.T) {
	storetest.TestBuiltinUnitsOnCancelledRun(t, memoryStore)
}

func TestDedupeWindow(t *testing.T) {
	storetest.TestDedupeWindow(t, memoryStore)
}

func TestReplayUnroutedWithoutMigrateStaysOpen(t *testing.T) {
	storetest.TestReplayUnroutedWithoutMigrateStaysOpen(t, memoryStore)
}

func TestReplayUnroutedWithMigrate(t *testing.T) {
	storetest.TestReplayUnroutedWithMigrate(t, memoryStore)
}

func TestReplayErrors(t *testing.T) {
	storetest.TestReplayErrors(t, memoryStore)
}

func TestDiscard(t *testing.T) {
	storetest.TestDiscard(t, memoryStore)
}

func TestReplayMigrateToRevisionWithoutStep(t *testing.T) {
	storetest.TestReplayMigrateToRevisionWithoutStep(t, memoryStore)
}

func TestReplayMigrateRefusedWhileLiveStepMissing(t *testing.T) {
	storetest.TestReplayMigrateRefusedWhileLiveStepMissing(t, memoryStore)
}

func TestOrphanStepEndsDead(t *testing.T) {
	storetest.TestOrphanStepEndsDead(t, memoryStore)
}

func TestReplayRestartsTheRetryBudget(t *testing.T) {
	storetest.TestReplayRestartsTheRetryBudget(t, memoryStore)
}

func TestDoneStaleLease(t *testing.T) {
	storetest.TestDoneStaleLease(t, memoryStore)
}

func TestDoneInvalidOutput(t *testing.T) {
	storetest.TestDoneInvalidOutput(t, memoryStore)
}

func TestDoneKeyRule(t *testing.T) {
	storetest.TestDoneKeyRule(t, memoryStore)
}

func TestDoneRouteEvaluationFails(t *testing.T) {
	storetest.TestDoneRouteEvaluationFails(t, memoryStore)
}

func TestDoneRouteDoesNotCompile(t *testing.T) {
	storetest.TestDoneRouteDoesNotCompile(t, memoryStore)
}

func TestUnitsSurfaceStoreErrors(t *testing.T) {
	storetest.TestUnitsSurfaceStoreErrors(t, memoryStore)
}

func TestEvalResults(t *testing.T) {
	storetest.TestEvalResults(t, memoryStore)
}

func TestSkipFiringsAndLastFiring(t *testing.T) {
	storetest.TestSkipFiringsAndLastFiring(t, memoryStore)
}

func TestPipelines(t *testing.T) {
	storetest.TestPipelines(t, memoryStore)
}

func TestLeasePipelineSlots(t *testing.T) {
	storetest.TestLeasePipelineSlots(t, memoryStore)
}

func TestInsightLifecycle(t *testing.T) {
	storetest.TestInsightLifecycle(t, memoryStore)
}

func TestInsightRequests(t *testing.T) {
	storetest.TestInsightRequests(t, memoryStore)
}

func TestRecordInsightsFailpoint(t *testing.T) {
	storetest.TestRecordInsightsFailpoint(t, memoryStore)
}

func TestLease(t *testing.T) {
	storetest.TestLease(t, memoryStore)
}

func TestLeaseSlotsAndExclusions(t *testing.T) {
	storetest.TestLeaseSlotsAndExclusions(t, memoryStore)
}

func TestRenew(t *testing.T) {
	storetest.TestRenew(t, memoryStore)
}

func TestApplyRevision(t *testing.T) {
	storetest.TestApplyRevision(t, memoryStore)
}

func TestApplyRevisionPausedSteps(t *testing.T) {
	storetest.TestApplyRevisionPausedSteps(t, memoryStore)
}

func TestFire(t *testing.T) {
	storetest.TestFire(t, memoryStore)
}

func TestFireRevisionOfAnotherPipeline(t *testing.T) {
	storetest.TestFireRevisionOfAnotherPipeline(t, memoryStore)
}

func TestSetPaused(t *testing.T) {
	storetest.TestSetPaused(t, memoryStore)
}

func TestDisable(t *testing.T) {
	storetest.TestDisable(t, memoryStore)
}

func TestRollback(t *testing.T) {
	storetest.TestRollback(t, memoryStore)
}

func TestRandomUnitSequences(t *testing.T) {
	storetest.TestRandomUnitSequences(t, memoryStore)
}

func TestStartReplay(t *testing.T) {
	storetest.TestStartReplay(t, memoryStore)
}

func TestConcurrentReplaysTakeDistinctGenerations(t *testing.T) {
	storetest.TestConcurrentReplaysTakeDistinctGenerations(t, memoryStore)
}

func TestStartReplayRefusals(t *testing.T) {
	storetest.TestStartReplayRefusals(t, memoryStore)
}

func TestReplayRefusesExpiredPayloads(t *testing.T) {
	storetest.TestReplayRefusesExpiredPayloads(t, memoryStore)
}

func TestReviewDecisions(t *testing.T) {
	storetest.TestReviewDecisions(t, memoryStore)
}

func TestReviewTimeouts(t *testing.T) {
	storetest.TestReviewTimeouts(t, memoryStore)
}

func TestAutoApproveAndSamples(t *testing.T) {
	storetest.TestAutoApproveAndSamples(t, memoryStore)
}

func TestReviewTimeoutSkipsABrokenReview(t *testing.T) {
	storetest.TestReviewTimeoutSkipsABrokenReview(t, memoryStore)
}

func TestRollupMath(t *testing.T) {
	storetest.TestRollupMath(t, memoryStore)
}

func TestDoneRefusesInvalidMetrics(t *testing.T) {
	storetest.TestDoneRefusesInvalidMetrics(t, memoryStore)
}

func TestRollupFailpoint(t *testing.T) {
	storetest.TestRollupFailpoint(t, memoryStore)
}

func TestMetricAndEventFilters(t *testing.T) {
	storetest.TestMetricAndEventFilters(t, memoryStore)
}

func TestCancelWhileLeased(t *testing.T) {
	storetest.TestCancelWhileLeased(t, memoryStore)
}

func TestCancelFailAndExpiry(t *testing.T) {
	storetest.TestCancelFailAndExpiry(t, memoryStore)
}

func TestCancelPendingAndRetrying(t *testing.T) {
	storetest.TestCancelPendingAndRetrying(t, memoryStore)
}

func TestCancelErrors(t *testing.T) {
	storetest.TestCancelErrors(t, memoryStore)
}

func TestOpenRevisions(t *testing.T) {
	storetest.TestOpenRevisions(t, memoryStore)
}

func TestConcurrentDone(t *testing.T) {
	storetest.TestConcurrentDone(t, memoryStore)
}

func TestCloseDrainsQueuedUnits(t *testing.T) {
	storetest.TestCloseDrainsQueuedUnits(t, memoryStore)
}

func TestFailpointBeforeCommit(t *testing.T) {
	storetest.TestFailpointBeforeCommit(t, memoryStore)
}

func TestFailpointAfterCommit(t *testing.T) {
	storetest.TestFailpointAfterCommit(t, memoryStore)
}

func TestClosedStore(t *testing.T) {
	storetest.TestClosedStore(t, memoryStore)
}

func TestCancelledContext(t *testing.T) {
	storetest.TestCancelledContext(t, memoryStore)
}

func TestReads(t *testing.T) {
	storetest.TestReads(t, memoryStore)
}

func TestAPIReads(t *testing.T) {
	storetest.TestAPIReads(t, memoryStore)
}

func TestDoneFirstCommit(t *testing.T) {
	storetest.TestDoneFirstCommit(t, memoryStore)
}

func TestRevisionNumbersSurviveACrash(t *testing.T) {
	storetest.TestRevisionNumbersSurviveACrash(t, memoryStore)
}
