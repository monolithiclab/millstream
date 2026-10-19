package delivery_test

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// The conformance suite's share of this package (store/storetest): the pipeline, run and delivery units: revisions,
// firings, leases, done and fail, routes, built-in steps, budgets, dead letters and replays, the writer. One top-level
// test per function, so that test-times measures each; TestConformanceListed (internal/store/sqlstore) keeps the shares
// in step with storetest.Tests.

func TestDoneRefusesInvalidArtifacts(t *testing.T) {
	storetest.TestDoneRefusesInvalidArtifacts(t, memoryStore)
}

func TestAttemptArtifacts(t *testing.T) {
	storetest.TestAttemptArtifacts(t, memoryStore)
}

func TestReservationChargedWhenUnreported(t *testing.T) {
	storetest.TestReservationChargedWhenUnreported(t, memoryStore)
}

func TestFailCostReplacesReservation(t *testing.T) {
	storetest.TestFailCostReplacesReservation(t, memoryStore)
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

func TestSkipFiringsAndLastFiring(t *testing.T) {
	storetest.TestSkipFiringsAndLastFiring(t, memoryStore)
}

func TestPipelines(t *testing.T) {
	storetest.TestPipelines(t, memoryStore)
}

func TestLease(t *testing.T) {
	storetest.TestLease(t, memoryStore)
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

func TestTriggerTokens(t *testing.T) {
	storetest.TestTriggerTokens(t, memoryStore)
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

func TestStartReplayRefusals(t *testing.T) {
	storetest.TestStartReplayRefusals(t, memoryStore)
}

func TestReplayRefusesExpiredPayloads(t *testing.T) {
	storetest.TestReplayRefusesExpiredPayloads(t, memoryStore)
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

func TestCloseDrainsQueuedUnits(t *testing.T) {
	storetest.TestCloseDrainsQueuedUnits(t, memoryStore)
}

func TestFailpointBeforeCommit(t *testing.T) {
	storetest.TestFailpointBeforeCommit(t, memoryStore)
}

func TestFailpointAfterCommit(t *testing.T) {
	storetest.TestFailpointAfterCommit(t, memoryStore)
}

func TestDoneFirstCommit(t *testing.T) {
	storetest.TestDoneFirstCommit(t, memoryStore)
}

func TestRevisionNumbersSurviveACrash(t *testing.T) {
	storetest.TestRevisionNumbersSurviveACrash(t, memoryStore)
}
