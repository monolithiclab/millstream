//go:build e2e

package sqlstore_test

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// The conformance tests only the e2e build runs; the default build's run from the conformance/... packages.

func init() { conformanceFiles = append(conformanceFiles, "conformance_e2e_test.go") }

func TestDoneCountsArtifactsPerItem(t *testing.T) {
	storetest.TestDoneCountsArtifactsPerItem(t, memoryStore)
}

func TestArtifactsTravelWithItems(t *testing.T) {
	storetest.TestArtifactsTravelWithItems(t, memoryStore)
}

func TestDayBudgetRollsOverByUTCDay(t *testing.T) {
	storetest.TestDayBudgetRollsOverByUTCDay(t, memoryStore)
}

func TestBudgetSkipsPastHeldBackDeliveries(t *testing.T) {
	storetest.TestBudgetSkipsPastHeldBackDeliveries(t, memoryStore)
}

func TestDedupeScopedByGeneration(t *testing.T) {
	storetest.TestDedupeScopedByGeneration(t, memoryStore)
}

func TestDedupePassPublishesTheItemUnchanged(t *testing.T) {
	storetest.TestDedupePassPublishesTheItemUnchanged(t, memoryStore)
}

func TestReplayDeadAgainCreatesNewDeadLetter(t *testing.T) {
	storetest.TestReplayDeadAgainCreatesNewDeadLetter(t, memoryStore)
}

func TestDiscardBeforeSettlement(t *testing.T) {
	storetest.TestDiscardBeforeSettlement(t, memoryStore)
}

func TestReplayDead(t *testing.T) {
	storetest.TestReplayDead(t, memoryStore)
}

func TestDoneTerminalStepDropsOutputs(t *testing.T) {
	storetest.TestDoneTerminalStepDropsOutputs(t, memoryStore)
}

func TestDoneFanOutAndSettle(t *testing.T) {
	storetest.TestDoneFanOutAndSettle(t, memoryStore)
}

func TestDoneRouting(t *testing.T) {
	storetest.TestDoneRouting(t, memoryStore)
}

func TestFailNonRetryableIsDeadAtOnce(t *testing.T) {
	storetest.TestFailNonRetryableIsDeadAtOnce(t, memoryStore)
}

func TestFailTruncatesMessage(t *testing.T) {
	storetest.TestFailTruncatesMessage(t, memoryStore)
}

func TestFailRetrySchedule(t *testing.T) {
	storetest.TestFailRetrySchedule(t, memoryStore)
}

func TestLeaseRoundRobin(t *testing.T) {
	storetest.TestLeaseRoundRobin(t, memoryStore)
}

func TestLeaseFencingTokenGrows(t *testing.T) {
	storetest.TestLeaseFencingTokenGrows(t, memoryStore)
}

func TestLeaseSkipsFutureRetries(t *testing.T) {
	storetest.TestLeaseSkipsFutureRetries(t, memoryStore)
}

func TestLeaseExpiryConsumesAttempts(t *testing.T) {
	storetest.TestLeaseExpiryConsumesAttempts(t, memoryStore)
}

func TestFireIsolatesPipelines(t *testing.T) {
	storetest.TestFireIsolatesPipelines(t, memoryStore)
}

func TestFireWithoutConsumersSettles(t *testing.T) {
	storetest.TestFireWithoutConsumersSettles(t, memoryStore)
}

func TestReplayResetsTimings(t *testing.T) {
	storetest.TestReplayResetsTimings(t, memoryStore)
}

func TestDeliveryTimings(t *testing.T) {
	storetest.TestDeliveryTimings(t, memoryStore)
}

func TestFailingUnitsRollBackAlone(t *testing.T) {
	storetest.TestFailingUnitsRollBackAlone(t, memoryStore)
}

func TestRepublishedItemCountsNothing(t *testing.T) {
	storetest.TestRepublishedItemCountsNothing(t, memoryStore)
}

func TestImportReopens(t *testing.T) {
	storetest.TestImportReopens(t, memoryStore)
}

func TestImportRevisionsAndChains(t *testing.T) {
	storetest.TestImportRevisionsAndChains(t, memoryStore)
}

func TestImportFollowsReviews(t *testing.T) {
	storetest.TestImportFollowsReviews(t, memoryStore)
}

func TestRetainKeepsSampledItems(t *testing.T) {
	storetest.TestRetainKeepsSampledItems(t, memoryStore)
}

func TestPurgeErasesReviewsAndDeadLetters(t *testing.T) {
	storetest.TestPurgeErasesReviewsAndDeadLetters(t, memoryStore)
}

func TestPurgeAcrossImports(t *testing.T) {
	storetest.TestPurgeAcrossImports(t, memoryStore)
}

func TestRunBudgetConcurrentLeases(t *testing.T) {
	storetest.TestRunBudgetConcurrentLeases(t, memoryStore)
}

func TestConcurrentDone(t *testing.T) {
	storetest.TestConcurrentDone(t, memoryStore)
}

func TestLeasePipelineSlots(t *testing.T) {
	storetest.TestLeasePipelineSlots(t, memoryStore)
}

func TestLeaseSlotsAndExclusions(t *testing.T) {
	storetest.TestLeaseSlotsAndExclusions(t, memoryStore)
}

func TestConcurrentReplaysTakeDistinctGenerations(t *testing.T) {
	storetest.TestConcurrentReplaysTakeDistinctGenerations(t, memoryStore)
}

func TestReplayRestartsTheRetryBudget(t *testing.T) {
	storetest.TestReplayRestartsTheRetryBudget(t, memoryStore)
}

func TestMetricAndEventFilters(t *testing.T) {
	storetest.TestMetricAndEventFilters(t, memoryStore)
}

func TestRollupFailpoint(t *testing.T) {
	storetest.TestRollupFailpoint(t, memoryStore)
}

func TestImportFollowsRollback(t *testing.T) {
	storetest.TestImportFollowsRollback(t, memoryStore)
}

func TestBudgetLeavesUserPauses(t *testing.T) {
	storetest.TestBudgetLeavesUserPauses(t, memoryStore)
}

func TestMetricTotalsSurviveRetention(t *testing.T) {
	storetest.TestMetricTotalsSurviveRetention(t, memoryStore)
}
