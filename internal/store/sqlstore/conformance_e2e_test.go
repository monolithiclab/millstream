//go:build e2e

package sqlstore_test

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// The conformance tests only the e2e build runs: see conformance_test.go.

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
