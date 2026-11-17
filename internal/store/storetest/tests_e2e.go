//go:build e2e

package storetest

import "slices"

// e2eBuild says the build holds the *_e2e.go files.
const e2eBuild = true

var e2eTests = slices.Concat(
	area(Delivery,
		TestBudgetLeavesUserPauses, TestReplayRestartsTheRetryBudget, TestConcurrentReplaysTakeDistinctGenerations,
		TestLeaseSlotsAndExclusions, TestLeasePipelineSlots, TestConcurrentDone, TestRunBudgetConcurrentLeases,
		TestDoneCountsArtifactsPerItem, TestArtifactsTravelWithItems, TestDayBudgetRollsOverByUTCDay,
		TestBudgetSkipsPastHeldBackDeliveries, TestDedupeScopedByGeneration, TestDedupePassPublishesTheItemUnchanged,
		TestReplayDeadAgainCreatesNewDeadLetter, TestDiscardBeforeSettlement, TestReplayDead,
		TestDoneTerminalStepDropsOutputs, TestDoneFanOutAndSettle, TestDoneRouting, TestFailNonRetryableIsDeadAtOnce,
		TestFailTruncatesMessage, TestFailRetrySchedule, TestLeaseRoundRobin, TestLeaseFencingTokenGrows,
		TestLeaseSkipsFutureRetries, TestLeaseExpiryConsumesAttempts, TestFireIsolatesPipelines,
		TestFireWithoutConsumersSettles, TestFailingUnitsRollBackAlone, TestRepublishedItemCountsNothing),
	area(Review,
		TestImportFollowsRollback, TestImportReopens, TestImportRevisionsAndChains, TestImportFollowsReviews),
	area(Retention,
		TestMetricTotalsSurviveRetention, TestRollupFailpoint, TestMetricAndEventFilters, TestPurgeAcrossImports,
		TestRetainKeepsSampledItems, TestPurgeErasesReviewsAndDeadLetters, TestReplayResetsTimings,
		TestDeliveryTimings),
)
