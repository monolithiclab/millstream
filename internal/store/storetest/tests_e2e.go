//go:build e2e

package storetest

// e2eBuild says the build holds the *_e2e.go files.
const e2eBuild = true

var e2eTests = []Test{
	{"TestRetainKeepsSampledItems", TestRetainKeepsSampledItems},
	{"TestPurgeErasesReviewsAndDeadLetters", TestPurgeErasesReviewsAndDeadLetters},
	{"TestImportReopens", TestImportReopens},
	{"TestImportRevisionsAndChains", TestImportRevisionsAndChains},
	{"TestImportFollowsReviews", TestImportFollowsReviews},
	{"TestDoneCountsArtifactsPerItem", TestDoneCountsArtifactsPerItem},
	{"TestArtifactsTravelWithItems", TestArtifactsTravelWithItems},
	{"TestDayBudgetRollsOverByUTCDay", TestDayBudgetRollsOverByUTCDay},
	{"TestBudgetSkipsPastHeldBackDeliveries", TestBudgetSkipsPastHeldBackDeliveries},
	{"TestDedupeScopedByGeneration", TestDedupeScopedByGeneration},
	{"TestDedupePassPublishesTheItemUnchanged", TestDedupePassPublishesTheItemUnchanged},
	{"TestReplayDeadAgainCreatesNewDeadLetter", TestReplayDeadAgainCreatesNewDeadLetter},
	{"TestDiscardBeforeSettlement", TestDiscardBeforeSettlement},
	{"TestReplayDead", TestReplayDead},
	{"TestDoneTerminalStepDropsOutputs", TestDoneTerminalStepDropsOutputs},
	{"TestDoneFanOutAndSettle", TestDoneFanOutAndSettle},
	{"TestDoneRouting", TestDoneRouting},
	{"TestFailNonRetryableIsDeadAtOnce", TestFailNonRetryableIsDeadAtOnce},
	{"TestFailTruncatesMessage", TestFailTruncatesMessage},
	{"TestFailRetrySchedule", TestFailRetrySchedule},
	{"TestLeaseRoundRobin", TestLeaseRoundRobin},
	{"TestLeaseFencingTokenGrows", TestLeaseFencingTokenGrows},
	{"TestLeaseSkipsFutureRetries", TestLeaseSkipsFutureRetries},
	{"TestLeaseExpiryConsumesAttempts", TestLeaseExpiryConsumesAttempts},
	{"TestFireIsolatesPipelines", TestFireIsolatesPipelines},
	{"TestFireWithoutConsumersSettles", TestFireWithoutConsumersSettles},
	{"TestReplayResetsTimings", TestReplayResetsTimings},
	{"TestDeliveryTimings", TestDeliveryTimings},
	{"TestFailingUnitsRollBackAlone", TestFailingUnitsRollBackAlone},
	{"TestRepublishedItemCountsNothing", TestRepublishedItemCountsNothing},
}
