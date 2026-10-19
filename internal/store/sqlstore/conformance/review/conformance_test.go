package review_test

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// The conformance suite's share of this package (store/storetest): reviews, audits and fallbacks, imports, insights,
// evals, proposals and the API reads. One top-level test per function, so that test-times measures each;
// TestConformanceListed (internal/store/sqlstore) keeps the shares in step with storetest.Tests.

func TestEvalResults(t *testing.T) {
	storetest.TestEvalResults(t, memoryStore)
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

func TestImportCopiesAndRuns(t *testing.T) {
	storetest.TestImportCopiesAndRuns(t, memoryStore)
}

func TestImportPausedConsumerAndCancel(t *testing.T) {
	storetest.TestImportPausedConsumerAndCancel(t, memoryStore)
}

func TestImportProducerReplay(t *testing.T) {
	storetest.TestImportProducerReplay(t, memoryStore)
}

func TestImportBackfill(t *testing.T) {
	storetest.TestImportBackfill(t, memoryStore)
}

func TestAbstainAndFallback(t *testing.T) {
	storetest.TestAbstainAndFallback(t, memoryStore)
}

func TestAudits(t *testing.T) {
	storetest.TestAudits(t, memoryStore)
}

func TestAuditBudget(t *testing.T) {
	storetest.TestAuditBudget(t, memoryStore)
}

func TestFallbackEdges(t *testing.T) {
	storetest.TestFallbackEdges(t, memoryStore)
}

func TestAuditsSurfaceStoreErrors(t *testing.T) {
	storetest.TestAuditsSurfaceStoreErrors(t, memoryStore)
}

func TestProposalLifecycle(t *testing.T) {
	storetest.TestProposalLifecycle(t, memoryStore)
}

func TestProposalMovedBase(t *testing.T) {
	storetest.TestProposalMovedBase(t, memoryStore)
}

func TestProposalSuppression(t *testing.T) {
	storetest.TestProposalSuppression(t, memoryStore)
}

func TestProposalPublishing(t *testing.T) {
	storetest.TestProposalPublishing(t, memoryStore)
}

func TestProposalUnitsFailpoints(t *testing.T) {
	storetest.TestProposalUnitsFailpoints(t, memoryStore)
}
