package retention_test

import (
	"testing"

	"github.com/monolithiclab/millstream/internal/store/storetest"
)

// The conformance suite's share of this package (store/storetest): retention, purges and rollups, evidence, nodes, and
// units facing a missing table. One top-level test per function, so that test-times measures each;
// TestConformanceListed (internal/store/sqlstore) keeps the shares in step with storetest.Tests.

func TestUnitsSurfaceStoreErrors(t *testing.T) {
	storetest.TestUnitsSurfaceStoreErrors(t, memoryStore)
}

func TestNodes(t *testing.T) {
	storetest.TestNodes(t, memoryStore)
}

func TestRollupMath(t *testing.T) {
	storetest.TestRollupMath(t, memoryStore)
}

func TestDoneRefusesInvalidMetrics(t *testing.T) {
	storetest.TestDoneRefusesInvalidMetrics(t, memoryStore)
}

func TestRetainKeepsWhatUnfinishedWorkNeeds(t *testing.T) {
	storetest.TestRetainKeepsWhatUnfinishedWorkNeeds(t, memoryStore)
}

func TestRetainEventsTranscriptsRollups(t *testing.T) {
	storetest.TestRetainEventsTranscriptsRollups(t, memoryStore)
}

func TestPurgeResumesAfterCrash(t *testing.T) {
	storetest.TestPurgeResumesAfterCrash(t, memoryStore)
}

func TestEvidenceExports(t *testing.T) {
	storetest.TestEvidenceExports(t, memoryStore)
}

func TestEvidenceImports(t *testing.T) {
	storetest.TestEvidenceImports(t, memoryStore)
}
