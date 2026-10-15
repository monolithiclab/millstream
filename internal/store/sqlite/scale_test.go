//go:build !e2e

package sqlite

// The default suite's scale: the smallest runs that still exercise each behaviour. The e2e build runs them at full
// scale (scale_e2e_test.go).

// propertySeeds are TestRandomUnitSequences' seeds: two short runs whose seeds each reach every kind of step of the
// model, including the revision change and the stale owner, within their propertySteps steps.
var propertySeeds = []uint64{238, 271}

const propertySteps = 20

// concurrentDones is how many Done calls TestConcurrentDone races: enough to share a batch.
const concurrentDones = 4

// offlineTables are the tables TestUnitsSurfaceStoreErrors takes offline: events, which every unit writes, and the
// tables of the delivery and the revision paths.
var offlineTables = []string{"events", "deliveries", "revisions"}
