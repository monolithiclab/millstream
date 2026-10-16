//go:build !e2e

package storetest

// The default suite's scale: the smallest runs that still exercise each behaviour. The e2e build runs them at full
// scale (scale_e2e_test.go).

// propertySeeds are TestRandomUnitSequences' seeds: one short run whose seed reaches every kind of step of the
// model, including the revision change and the stale owner, within its propertySteps steps.
var propertySeeds = []uint64{238}

const propertySteps = 20

// concurrentDones is how many Done calls TestConcurrentDone races: enough to share a batch.
const concurrentDones = 4

// offlineTables are the tables TestUnitsSurfaceStoreErrors takes offline: events, which every unit writes. Renaming
// a table expires every prepared statement and keeps the database out of the Memory pool, so each table costs a
// cold store.
var offlineTables = []string{"events"}

// fullScale says the e2e build runs every case of the tests that scale theirs down.
const fullScale = false

// concurrentLeases is how many Lease calls TestRunBudgetConcurrentLeases races for three reservations.
const concurrentLeases = 4
