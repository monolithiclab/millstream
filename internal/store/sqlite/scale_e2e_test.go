//go:build e2e

package sqlite

// The e2e suite's scale: eight long random sequences, many concurrent Done calls, every table offline.

var propertySeeds = []uint64{0, 1, 2, 3, 4, 5, 6, 7}

const propertySteps = 250

const concurrentDones = 16

var offlineTables = []string{"events", "deliveries", "items", "dead_letters", "runs", "revisions", "pipelines",
	"steps_state", "packages"}
