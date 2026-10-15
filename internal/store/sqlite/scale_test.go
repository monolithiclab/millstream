//go:build !e2e

package sqlite

// The default suite's random sequences: two short runs whose seeds each reach every kind of step of the model,
// including the revision change and the stale owner.
var propertySeeds = []uint64{2, 6}

const propertySteps = 40
