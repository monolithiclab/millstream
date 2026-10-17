//go:build race

package engine

// raceEnabled: under the race detector (make test-e2e) the tests keep every processor, so that the engine's
// goroutines really run in parallel.
const raceEnabled = true
