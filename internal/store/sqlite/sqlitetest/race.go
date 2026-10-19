//go:build race

package sqlitetest

// raceEnabled: under the race detector (make test-e2e) Main keeps every processor, so that the concurrent units of
// the conformance suite really run in parallel.
const raceEnabled = true
