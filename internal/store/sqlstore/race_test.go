//go:build race

package sqlstore_test

// raceEnabled: under the race detector (make test-e2e) the tests keep every processor, so that the concurrent units
// of the conformance suite really run in parallel.
const raceEnabled = true
