//go:build e2e && !race

package e2e

// raceEnabled is true when the tests run with -race; see race_test.go.
const raceEnabled = false
