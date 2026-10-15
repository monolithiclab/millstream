//go:build !race

package e2e

// raceEnabled is true when the tests run with -race; see race_test.go.
const raceEnabled = false
