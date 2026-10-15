//go:build race

package e2e

// raceEnabled is true when the tests run with -race: the millstream binary is then built with it too, so a data race
// in the engine under crash and recovery fails the suite (a race-detecting binary exits with status 66).
const raceEnabled = true
