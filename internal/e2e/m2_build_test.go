//go:build e2e

package e2e

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

func init() { extraBuilds = append(extraBuilds, buildM2) }

// The M2 fixture's package directories and harness settings, built by TestMain: fixtureM2 is examples/m2-fixture as
// committed (without its recordings and test files); fixtureM2NoDedupe is the same package without its dedupe step,
// score reading the crawl's stream directly; fixtureM2Lessons has the second version of the score step's lessons.
// m2Recordings are the committed recordings; m2CrashRecordings lack the one of crashListing with the committed
// lessons, so that delivery fails on the agent path. envFile lets agents run unsandboxed and prices the fake model.
var (
	fixtureM2         string
	fixtureM2NoDedupe string
	fixtureM2Lessons  string
	m2Recordings      string
	m2CrashRecordings string
	envFile           string
)

// m2Actions are the packages the M2 fixture's exec actions build from: its own crawl, the M1 fixture's others.
var m2Actions = map[string]string{
	"crawl":       "./examples/m2-fixture/actions/crawl",
	"rentability": "./examples/m1-fixture/actions/rentability",
	"report":      "./examples/m1-fixture/actions/report",
}

// buildM2 copies the M2 fixture into dir/m2 with its exec actions compiled, derives its variants, the crash suite's
// recordings and the environment file.
func buildM2(root, dir string) error {
	src := filepath.Join(root, "examples", "m2-fixture")
	fixtureM2 = filepath.Join(dir, "m2")
	if err := os.CopyFS(fixtureM2, os.DirFS(src)); err != nil {
		return err
	}
	for _, p := range m2NotPackaged {
		if err := os.RemoveAll(filepath.Join(fixtureM2, p)); err != nil {
			return err
		}
	}
	platform := runtime.GOOS + "-" + runtime.GOARCH
	for name, pkg := range m2Actions {
		if err := goBuild(root, filepath.Join(fixtureM2, "actions", name, "bin", platform, name), pkg); err != nil {
			return err
		}
	}
	fixtureM2NoDedupe, fixtureM2Lessons = filepath.Join(dir, "m2-nodedupe"), filepath.Join(dir, "m2-lessons")
	for _, d := range []string{fixtureM2NoDedupe, fixtureM2Lessons} {
		if err := os.CopyFS(d, os.DirFS(fixtureM2)); err != nil {
			return err
		}
	}
	lessons, err := os.ReadFile(filepath.Join(src, "testdata", "score-lessons-v2.yaml")) // #nosec G304 -- fixture
	if err != nil {
		return err
	}
	m2Recordings, m2CrashRecordings = filepath.Join(src, "recordings"), filepath.Join(dir, "m2-crash-recordings")
	envFile = filepath.Join(dir, "millstream.yaml")
	return errors.Join(
		rewrite(filepath.Join(fixtureM2NoDedupe, "pipeline.yaml"), [2]string{dedupeStep, ""},
			[2]string{"    in: listings.unique\n", "    in: listings.raw\n"}),
		os.WriteFile(filepath.Join(fixtureM2Lessons, "lessons", "score.yaml"), lessons, 0o600),
		os.WriteFile(envFile, []byte("agents: { allowUnsandboxed: true }\n"+m2Prices), 0o600),
		crashRecordings(m2Recordings, m2CrashRecordings),
	)
}

// m2NotPackaged are the M2 fixture's files outside the package the engine stores: the recordings and the
// recording test.
var m2NotPackaged = []string{"recordings", "testdata", "recordings_test.go"}

// m2Prices prices the M2 fixture's fake model, in an environment file.
var m2Prices = fmt.Sprintf("prices: { %s: { inPerMtok: %d, outPerMtok: %d } }\n", m2Model, m2InPerMtok, m2OutPerMtok)

// dedupeStep is the M2 fixture's dedupe step, as pipeline.yaml writes it.
const dedupeStep = `  dedupe:
    action: dedupe
    in: listings.raw
    out: listings.unique
    config: { key: item.id, window: 30d }

`

// crashRecordings copies the recordings in src to dst, but the one of crashListing with the committed lessons.
func crashRecordings(src, dst string) error {
	files, err := filepath.Glob(filepath.Join(src, "*.json"))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o750); err != nil {
		return err
	}
	left := 0
	for _, f := range files {
		data, err := os.ReadFile(f) // #nosec G304 -- fixture recordings
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte("Listing "+crashListing+":")) &&
			bytes.Contains(data, []byte(fmt.Sprintf("by %d points", lessonsV1Penalty))) {
			left++
			continue
		}
		if err := os.WriteFile(filepath.Join(dst, filepath.Base(f)), data, 0o600); err != nil {
			return err
		}
	}
	if left != 1 {
		return fmt.Errorf("%s: %d recordings of %s with the committed lessons, want 1", src, left, crashListing)
	}
	return nil
}
