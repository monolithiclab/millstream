//go:build e2e

// Package e2e runs the millstream binary on the M1 fixture (examples/m1-fixture) the way a user does, as the M1
// exit criterion: an end-to-end run with its designed dead letter and its replay after a fix, and the crash suite of
// spec section 16, which kills the process at both failpoints of every atomic unit and at seeded random points,
// reruns it on the same database and checks it reaches exactly the state of a run that never crashed.
//
// The whole package is behind the e2e build tag (make test-e2e): it builds binaries and runs processes. The M2 exit
// criterion runs the M2 fixture (examples/m2-fixture), whose score step is an agent replayed from committed
// recordings: a scored day, seven days replayed through a newly inserted dedupe, a day replayed after a lessons edit,
// and the crash matrix on the agent path. TestLiveScore reaches the real claude CLI on request only.
//
// TestMain builds the binary and the fixture's actions once, into a temporary directory: build output never lands
// in the repository. Every test uses its own database, so the tests run in parallel.
package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres/pgtest"
	"github.com/monolithiclab/millstream/internal/store/stores"
)

// The fixture's package directories, built by TestMain: v1 is examples/m1-fixture as committed; v2 is the same
// package with the rentability fix (strict: false), the revision a dead-letter replay migrates to; builtins is v1
// with a dedupe, a filter and an auto-approving review step between crawl and score, so the crash suite reaches the
// builtin units; review and review-timeout are v1 with a review step between score and rentability, the second with
// a short timeout.
var (
	binary               string
	fixtureV1            string
	fixtureV2            string
	fixtureBuiltins      string
	fixtureReview        string
	fixtureReviewTimeout string
	// fixtureImporter is a pipeline importing the builtins fixture's listings.crawled (spec section 4.12), run with it
	// so that the crash suite covers a Done that imports.
	fixtureImporter string
)

// extraBuilds are the builds of the fixtures behind the e2e build tag, registered by their files' init functions;
// TestMain runs them with the others, given the repository root and the build directory.
var extraBuilds []func(root, dir string) error

func TestMain(m *testing.M) { os.Exit(runTests(m)) }

func runTests(m *testing.M) int {
	defer pgtest.Stop()
	dir, err := os.MkdirTemp("", "millstream-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if err := build(dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	pgEnvFile = filepath.Join(dir, "postgres.yaml")
	if err := os.WriteFile(pgEnvFile, []byte("store: { postgres: { dsn: { env: MILLSTREAM_DSN } } }\n"), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}

// backend is a store the tests run the binary on: a database is a SQLite path, or a Postgres DSN.
type backend struct {
	name  string
	newDB func(t *testing.T) string
}

var (
	sqliteBackend   = backend{"sqlite", dbPath}
	postgresBackend = backend{"postgres", pgDB}
	// backends are the stores the crash suite runs on.
	backends = []backend{sqliteBackend, postgresBackend}
)

// pgEnvFile is the environment file the binary reads on a Postgres database: the store key, the DSN in
// MILLSTREAM_DSN.
var pgEnvFile string

// pgDirs maps each Postgres database of a test to the directory its commands run in, where the binary keeps what
// lives next to the state database (the artifacts).
var pgDirs sync.Map

// pgDB returns a new Postgres database for the test (skipping it without Docker), with a working directory.
func pgDB(t *testing.T) string {
	t.Helper()
	dsn := pgtest.DSN(t)
	pgDirs.Store(dsn, t.TempDir())
	return dsn
}

// isPostgres reports whether db is a Postgres database.
func isPostgres(db string) bool { return strings.HasPrefix(db, "postgres://") }

// openStore opens db as a store, for the checks that read it once the commands ended.
func openStore(t *testing.T, db string) store.Store {
	t.Helper()
	cfg := pipeline.StoreConfig{SQLitePath: db}
	if isPostgres(db) {
		cfg = pipeline.StoreConfig{PostgresDSN: &pipeline.SecretSource{Env: "MILLSTREAM_DSN"}}
	}
	st, err := stores.Open(t.Context(), cfg, db, func(string) (string, bool) { return db, true })
	if err != nil {
		t.Fatalf("open %s: %v", db, err)
	}
	return st
}

// repoRoot returns the repository root, two levels above this file.
func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("locate the e2e package source")
	}
	return filepath.Join(filepath.Dir(file), "..", ".."), nil
}

// build compiles the millstream binary and copies the fixture twice into dir, with each action compiled under
// bin/<os>-<arch>/ where the manifests' bare commands resolve.
func build(dir string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	binary = filepath.Join(dir, "millstream")
	var flags []string
	if raceEnabled {
		flags = []string{"-race"}
	}
	builds := append([]func(string, string) error{buildFixture, func(root, _ string) error {
		return goBuild(root, binary, "./cmd/millstream", flags...)
	}}, extraBuilds...)
	built := make(chan error, len(builds)) // the binary and the fixtures build at once
	for _, b := range builds {
		go func() { built <- b(root, dir) }()
	}
	var errs []error
	for range builds {
		errs = append(errs, <-built)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	fixtureBuiltins = filepath.Join(dir, "builtins")
	fixtureReview = filepath.Join(dir, "review")
	fixtureReviewTimeout = filepath.Join(dir, "review-timeout")
	fixtureImporter = filepath.Join(dir, "importer")
	if err := os.MkdirAll(fixtureImporter, 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(fixtureImporter, "pipeline.yaml"), []byte(importerPipeline), 0o600); err != nil {
		return err
	}
	for _, d := range []string{fixtureBuiltins, fixtureReview, fixtureReviewTimeout} {
		if err := os.CopyFS(d, os.DirFS(fixtureV1)); err != nil {
			return err
		}
	}
	return errors.Join(fixRentability(filepath.Join(fixtureV2, "pipeline.yaml")),
		insertBuiltins(filepath.Join(fixtureBuiltins, "pipeline.yaml")),
		insertReview(filepath.Join(fixtureReview, "pipeline.yaml"), reviewStep),
		insertReview(filepath.Join(fixtureReviewTimeout, "pipeline.yaml"), reviewTimeoutStep))
}

// buildFixture copies the fixture into dir/v1 with its actions compiled, and v1 into dir/v2.
func buildFixture(root, dir string) error {
	src := filepath.Join(root, "examples", "m1-fixture")
	fixtureV1, fixtureV2 = filepath.Join(dir, "v1"), filepath.Join(dir, "v2")
	if err := os.CopyFS(fixtureV1, os.DirFS(src)); err != nil {
		return err
	}
	actions, err := os.ReadDir(filepath.Join(src, "actions"))
	if err != nil {
		return err
	}
	platform := runtime.GOOS + "-" + runtime.GOARCH
	for _, a := range actions {
		out := filepath.Join(fixtureV1, "actions", a.Name(), "bin", platform, a.Name())
		if err := goBuild(root, out, "./examples/m1-fixture/actions/"+a.Name()); err != nil {
			return err
		}
	}
	return os.CopyFS(fixtureV2, os.DirFS(fixtureV1))
}

func goBuild(root, out, pkg string, flags ...string) error {
	args := append([]string{"build", "-ldflags=-s -w", "-o", out}, flags...)
	cmd := osexec.Command("go", append(args, pkg)...)
	cmd.Dir = root
	if b, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("go build %s: %w\n%s", pkg, err, b)
	}
	return nil
}

// rewrite replaces, in the file at path, each pair's first string, which must appear exactly once, with its second.
func rewrite(path string, pairs ...[2]string) error {
	data, err := os.ReadFile(path) // #nosec G304 -- a fixture copy
	if err != nil {
		return err
	}
	for _, r := range pairs {
		if bytes.Count(data, []byte(r[0])) != 1 {
			return fmt.Errorf("%s: want exactly one %q to rewrite", path, r[0])
		}
		data = bytes.Replace(data, []byte(r[0]), []byte(r[1]), 1)
	}
	return os.WriteFile(path, data, 0o600)
}

// fixRentability turns v1 into v2: rentability prices a listing without a rent estimate instead of failing it.
func fixRentability(path string) error {
	return rewrite(path, [2]string{"config: { strict: true }", "config: { strict: false }"})
}

// builtinSteps are the steps insertBuiltins puts between crawl and score: a dedupe on the listing id, which lets
// every listing through since ids are unique (a key that repeated would make which item passes depend on delivery
// order, and so differ after a crash), a filter dropping the listings in good condition, and a review approving
// every listing by rule, each one a review sample (the Park unit's auto-approval path).
const builtinSteps = `  unique:
    action: dedupe
    in: listings.crawled
    out: listings.unique
    config: { key: item.id, window: 30d }

  affordable:
    action: filter
    in: listings.unique
    out: listings.affordable
    config: { expr: "item.condition != 'good'" }

  vetted:
    action: review
    in: listings.affordable
    out: listings.raw
    config: { inbox: analysts, autoApprove: "true", sampleRate: 1 }

  score:
`

// importerPipeline imports the builtins fixture's crawled listings and keeps those not in good condition, with a
// builtin, so it needs no action binary.
const importerPipeline = `pipeline: m1-importer
streams:
  crawled: { pipeline: m1-fixture, stream: listings.crawled }
steps:
  keep: { action: filter, in: crawled, config: { expr: "item.condition != 'good'" } }
`

// reviewStep is the review step insertReview puts between score and rentability: listings scoring 85 or more are
// approved by rule, half of them sampled; the others wait in the analysts inbox.
const reviewStep = `  vetting:
    action: review
    in: listings.shortlist
    out: listings.vetted
    config:
      inbox: analysts
      editable: [/monthlyRentEur]
      reasons: [overpriced, stale]
      autoApprove: "item.score >= 85"
      sampleRate: 0.5
      display: "{{.item.city}}: {{.item.priceEur}} EUR, score {{.item.score}}"

  rentability:
`

// reviewTimeoutStep is reviewStep with a one-second timeout that rejects.
var reviewTimeoutStep = strings.Replace(reviewStep, "inbox: analysts\n", "inbox: analysts\n      timeout: 1s\n", 1)

// insertReview turns v1 into a review variant: score's shortlist goes through step, a review step writing the
// listings.vetted rentability reads.
func insertReview(path, step string) error {
	return rewrite(path, [2]string{"    in: listings.shortlist\n", "    in: listings.vetted\n"},
		[2]string{"  rentability:\n", step})
}

// insertBuiltins turns v1 into the builtins variant: crawl writes listings.crawled, which unique, affordable and
// vetted turn into the listings.raw score reads.
func insertBuiltins(path string) error {
	return rewrite(path, [2]string{"    out: listings.raw\n", "    out: listings.crawled\n"},
		[2]string{"  score:\n", builtinSteps})
}

// commandTimeout bounds every millstream invocation, so a hang fails the test instead of the whole suite.
const commandTimeout = 2 * time.Minute

// result is how a millstream invocation ended.
type result struct {
	code        int
	stdout      string
	stderr      string
	elapsedTime time.Duration
}

func (r result) String() string {
	return fmt.Sprintf("exit %d after %s\nstdout:\n%s\nstderr (last lines):\n%s", r.code,
		r.elapsedTime.Round(time.Millisecond), r.stdout, lastLines(r.stderr, 8))
}

// lastLines returns the last n lines of s.
func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], "\n")
}

// command returns a millstream invocation on db, with the environment of the test process minus every
// MILLSTREAM_ variable, plus env. On a Postgres database, the store key of pgEnvFile names it, the command runs in
// the database's directory, and --direct lets a command that changes state write from its own process, taking the
// leadership lock: the suite runs its commands while no engine runs.
func command(ctx context.Context, db string, env []string, args ...string) (*osexec.Cmd, *bytes.Buffer,
	*bytes.Buffer,
) {
	global := []string{"--db", db}
	if isPostgres(db) {
		global = []string{"--env-file", pgEnvFile, "--direct"}
	}
	cmd := osexec.CommandContext(ctx, binary, append(global, args...)...) // #nosec G204 -- test binary
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "MILLSTREAM_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, env...)
	if isPostgres(db) {
		cmd.Env = append(cmd.Env, "MILLSTREAM_DSN="+db)
		if dir, ok := pgDirs.Load(db); ok {
			cmd.Dir = dir.(string)
		}
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = 10 * time.Second
	return cmd, &stdout, &stderr
}

// exitCode returns the exit status of a finished command, or fails the test when it did not run or exit.
func exitCode(t *testing.T, err error) int {
	t.Helper()
	var exit *osexec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &exit) && exit.Exited():
		return exit.ExitCode()
	case errors.As(err, &exit):
		return -1 // killed by a signal
	default:
		t.Fatalf("millstream did not run: %v", err)
		return 0
	}
}

// millstream runs the binary on db to completion.
func millstream(t *testing.T, db string, env []string, args ...string) result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), commandTimeout)
	defer cancel()
	cmd, stdout, stderr := command(ctx, db, env, args...)
	start := time.Now()
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("millstream %s: timed out after %s\nstderr:\n%s", strings.Join(args, " "), commandTimeout,
			lastLines(stderr.String(), 40))
	}
	return result{code: exitCode(t, err), stdout: stdout.String(), stderr: stderr.String(),
		elapsedTime: time.Since(start)}
}

// mustExit runs millstream and fails the test unless it exits with code.
func mustExit(t *testing.T, code int, db string, env []string, args ...string) result {
	t.Helper()
	r := millstream(t, db, env, args...)
	if r.code != code {
		t.Fatalf("millstream %s: want exit %d, got %s", strings.Join(args, " "), code, r)
	}
	return r
}

// dbPath returns a fresh database path for the test.
func dbPath(t *testing.T) string { return filepath.Join(t.TempDir(), "state", "millstream.db") }
