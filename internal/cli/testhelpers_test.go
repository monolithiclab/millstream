package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest/fakeexec"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres/pgtest"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
	"github.com/monolithiclab/millstream/internal/store/sqlite/sqlitetest"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// testToken is a token as long as auth.token and node.token must be (pipeline.MinTokenLength).
const testToken = "test-token-0123456789abcdef-0123" // #nosec G101 -- a test fixture

// testDir is TestMain's temporary directory, for what tests build once and share; testdata is the absolute path
// of the testdata directory.
var testDir, testdata string

func TestMain(m *testing.M) {
	// The engine runs unsandboxed agents as its own executable's agent subcommand: in the e2e build, which starts real
	// processes, this binary (the default build serves them in-process, see testLaunchers).
	if len(os.Args) > 1 && os.Args[1] == "agent" {
		os.Exit(Main(context.Background(), os.Args[1:], StdIO(), "test"))
	}
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	// Building the Kong parser, which every command run through Run does, allocates enough for the collector to cost
	// a fifth of it at the default target: the tests trade memory for time (the parse benchmark: 0.85 ms to 0.7 ms).
	debug.SetGCPercent(400)
	sqlitetest.Warm()
	dir, err := os.MkdirTemp("", "cli-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	defer fakeexec.Cleanup()
	defer pgtest.Stop() // the e2e build's Postgres container, if a test started one
	testDir = dir
	if testdata, err = filepath.Abs("testdata"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := prepareStates(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer testdir.Remove()
	return m.Run()
}

// runPackage copies testdata/<name> to a temporary directory, places the fake action at actions/fake/fakeaction (a
// stub in the default build, which runs it in-process, the built binary in the e2e build), and returns the copy's
// path.
func runPackage(t *testing.T, name string) string {
	t.Helper()
	dst := testdir.New(t)
	if err := copyPackage(dst, name); err != nil {
		t.Fatal(err)
	}
	return dst
}

// sharedPackage is testdata/<name> for the tests that do not modify it: in the default build the directory itself,
// whose fake action the in-process launcher never runs; in the e2e build a copy as runPackage makes it, with the
// fake action binary, made once per process.
func sharedPackage(t *testing.T, name string) string {
	t.Helper()
	if !e2eBuild {
		return fixture(name)
	}
	f, _ := sharedPackages.LoadOrStore(name, sync.OnceValues(func() (string, error) {
		dst := filepath.Join(testDir, "shared", name)
		return dst, copyPackage(dst, name)
	}))
	dst, err := f.(func() (string, error))()
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

// sharedPackages holds a func() (string, error) per sharedPackage name.
var sharedPackages sync.Map

// copyPackage copies testdata/<name> to dst with the fake action at actions/fake/fakeaction: the fixture's stub in
// the default build, the built binary in the e2e build.
func copyPackage(dst, name string) error {
	if err := copyTree(dst, name); err != nil || !e2eBuild {
		return err
	}
	return fakeexec.Place(filepath.Join(dst, "actions", "fake", "fakeaction"))
}

// copyTree copies testdata/<name> to dst.
func copyTree(dst, name string) error { return os.CopyFS(dst, os.DirFS(fixture(name))) }

// dbPath returns a fresh database path under a temporary directory that does not exist yet.
func dbPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(testdir.New(t), "state", "millstream.db")
}

// runCLICtx runs the CLI under ctx with env as the whole environment.
func runCLICtx(t *testing.T, ctx context.Context, env map[string]string, argv ...string) (code int, stdout,
	stderr string,
) {
	t.Helper()
	var out, errb bytes.Buffer
	code = Run(ctx, argv, IO{In: strings.NewReader(""), Out: &out, Err: &errb}, "dev", func(rt *Runtime) {
		rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
		rt.Environ = func() []string { return slices.Collect(maps.Keys(env)) }
		rt.Launchers = testLaunchers()
	})
	return code, out.String(), errb.String()
}

// mustRun runs the CLI and fails the test unless it exits with want.
func mustRun(t *testing.T, want int, argv ...string) (stdout, stderr string) {
	t.Helper()
	code, out, errOut := runCLICtx(t, context.Background(), nil, argv...)
	if code != want {
		t.Fatalf("millstream %s: code %d, want %d\nstdout:\n%s\nstderr:\n%s", strings.Join(argv, " "), code, want,
			out, errOut)
	}
	return out, errOut
}

// syncBuffer is a buffer safe for a command writing while the test reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// watchBuffer is a syncBuffer that closes Done once what was written satisfies cond: tests wait on a command's
// output with it instead of polling.
type watchBuffer struct {
	syncBuffer
	cond func(string) bool
	Done chan struct{}
	once sync.Once
}

func newWatchBuffer(cond func(string) bool) *watchBuffer {
	return &watchBuffer{cond: cond, Done: make(chan struct{})}
}

func (b *watchBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	if b.cond(b.buf.String()) {
		b.once.Do(func() { close(b.Done) })
	}
	return n, err
}

// state is a test's state database: the run fixture applied and run r1 settled, all of its score deliveries failed
// when scoreErrors. The default build fills an in-memory store through engine stubs, which the test's commands share
// (Runtime.Store); the e2e build copies a database run --once settled with the fake action, which each command opens
// with --db, as the binary does.
type state struct {
	t   *testing.T
	db  string
	pkg string      // the package applied: e2e build only
	st  store.Store // nil under the e2e tag: the commands open the copied database
}

// exec runs the CLI against the state under ctx with env as the whole environment.
func (s *state) exec(ctx context.Context, env map[string]string, out io.Writer, argv ...string) (code int,
	stderr string,
) {
	var errb bytes.Buffer
	code = Run(ctx, append([]string{"--db", dsn(s.db)}, argv...), IO{In: strings.NewReader(""), Out: out, Err: &errb},
		"dev", func(rt *Runtime) {
			rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
			rt.Launchers = testLaunchers()
			rt.Store = s.st
			rt.CurrentUser = noAccount
		})
	return code, errb.String()
}

// run runs the CLI against the state and returns its exit code and output.
func (s *state) run(argv ...string) (code int, stdout, stderr string) {
	var out bytes.Buffer
	code, stderr = s.exec(context.Background(), nil, &out, argv...)
	return code, out.String(), stderr
}

// must runs the CLI against the state and fails the test unless it exits with want.
func (s *state) must(want int, argv ...string) (stdout, stderr string) {
	s.t.Helper()
	code, out, errOut := s.run(argv...)
	if code != want {
		s.t.Fatalf("millstream %s: code %d, want %d\nstdout:\n%s\nstderr:\n%s", strings.Join(argv, " "), code, want,
			out, errOut)
	}
	return out, errOut
}

// newEmptyState is a state with nothing applied yet: in the default build an in-memory store, with a --db path that
// is never created; in the e2e build a --db path that does not exist yet, which the first command creates.
func newEmptyState(t *testing.T) *state {
	t.Helper()
	if e2eBuild {
		return &state{t: t, db: dbPath(t)}
	}
	return &state{t: t, db: unusedDB(), st: memoryStore(t)}
}

// memoryStore opens an in-memory store, closed when the test ends.
func memoryStore(t *testing.T) *sqlite.Store {
	t.Helper()
	st, err := sqlite.Open(sqlite.Memory, sqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// unusedDB is the --db path of the states whose commands use an in-memory store: nothing creates it.
// dsn is the store DSN of the SQLite database at path, for --db; empty without a path.
func dsn(path string) string {
	if path == "" {
		return ""
	}
	return "sqlite://" + path
}

func unusedDB() string { return filepath.Join(testDir, "unused", "millstream.db") }

// runner is a command as the parser builds it.
type runner interface{ Run(rt *Runtime) error }

// call runs cmd against the state without the parser, which costs more than most commands' work: a table of a
// command's refusals builds each case's command directly, and a test of the same command through Run covers its
// flags and exit codes. Fields the parser would default are the case's to set.
func (s *state) call(env map[string]string, cmd runner) (stdout, stderr string, err error) {
	var out, errb bytes.Buffer
	rt := &Runtime{Ctx: context.Background(), IO: IO{In: strings.NewReader(""), Out: &out, Err: &errb},
		Version: "dev", Env: func(k string) (string, bool) { v, ok := env[k]; return v, ok }, DB: dsn(s.db),
		Launchers: testLaunchers(), Store: s.st, CurrentUser: noAccount}
	err = cmd.Run(rt)
	return out.String(), errb.String(), err
}

// noAccount is a system that names no account: the tests decide who runs a command through $USER alone.
func noAccount() (string, error) { return "", errors.New("no account") }

// refusal is a case of a table of refusals: cmd fails with an error that holds want, a usage error (exit status 2)
// when usage.
type refusal struct {
	cmd   runner
	usage bool
	want  string
}

// checkRefusals runs each refusal against the state.
func (s *state) checkRefusals(t *testing.T, env map[string]string, refusals []refusal) {
	t.Helper()
	for _, r := range refusals {
		_, stderr, err := s.call(env, r.cmd)
		var usage usageError
		if err == nil || errors.As(err, &usage) != r.usage || !strings.Contains(err.Error()+"\n"+stderr, r.want) {
			t.Errorf("%+v: error %v (usage %v), stderr %q; want usage %v, %q", r.cmd, err, errors.As(err, &usage),
				stderr, r.usage, r.want)
		}
	}
}

// status is what millstream status prints, run without the parser: the tests read the state with it, and
// Test_triggerCommand runs status through Run.
func (s *state) status() string {
	s.t.Helper()
	out, _, err := s.call(nil, StatusCmd{Limit: 10})
	if err != nil {
		s.t.Fatalf("status: %v", err)
	}
	return out
}

// direct runs cmd, built as the parser would build it, without the parser, and returns what Run would: the exit
// code, stdout, and stderr with the error printed as Run prints it. The tables of a command's outputs use it for
// the cases whose flags another case covers through Run.
func direct(env map[string]string, cmd runner) (code int, stdout, stderr string) {
	stdout, stderr, err := (&state{}).call(env, cmd)
	switch {
	case err == nil:
		return exitOK, stdout, stderr
	case errors.Is(err, errFailed):
		return exitFail, stdout, stderr
	case errors.As(err, new(usageError)):
		return exitUsage, stdout, stderr + "millstream: " + err.Error() + "\n"
	default:
		return exitFail, stdout, stderr + "millstream: " + err.Error() + "\n"
	}
}

// viaServer returns a runner of commands built as the parser would build them, going through the API h serves:
// env, their whole environment, names the server (ServerEnv). It returns what a command prints, and its error.
func viaServer(h http.Handler, env map[string]string) func(runner) (string, error) {
	return func(cmd runner) (string, error) {
		var out, errb bytes.Buffer
		rt := &Runtime{Ctx: context.Background(), IO: IO{In: strings.NewReader(""), Out: &out, Err: &errb},
			Env: func(k string) (string, bool) { v, ok := env[k]; return v, ok }, DB: dsn(unusedDB()),
			HTTPClient: &http.Client{Transport: handlerTransport{h}}, CurrentUser: noAccount}
		err := cmd.Run(rt)
		return out.String(), err
	}
}
