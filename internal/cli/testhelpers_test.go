package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest/fakeexec"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

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
	dir, err := os.MkdirTemp("", "cli-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	defer fakeexec.Cleanup()
	testDir = dir
	if testdata, err = filepath.Abs("testdata"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}

// runPackage copies testdata/<name> to a temporary directory, places the fake action at actions/fake/fakeaction (a
// stub in the default build, which runs it in-process, the built binary in the e2e build), and returns the copy's
// path.
func runPackage(t *testing.T, name string) string {
	t.Helper()
	dst := t.TempDir()
	if err := copyPackage(dst, name); err != nil {
		t.Fatal(err)
	}
	return dst
}

// sharedPackage is a copy of testdata/<name> as runPackage makes it, made once per process for the tests that do not
// modify it.
func sharedPackage(t *testing.T, name string) string {
	t.Helper()
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

// copyPackage copies testdata/<name> to dst and places the fake action at actions/fake/fakeaction.
func copyPackage(dst, name string) error {
	src := fixture(name)
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		data, err := os.ReadFile(p) // #nosec G304 -- test fixtures
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
	if err != nil {
		return err
	}
	return fakeexec.Place(filepath.Join(dst, "actions", "fake", "fakeaction"))
}

// dbPath returns a fresh database path under a temporary directory that does not exist yet.
func dbPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "state", "millstream.db")
}

// runCLICtx runs the CLI under ctx with env as the whole environment.
func runCLICtx(t *testing.T, ctx context.Context, env map[string]string, argv ...string) (code int, stdout,
	stderr string,
) {
	t.Helper()
	var out, errb bytes.Buffer
	code = Run(ctx, argv, IO{In: strings.NewReader(""), Out: &out, Err: &errb}, "dev", func(rt *Runtime) {
		rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
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
	pkg string // the package applied: e2e build only
	st  *sqlite.Store
}

// exec runs the CLI against the state under ctx with env as the whole environment.
func (s *state) exec(ctx context.Context, env map[string]string, out io.Writer, argv ...string) (code int,
	stderr string,
) {
	var errb bytes.Buffer
	code = Run(ctx, append([]string{"--db", s.db}, argv...), IO{In: strings.NewReader(""), Out: out, Err: &errb},
		"dev", func(rt *Runtime) {
			rt.Env = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
			rt.Launchers = testLaunchers()
			rt.Store = s.st
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
func unusedDB() string { return filepath.Join(testDir, "unused", "millstream.db") }

// runner is a command as the parser builds it.
type runner interface{ Run(rt *Runtime) error }

// call runs cmd against the state without the parser, which costs more than most commands' work: a table of a
// command's refusals builds each case's command directly, and a test of the same command through Run covers its
// flags and exit codes. Fields the parser would default are the case's to set.
func (s *state) call(env map[string]string, cmd runner) (stdout, stderr string, err error) {
	var out, errb bytes.Buffer
	rt := &Runtime{Ctx: context.Background(), IO: IO{In: strings.NewReader(""), Out: &out, Err: &errb},
		Version: "dev", Env: func(k string) (string, bool) { v, ok := env[k]; return v, ok }, DB: s.db,
		Launchers: testLaunchers(), Store: s.st}
	err = cmd.Run(rt)
	return out.String(), errb.String(), err
}

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
