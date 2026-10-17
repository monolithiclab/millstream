//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	osexec "os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
	"github.com/monolithiclab/millstream/internal/store/postgres/pgtest"
	"github.com/monolithiclab/millstream/internal/store/sqlstore"
)

// The failover chaos suite (spec sections 5.9 and 16): two serve processes on one Postgres database, each with its
// embedded node, run the builtins fixture, with a pipeline importing one of its streams, fired through the active
// engine's API with token auth on. The active one is killed at every boundary of every unit (its failpoints, as the
// crash suite arms them) and at seeded random points, or loses its database session while it lives, or is cut off
// from the database by a proxy; the standby takes over and finishes the run, and the store must end exactly as the
// clean run's, with the crash suite's invariants. Each test reports how long the takeovers took: from the active
// engine's end (its process gone, or its session ended) to the standby's lock, and to its first dispatch.

// failoverLeaseTTL is the serve processes' lease: the dead engine's leases expire quickly on the new one.
const failoverLeaseTTL = "1s"

// failoverEngine is the environment file's engine key in the suite: the write connection checked every 500 ms and
// watched with 1 s keepalives, so a cut-off engine steps down, and its lock is released, within 2.5 s.
const failoverEngine = "engine: { lockCheck: 500ms, keepalive: 1s }\n"

// failoverToken is the deployment's auth.token: the suite runs with token auth on, the standby's API included.
const failoverToken = "failover-token-0123456789abcdef-0"

// settleTimeout bounds how long a failover test waits for the run to settle.
const settleTimeout = 2 * time.Minute

// failover is one test's deployment: a database, the directory both engines run in (their artifact store is
// shared, as a volume would be), and the environment file.
type failover struct {
	t       *testing.T
	db      string
	dir     string
	envFile string
	// serveArgs are more flags of the serve processes.
	serveArgs []string
}

func newFailover(t *testing.T) *failover {
	t.Helper()
	db := pgDB(t)
	dir, _ := pgDirs.Load(db)
	f := &failover{t: t, db: db, dir: dir.(string), envFile: filepath.Join(dir.(string), "millstream.yaml")}
	if err := os.WriteFile(f.envFile, []byte("store: { postgres: { dsn: { env: MILLSTREAM_DSN } } }\n"+
		"listen: 127.0.0.1:0\nauth: { token: { env: MILLSTREAM_TOKEN } }\n"+failoverEngine), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

// environ is the environment of a millstream process of the test: the test's own without MILLSTREAM_ variables,
// the DSN, the token (the engines' auth.token, the clients' bearer token), a user to record decisions under, and
// extra.
func (f *failover) environ(dsn string, extra ...string) []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "MILLSTREAM_") && !strings.HasPrefix(kv, "USER=") {
			env = append(env, kv)
		}
	}
	return append(env, append([]string{"MILLSTREAM_DSN=" + dsn, "MILLSTREAM_TOKEN=" + failoverToken, "USER=ops"},
		extra...)...)
}

// engineProc is a serve process.
type engineProc struct {
	name   string
	cmd    *osexec.Cmd
	log    *procLog
	url    string
	done   chan struct{} // closed once it exited
	end    time.Time     // when it ended: killed, or its last output (Wait returns later while its children hold its pipes); read after done
	mu     sync.Mutex    // guards killed
	killed time.Time
	code   int // its exit status, -1 for a signal; read after done
}

// startEngine starts serve on dsn with extra environment variables and waits until it listens.
func (f *failover) startEngine(name, dsn string, extra ...string) *engineProc {
	f.t.Helper()
	p := &engineProc{name: name, log: newProcLog(), done: make(chan struct{})}
	p.cmd = osexec.Command(binary, append([]string{"--env-file", f.envFile, "serve", "--lease-ttl", failoverLeaseTTL}, // #nosec G204 -- test binary
		f.serveArgs...)...)
	p.cmd.Env, p.cmd.Dir = f.environ(dsn, extra...), f.dir
	p.cmd.Stdout, p.cmd.Stderr = p.log, p.log
	p.cmd.WaitDelay = 10 * time.Second
	if err := p.cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	go func() {
		err := p.cmd.Wait()
		p.mu.Lock()
		p.end = p.killed
		p.mu.Unlock()
		if p.end.IsZero() {
			p.end = p.log.last()
		}
		p.code = exitCode(f.t, err)
		close(p.done)
	}()
	f.t.Cleanup(func() {
		if f.t.Failed() && p.alive() {
			_ = p.cmd.Process.Signal(syscall.SIGQUIT) // its goroutines, for the log below
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
			}
		}
		_ = p.cmd.Process.Kill()
		<-p.done
		if f.t.Failed() {
			f.t.Logf("engine %s:\n%s", p.name, lastLines(p.log.String(), 400))
		}
	})
	line := p.waitLine(f.t, "serving the UI and the API on ", time.Minute)
	_, url, _ := strings.Cut(line, "serving the UI and the API on ")
	p.url = strings.TrimSuffix(url, "/")
	return p
}

// kill kills the process with SIGKILL and waits for it; its end is the kill.
func (p *engineProc) kill() {
	p.mu.Lock()
	p.killed = time.Now()
	p.mu.Unlock()
	_ = p.cmd.Process.Kill()
	<-p.done
}

// alive reports whether the process still runs.
func (p *engineProc) alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// waitLine waits until the process logged a line containing text, and returns it; it fails the test at the
// timeout, or when the process exits first.
func (p *engineProc) waitLine(t *testing.T, text string, timeout time.Duration) string {
	t.Helper()
	line, ok := p.log.wait(func(l logLine) bool { return strings.Contains(l.text, text) }, timeout, p.done)
	if !ok {
		t.Fatalf("engine %s never logged %q (exited: %v)\n%s", p.name, text, !p.alive(), lastLines(p.log.String(), 20))
	}
	return line.text
}

// waitDone waits until the engine reported n deliveries done.
func (p *engineProc) waitDone(t *testing.T, n int) {
	t.Helper()
	done := 0
	if _, ok := p.log.wait(func(l logLine) bool {
		if strings.Contains(l.text, deliveryDone) {
			done++
		}
		return done >= n
	}, settleTimeout, p.done); !ok {
		t.Fatalf("engine %s reported %d deliveries done, want %d\n%s", p.name, done, n, lastLines(p.log.String(), 20))
	}
}

// stop ends the process with SIGTERM and waits for it.
func (p *engineProc) stop(t *testing.T) {
	t.Helper()
	if !p.alive() {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		t.Errorf("engine %s did not stop on SIGTERM", p.name)
		_ = p.cmd.Process.Kill()
		<-p.done
	}
	// An armed failpoint may still be reached while the engine stops (a projector pass): that exit is the matrix's.
	if p.code != 0 && p.code != failpoint.ExitCode {
		t.Errorf("engine %s exited with %d", p.name, p.code)
	}
}

// logTime returns the time field of a JSON log line.
func logTime(t *testing.T, line string) time.Time {
	t.Helper()
	var l struct{ Time time.Time }
	if err := json.Unmarshal([]byte(line), &l); err != nil {
		t.Fatalf("log line %q: %v", line, err)
	}
	return l.Time
}

// logField returns a string field of a JSON log line.
func logField(t *testing.T, line, key string) string {
	t.Helper()
	var l map[string]any
	if err := json.Unmarshal([]byte(line), &l); err != nil {
		t.Fatalf("log line %q: %v", line, err)
	}
	s, _ := l[key].(string)
	return s
}

// logLine is a line a process wrote.
type logLine struct{ text string }

// last returns when the process last wrote a line.
func (l *procLog) last() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastAt
}

// procLog collects a process's output, stdout and stderr together, by line, and wakes the waiters at each.
type procLog struct {
	mu      sync.Mutex
	all     bytes.Buffer
	partial []byte
	lines   []logLine
	lastAt  time.Time
	changed chan struct{}
}

func newProcLog() *procLog { return &procLog{changed: make(chan struct{})} }

func (l *procLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.all.Write(p)
	l.partial = append(l.partial, p...)
	added := false
	for {
		line, rest, ok := bytes.Cut(l.partial, []byte("\n"))
		if !ok {
			break
		}
		l.lines = append(l.lines, logLine{text: string(line)})
		l.lastAt = time.Now()
		l.partial, added = rest, true
	}
	if added {
		close(l.changed)
		l.changed = make(chan struct{})
	}
	return len(p), nil
}

func (l *procLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.all.String()
}

// count returns how many lines contain text.
func (l *procLog) count(text string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line.text, text) {
			n++
		}
	}
	return n
}

// wait returns the first line matching, waiting for it until the timeout or until stop is closed.
func (l *procLog) wait(match func(logLine) bool, timeout time.Duration, stop <-chan struct{}) (logLine, bool) {
	deadline := time.After(timeout)
	for i := 0; ; {
		l.mu.Lock()
		for ; i < len(l.lines); i++ {
			if match(l.lines[i]) {
				line := l.lines[i]
				l.mu.Unlock()
				return line, true
			}
		}
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-changed:
		case <-stop:
			stop = nil // read what it wrote before it ended, then give up
			l.mu.Lock()
			n := len(l.lines)
			l.mu.Unlock()
			if i >= n {
				return logLine{}, false
			}
		case <-deadline:
			return logLine{}, false
		}
	}
}

// client runs a millstream command against the API at url and returns its outcome.
func (f *failover) client(url string, args ...string) result {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.t.Context(), commandTimeout)
	defer cancel()
	cmd := osexec.CommandContext(ctx, binary, append([]string{"--env-file", f.envFile, "--server", url}, args...)...) // #nosec G204 -- test binary
	cmd.Env, cmd.Dir = f.environ(f.db), f.dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	return result{code: exitCode(f.t, err), stdout: stdout.String(), stderr: stderr.String()}
}

// onActive runs a command that changes state through whichever engine accepts it, trying them in turn until one
// does: the active one, or the standby once it took over. It fails the test after a minute.
func (f *failover) onActive(engines []*engineProc, args ...string) result {
	f.t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		var last result
		for _, e := range engines {
			if !e.alive() {
				continue
			}
			if last = f.client(e.url, args...); last.code == 0 {
				return last
			}
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("millstream %s: no engine accepted it: %s", strings.Join(args, " "), last)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// fireRun applies the builtins fixture and its importer, as the clean run has them, and fires the crash suite's run
// through the engines' API.
func (f *failover) fireRun(engines []*engineProc) {
	f.t.Helper()
	f.onActive(engines, "apply", fixtureBuiltins, fixtureImporter)
	f.onActive(engines, "trigger", pipelineName, "adhoc", "--request-id", crashRequest)
}

// waitSettled waits until the crash suite's run and every run it opened settled, reading the store read-only beside
// the engines.
func (f *failover) waitSettled() {
	f.t.Helper()
	ctx := f.t.Context()
	st, err := postgres.OpenReadOnly(ctx, f.db, postgres.Options{})
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	deadline := time.Now().Add(settleTimeout)
	for {
		run, err := st.Run(ctx, pipelineName, crashRun)
		if err == nil && !run.SettledAt.IsZero() {
			// The importer's run settles once its own work after the producer run's is done.
			open, err := st.Runs(ctx, store.RunFilter{Unsettled: true})
			if err == nil && len(open) == 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("the run did not settle within %s: %+v, %v", settleTimeout, run, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// takeover is how long a standby took to lead after the active engine ended, and to dispatch its first delivery.
type takeover struct {
	lead, dispatch time.Duration
}

// measureTakeover reads from by's log its takeover after end, when the active engine ended: when it next led, and
// its first dispatch in that term, waited for up to wait, zero when there is none by then (the run needed none from
// it).
func measureTakeover(t *testing.T, by *engineProc, end time.Time, wait time.Duration) takeover {
	t.Helper()
	after := func(text string, from time.Time) func(logLine) bool {
		return func(l logLine) bool { return strings.Contains(l.text, text) && !logTime(t, l.text).Before(from) }
	}
	lead, ok := by.log.wait(after(`"msg":"leading the deployment"`, end), time.Minute, by.done)
	if !ok {
		t.Fatalf("engine %s did not take over\n%s", by.name, lastLines(by.log.String(), 20))
	}
	led := logTime(t, lead.text)
	tk := takeover{lead: led.Sub(end)}
	if line, ok := by.log.wait(after(`"msg":"first dispatch"`, led), wait, by.done); ok {
		tk.dispatch = logTime(t, line.text).Sub(end)
	}
	return tk
}

// takeovers collects a test's takeover times for its report.
type takeovers struct {
	mu   sync.Mutex
	list []takeover
}

func (ts *takeovers) add(tk takeover) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.list = append(ts.list, tk)
}

// report logs the takeovers' lock and first dispatch times: minimum, median and maximum.
func (ts *takeovers) report(t *testing.T, what string) {
	t.Helper()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	t.Logf("%s takeovers: end to lock %s; end to first dispatch %s", what,
		durationStats(ts.list, func(tk takeover) time.Duration { return tk.lead }),
		durationStats(ts.list, func(tk takeover) time.Duration { return tk.dispatch }))
}

// durationStats formats the minimum, median and maximum of the positive durations pick returns.
func durationStats(tks []takeover, pick func(takeover) time.Duration) string {
	var ds []time.Duration
	for _, tk := range tks {
		if d := pick(tk); d > 0 {
			ds = append(ds, d)
		}
	}
	if len(ds) == 0 {
		return "none"
	}
	slices.Sort(ds)
	r := func(d time.Duration) time.Duration { return d.Round(time.Millisecond) }
	return fmt.Sprintf("min %s, median %s, max %s over %d", r(ds[0]), r(ds[len(ds)/2]), r(ds[len(ds)-1]), len(ds))
}

// failoverReached are the units whose failpoints the active engine must reach in the failover matrix: the run's,
// through serve's API and its embedded node. The others are the crash suite's commands' (TestCrashInCommands), or
// the projectors', which serve runs every 15 seconds, after the run has settled.
var failoverReached = []string{"applyRevision", "fire", "lease", "renew", "done", "fail", "expireLeases", "filter",
	"dedupe", "park"}

// TestFailoverAtFailpoints kills the active engine at each failpoint of each unit, as the crash suite does, with a
// standby waiting: the standby takes over and finishes the run, which must end as the clean run.
func TestFailoverAtFailpoints(t *testing.T) {
	t.Parallel()
	want := cleanBuiltinsRunOn(t, postgresBackend)
	var (
		mu  sync.Mutex
		hit = map[string]bool{}
		tks takeovers
	)
	t.Run("matrix", func(t *testing.T) {
		for _, unit := range sqlstore.Units {
			for _, phase := range phases {
				name := unit + "." + phase
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					f := newFailover(t)
					var extra []string
					if unit == "renew" {
						extra = []string{"MILLSTREAM_VAR_CRAWL_DELAY_MS=1500"}
					}
					a := f.startEngine("a", f.db, append(extra, failpointEnv(name)...)...)
					a.waitLine(t, `"msg":"leading the deployment"`, time.Minute)
					b := f.startEngine("b", f.db, extra...)
					f.fireRun([]*engineProc{a, b})
					f.waitSettled()
					died := !a.alive() && a.code == failpoint.ExitCode
					if died {
						tks.add(measureTakeover(t, b, a.end, 0))
					}
					a.stop(t)
					b.stop(t)
					mu.Lock()
					hit[name] = died
					mu.Unlock()
					wantSame(t, readSnapshot(t, f.db), want)
				})
			}
		}
	})
	var lines []string
	for _, name := range slices.Sorted(maps.Keys(hit)) {
		outcome := "not reached"
		if hit[name] {
			outcome = "hit, standby took over"
		}
		lines = append(lines, "  "+name+": "+outcome)
	}
	t.Logf("failover matrix (each recovered to the clean outcome unless reported above):\n%s", strings.Join(lines, "\n"))
	tks.report(t, "failpoint")
	if t.Failed() {
		return
	}
	for _, unit := range failoverReached {
		for _, phase := range phases {
			if !hit[unit+"."+phase] {
				t.Errorf("failpoint %s.%s was never hit: the failover suite does not cover it", unit, phase)
			}
		}
	}
}

// randomFailovers is how many seeded random points the suite kills the active engine at.
const randomFailovers = 8

// TestFailoverAtRandomPoints kills the active engine with SIGKILL at seeded random points of the run (a number of
// deliveries done, plus up to 5 ms), with a standby waiting, which finishes the run.
func TestFailoverAtRandomPoints(t *testing.T) {
	t.Parallel()
	want := cleanBuiltinsRunOn(t, postgresBackend)
	var tks takeovers
	t.Run("points", func(t *testing.T) {
		for i := range randomFailovers {
			rng := rand.New(rand.NewPCG(20261016, uint64(i))) // #nosec G404 -- reproducible test schedule
			after := 1 + rng.IntN(len(want.Deliveries)/2)
			delay := time.Duration(rng.Int64N(int64(5 * time.Millisecond)))
			t.Run(fmt.Sprintf("%d-done+%s", after, delay.Round(time.Microsecond)), func(t *testing.T) {
				t.Parallel()
				f := newFailover(t)
				a := f.startEngine("a", f.db)
				a.waitLine(t, `"msg":"leading the deployment"`, time.Minute)
				b := f.startEngine("b", f.db)
				b.waitLine(t, "waiting as the standby", time.Minute)
				go func() {
					done := 0
					if _, ok := a.log.wait(func(l logLine) bool {
						if strings.Contains(l.text, deliveryDone) {
							done++
						}
						return done >= after
					}, settleTimeout, a.done); ok {
						time.Sleep(delay)
						a.kill()
					}
				}()
				f.fireRun([]*engineProc{a, b})
				f.waitSettled()
				if !a.alive() {
					tks.add(measureTakeover(t, b, a.end, 0))
				} else {
					t.Logf("the run settled before its kill point")
				}
				a.stop(t)
				b.stop(t)
				wantSame(t, readSnapshot(t, f.db), want)
			})
		}
	})
	tks.report(t, "random kill")
}

// sessionPID returns the backend pid of the engine's write session, found by the engine id it carries.
func sessionPID(t *testing.T, db, engineID string) int {
	t.Helper()
	var pid int
	pgtest.Admin(t, func(ctx context.Context, c *pgx.Conn) error {
		return c.QueryRow(ctx, `SELECT pid FROM pg_stat_activity WHERE datname = $1 AND application_name LIKE $2`,
			pgtest.Database(t, db), "millstream-engine "+engineID+" %").Scan(&pid)
	})
	return pid
}

// wantNoWritesBetween checks that while the standby led, every lease went to its node: the engine that lost its
// session wrote nothing until it led again.
func wantNoWritesBetween(t *testing.T, db string, loser, winner *engineProc) {
	t.Helper()
	st := openStore(t, db)
	defer func() { _ = st.Close() }()
	q, ok := st.(rowQuerier)
	if !ok {
		t.Fatalf("%T has no QueryRow", st)
	}
	owner := func(p *engineProc) string { return fmt.Sprintf(`%%"owner":"embedded-%d"%%`, p.cmd.Process.Pid) }
	var n, term int
	if err := q.QueryRow(t.Context(), `SELECT COUNT(*) FROM events WHERE kind = ?1 AND data LIKE ?2`,
		store.EventDeliveryLeased, owner(winner)).Scan(&term); err != nil {
		t.Fatal(err)
	}
	if term == 0 {
		t.Fatal("the standby leased nothing during its term")
	}
	if err := q.QueryRow(t.Context(), `SELECT COUNT(*) FROM events WHERE kind = ?1 AND data LIKE ?2
		AND seq BETWEEN (SELECT MIN(seq) FROM events WHERE kind = ?1 AND data LIKE ?3)
			AND (SELECT MAX(seq) FROM events WHERE kind = ?1 AND data LIKE ?3)`,
		store.EventDeliveryLeased, owner(loser), owner(winner)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("the engine that lost its session leased %d deliveries during the standby's term", n)
	}
}

// TestFailoverOnSessionLoss ends the active engine's database session while its process lives
// (pg_terminate_backend): it steps down at once, writes nothing more, and stands by again; the standby takes over.
// Then the new active engine is killed, and the first, standing by, takes over again and finishes the run.
func TestFailoverOnSessionLoss(t *testing.T) {
	t.Parallel()
	want := cleanBuiltinsRunOn(t, postgresBackend)
	f := newFailover(t)
	slow := "MILLSTREAM_VAR_CRAWL_DELAY_MS=3000"
	a := f.startEngine("a", f.db, slow)
	leading := a.waitLine(t, `"msg":"leading the deployment"`, time.Minute)
	b := f.startEngine("b", f.db, slow)
	b.waitLine(t, "waiting as the standby", time.Minute)
	// Token auth on both engines: the standby refuses a request without the token, and signs the UI in.
	for _, e := range []*engineProc{a, b} {
		if code, _ := apiCall(t, e.url, http.MethodGet, "/api/v1/pipelines", "", nil); code != http.StatusUnauthorized {
			t.Fatalf("engine %s without the token: %d", e.name, code)
		}
	}
	session := signIn(t, b.url)
	f.fireRun([]*engineProc{a})
	a.waitLine(t, `"msg":"first dispatch"`, time.Minute)

	cut := time.Now()
	pid := sessionPID(t, f.db, logField(t, leading, "engine"))
	pgtest.Admin(t, func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid)
		return err
	})
	down := a.waitLine(t, "stepping down", 30*time.Second)
	if !a.alive() {
		t.Fatalf("the engine that lost its session exited (%d) instead of standing by", a.code)
	}
	tk := measureTakeover(t, b, cut, 30*time.Second)
	// The session the standby signed in lives on in its term as the active engine.
	if code, body := apiCall(t, b.url, http.MethodGet, "/api/v1/session", "", session); code != http.StatusOK {
		t.Fatalf("the standby's session once it leads: %d %s", code, body)
	}
	t.Logf("session ended: stepped down after %s; standby took the lock after %s, dispatched after %s",
		logTime(t, down).Sub(cut).Round(time.Millisecond), tk.lead.Round(time.Millisecond),
		tk.dispatch.Round(time.Millisecond))
	// The engine that stepped down waits for the lock again, its standby API up.
	if a.log.count("waiting as the standby") == 0 {
		a.waitLine(t, "waiting as the standby", 30*time.Second)
	}

	b.waitDone(t, 5) // the standby runs part of the run before it is killed in turn
	b.kill()
	back := measureTakeover(t, a, b.end, 30*time.Second)
	t.Logf("the first engine took over again after %s, dispatched after %s", back.lead.Round(time.Millisecond),
		back.dispatch.Round(time.Millisecond))
	f.waitSettled()
	a.stop(t)
	wantNoWritesBetween(t, f.db, a, b)
	wantSame(t, readSnapshot(t, f.db), want)
	if n := a.log.count(`"msg":"leading the deployment"`); n != 2 {
		t.Errorf("the first engine led %d times, want 2", n)
	}
}

// apiCall makes one request of an engine's API with the given headers and returns the status and body.
func apiCall(t *testing.T, base, method, path, body string, header map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// signIn signs the UI in on an engine and returns the session cookie's header.
func signIn(t *testing.T, base string) map[string]string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/api/v1/session",
		strings.NewReader(`{"token":"`+failoverToken+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", base)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK || len(resp.Cookies()) != 1 {
		t.Fatalf("sign in on %s: %d %v", base, resp.StatusCode, resp.Cookies())
	}
	c := resp.Cookies()[0]
	return map[string]string{"Cookie": c.Name + "=" + c.Value}
}

// cutProxy is a TCP proxy to Postgres that can cut the network: cut, it forwards nothing more in either direction
// while keeping every connection open, as a partition would, and new connections hang the same way until it heals.
// Its connections from before a cut stay dead.
type cutProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	cut    bool
	healed chan struct{} // closed when a cut ends
	closed chan struct{} // closed when the test ends
	conns  []net.Conn
}

func newCutProxy(t *testing.T, target string) *cutProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &cutProxy{ln: ln, target: target, healed: make(chan struct{}), closed: make(chan struct{})}
	close(p.healed)
	t.Cleanup(func() {
		close(p.closed)
		_ = ln.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.conns {
			_ = c.Close()
		}
	})
	go p.serve()
	return p
}

func (p *cutProxy) addr() string { return p.ln.Addr().String() }

func (p *cutProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.forward(c)
	}
}

func (p *cutProxy) forward(c net.Conn) {
	p.mu.Lock()
	p.conns = append(p.conns, c)
	healed := p.healed
	p.mu.Unlock()
	select { // a connection made while cut hangs until the network heals
	case <-healed:
	case <-p.closed:
		return
	}
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, up)
	gen := p.healed
	p.mu.Unlock()
	go p.copy(up, c, gen)
	p.copy(c, up, gen)
}

// copy forwards src to dst until the network is cut after gen, then forwards nothing, ever.
func (p *cutProxy) copy(dst, src net.Conn, gen chan struct{}) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		p.mu.Lock()
		dead := p.healed != gen || p.cut
		p.mu.Unlock()
		if dead {
			<-p.closed // a partition: the bytes vanish and the connection stays up
			return
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				return
			}
			_ = dst.(*net.TCPConn).CloseWrite()
			return
		}
	}
}

// partition cuts the network.
func (p *cutProxy) partition() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut, p.healed = true, make(chan struct{})
}

// heal restores the network for new connections.
func (p *cutProxy) heal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = false
	close(p.healed)
}

// TestFailoverOnPartition cuts the active engine off from Postgres with both engines alive: the active one steps
// down within its watch window, Postgres ends its silent session and releases the lock within the same window, and
// the standby takes over. Healed, the first engine stands by again, takes over when the second is killed, and the
// run finishes as the clean one.
func TestFailoverOnPartition(t *testing.T) {
	t.Parallel()
	want := cleanBuiltinsRunOn(t, postgresBackend)
	f := newFailover(t)
	cfg, err := pgx.ParseConfig(f.db)
	if err != nil {
		t.Fatal(err)
	}
	proxy := newCutProxy(t, fmt.Sprintf("%s:%d", cfg.Host, cfg.Port))
	viaProxy := strings.Replace(f.db, fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), proxy.addr(), 1)
	slow := "MILLSTREAM_VAR_CRAWL_DELAY_MS=3000"
	a := f.startEngine("a", viaProxy, slow)
	a.waitLine(t, `"msg":"leading the deployment"`, time.Minute)
	b := f.startEngine("b", f.db, slow)
	b.waitLine(t, "waiting as the standby", time.Minute)
	f.fireRun([]*engineProc{a})
	a.waitLine(t, `"msg":"first dispatch"`, time.Minute)

	cut := time.Now()
	proxy.partition()
	// Watched every 500 ms with 1 s keepalives: a ping unanswered for 2 s, or a batch for 3 s, loses the session;
	// Postgres ends a session silent for 2.5 s.
	const window = 500*time.Millisecond + 5*time.Second
	down := a.waitLine(t, "stepping down", 30*time.Second)
	stepDown := logTime(t, down).Sub(cut)
	tk := measureTakeover(t, b, cut, 30*time.Second)
	t.Logf("partition: stepped down after %s; standby took the lock after %s, dispatched after %s",
		stepDown.Round(time.Millisecond), tk.lead.Round(time.Millisecond), tk.dispatch.Round(time.Millisecond))
	if stepDown > window+time.Second {
		t.Errorf("the cut-off engine stepped down after %s, beyond its window of %s", stepDown, window)
	}
	if tk.lead > window+time.Second {
		t.Errorf("the standby took the lock %s after the cut, beyond the window of %s", tk.lead, window)
	}
	if !a.alive() {
		t.Fatalf("the cut-off engine exited (%d) instead of standing by", a.code)
	}

	proxy.heal()
	a.waitLine(t, "waiting as the standby", time.Minute)
	b.waitDone(t, 5)
	b.kill()
	measureTakeover(t, a, b.end, 0)
	f.waitSettled()
	a.stop(t)
	wantNoWritesBetween(t, f.db, a, b)
	wantSame(t, readSnapshot(t, f.db), want)
}
