package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/failpoint"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

var phases = []string{"beforeCommit", "afterCommit"}

// failpointEnv arms a failpoint in a millstream process.
func failpointEnv(name string) []string { return []string{failpoint.EnvVar + "=" + name} }

// reachedByRun are the units "run --once" on the builtins fixture must reach, so their failpoints must be hit; it
// runs the projectors (rollup, recordInsights) once its runs settled. The others are reached through their own
// commands (TestCrashInCommands), or, for skipFirings, only by a cron scheduler that missed a firing, which a
// one-shot run never starts.
var reachedByRun = []string{"applyRevision", "fire", "lease", "renew", "done", "fail", "expireLeases", "filter",
	"dedupe", "rollup", "recordInsights"}

// slowCrawl makes the crawl hold its lease for several heartbeats, so the renew unit is reached for sure.
var slowCrawl = []string{"--var", "crawlDelayMs=1500"}

// TestCrashAtFailpoints runs the builtins fixture with each failpoint of each atomic unit armed, then reruns it
// without on the same database: whether the process died at the failpoint or never reached it, the rerun must end in
// exactly the clean run's state (no item lost, none published twice, every run settled, the same dead letters).
func TestCrashAtFailpoints(t *testing.T) {
	t.Parallel()
	want := cleanBuiltinsRun(t)
	var (
		mu  sync.Mutex
		hit = map[string]bool{}
	)
	t.Run("matrix", func(t *testing.T) {
		for _, unit := range sqlite.Units {
			for _, phase := range phases {
				name := unit + "." + phase
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					db := dbPath(t)
					var flags []string
					if unit == "renew" {
						flags = slowCrawl
					}
					r := millstream(t, db, failpointEnv(name), runOnce(fixtureBuiltins, flags...)...)
					switch {
					case r.code == failpoint.ExitCode:
					case r.code == 1 && strings.Contains(r.stdout, string(store.RunCompletedWithErrors)):
					default:
						t.Fatalf("with %s armed: %s", name, r)
					}
					mu.Lock()
					hit[name] = r.code == failpoint.ExitCode
					mu.Unlock()
					mustExit(t, 1, db, nil, runOnce(fixtureBuiltins)...)
					wantSame(t, readSnapshot(t, db), want)
				})
			}
		}
	})
	var lines []string
	for _, name := range slices.Sorted(maps.Keys(hit)) {
		outcome := "not reached"
		if hit[name] {
			outcome = "hit"
		}
		lines = append(lines, "  "+name+": "+outcome)
	}
	t.Logf("crash matrix (each recovered to the clean outcome unless reported above):\n%s", strings.Join(lines, "\n"))
	if t.Failed() {
		return
	}
	for _, unit := range reachedByRun {
		for _, phase := range phases {
			if !hit[unit+"."+phase] {
				t.Errorf("failpoint %s.%s was never hit: the crash suite does not cover it", unit, phase)
			}
		}
	}
}

// TestCrashInCommands covers the units a run does not reach on its own, through the commands that apply them, with
// each failpoint armed in that command's process: the command dies, is run again, and the end state is the one the
// commands reach without a crash.
func TestCrashInCommands(t *testing.T) {
	t.Parallel()
	for _, phase := range phases {
		t.Run("cancel."+phase, func(t *testing.T) {
			t.Parallel()
			want := cleanRun(t)
			db := dbPath(t)
			mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
			// A second run, fired with no engine running: its delivery stays pending until cancelled.
			const extra = "run_extra_adhoc"
			mustExit(t, 0, db, nil, "trigger", pipelineName, "adhoc", "--request-id", "extra")
			cancel := []string{"cancel", pipelineName, extra}
			mustExit(t, failpoint.ExitCode, db, failpointEnv("cancel."+phase), cancel...)
			mustExit(t, 0, db, nil, cancel...) // cancelling a cancelled run is a no-op
			mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
			got := readSnapshot(t, db)
			if got.Runs[extra] != store.RunCancelled {
				t.Fatalf("run %s is %s, want cancelled", extra, got.Runs[extra])
			}
			wantSame(t, onlyRun(got, crashRun), want)
			cancelled := onlyRun(got, extra)
			for id, d := range cancelled.Deliveries {
				if d.State != store.DeliveryCancelled {
					t.Errorf("delivery %s of the cancelled run is %s", id, d.State)
				}
			}
			if len(cancelled.Items) != 1 {
				t.Errorf("the cancelled run has %d items, want its trigger item only", len(cancelled.Items))
			}
		})

		t.Run("replay."+phase, func(t *testing.T) {
			t.Parallel()
			want := replayedRun(t)
			db := dbPath(t)
			mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
			mustExit(t, 1, db, nil, runOnce(fixtureV2)...)
			mustExit(t, failpoint.ExitCode, db, failpointEnv("replay."+phase), replayArgs()...)
			r := mustExit(t, 0, db, nil, replayArgs()...)
			wantOut := map[string]string{"beforeCommit": "1 dead letter replayed\n", "afterCommit": "0 dead letters replayed\n"}
			if r.stdout != wantOut[phase] {
				t.Fatalf("replay again after a crash %s: %s", phase, r)
			}
			mustExit(t, 0, db, nil, runOnce(fixtureV2)...)
			wantSame(t, readSnapshot(t, db), want)
		})

		t.Run("startReplay."+phase, func(t *testing.T) {
			t.Parallel()
			want := replayedStepRun(t)
			db := dbPath(t)
			mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
			mustExit(t, failpoint.ExitCode, db, failpointEnv("startReplay."+phase), replayStepArgs()...)
			r := mustExit(t, 0, db, nil, replayStepArgs()...)
			wantOut := map[string]string{"beforeCommit": replayStepRun + "\n", "afterCommit": replayStepRun + " (already started)\n"}
			if r.stdout != wantOut[phase] {
				t.Fatalf("replay again after a crash %s: %s", phase, r)
			}
			mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
			wantSame(t, readSnapshot(t, db), want)
		})

		t.Run("discard."+phase, func(t *testing.T) {
			t.Parallel()
			want := cleanRun(t)
			db := dbPath(t)
			mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
			ids := slices.Collect(maps.Keys(want.DeadLetters))
			discard := append([]string{"dlq", "discard", "--pipeline", pipelineName}, ids...)
			mustExit(t, failpoint.ExitCode, db, failpointEnv("discard."+phase), discard...)
			if r := millstream(t, db, nil, discard...); phase == "beforeCommit" && r.code != 0 ||
				phase == "afterCommit" && (r.code != 1 || !strings.Contains(r.stderr, "not found")) {
				t.Fatalf("discard again after a crash %s: %s", phase, r)
			}
			mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
			discarded := want
			discarded.DeadLetters = map[string]deadLetter{}
			for id, dl := range want.DeadLetters {
				discarded.DeadLetters[id] = deadLetter{RunID: dl.RunID, State: store.DeadLetterDiscarded}
			}
			wantSame(t, readSnapshot(t, db), discarded)
		})

		t.Run("recordEval."+phase, func(t *testing.T) {
			t.Parallel()
			pkg := filepath.Join(t.TempDir(), "pkg")
			if err := os.CopyFS(pkg, os.DirFS(fixtureV1)); err != nil {
				t.Fatal(err)
			}
			action := filepath.Join(pkg, "actions", "score")
			if err := os.MkdirAll(filepath.Join(action, "evals"), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(action, "evals", "listing.yaml"), []byte(
				"input: { id: l1, city: Lyon, priceEur: 200000, surfaceM2: 50, condition: good }\n"+
					"assert: [\"output.score >= 0 && output.score <= 100\"]\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			db := dbPath(t)
			mustExit(t, failpoint.ExitCode, db, failpointEnv("recordEval."+phase), "eval", action)
			mustExit(t, 0, db, nil, "eval", action)
			st, err := sqlite.Open(db, sqlite.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = st.Close() }()
			// Committed before the crash, the first scores stay; rolled back, only the rerun's are there.
			want := map[string]int{"beforeCommit": 1, "afterCommit": 2}[phase]
			if got, err := st.EvalResults(t.Context(), "m1-fixture/score"); err != nil || len(got) != want {
				t.Fatalf("eval results after a crash %s: %+v, %v", phase, got, err)
			}
		})

		t.Run("setPaused."+phase, func(t *testing.T) {
			t.Parallel()
			want := cleanRun(t)
			db := dbPath(t)
			// Applying the package needs a run; the pipeline exists from then on.
			mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
			mustExit(t, failpoint.ExitCode, db, failpointEnv("setPaused."+phase), "pause", pipelineName)
			mustExit(t, 0, db, nil, "pause", pipelineName)
			if st := mustExit(t, 0, db, nil, "status"); !regexp.MustCompile(`(?m)^m1-fixture\s+paused\s`).
				MatchString(st.stdout) {
				t.Fatalf("status after pausing:\n%s", st.stdout)
			}
			mustExit(t, failpoint.ExitCode, db, failpointEnv("setPaused."+phase), "resume", pipelineName)
			mustExit(t, 0, db, nil, "resume", pipelineName)
			mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
			wantSame(t, readSnapshot(t, db), want)
		})
	}
}

// onlyRun keeps the records of one run, and the reference counts, database-wide, of the artifacts its items carry.
func onlyRun(s snapshot, runID string) snapshot {
	out := snapshot{
		Runs:      map[string]store.RunState{runID: s.Runs[runID]},
		Revisions: map[string]store.RevisionID{runID: s.Revisions[runID]},
		Items:     map[string]item{}, Deliveries: map[string]delivery{}, DeadLetters: map[string]deadLetter{},
		Refcounts: map[string]int64{},
	}
	for id, it := range s.Items {
		if it.RunID != runID {
			continue
		}
		out.Items[id] = it
		var refs []store.ArtifactRef
		_ = json.Unmarshal([]byte(it.Artifacts), &refs) // written by readSnapshot from the same type
		for _, r := range refs {
			out.Refcounts[r.SHA256] = s.Refcounts[r.SHA256]
		}
	}
	for id, d := range s.Deliveries {
		if d.RunID == runID {
			out.Deliveries[id] = d
		}
	}
	for id, dl := range s.DeadLetters {
		if dl.RunID == runID {
			out.DeadLetters[id] = dl
		}
	}
	return out
}

// randomKills is how many seeded random kill points the suite tries.
const randomKills = 20

// killMargin is how many of the last deliveries no random kill point waits for, so the run cannot finish first.
const killMargin = 10

// TestCrashAtRandomPoints kills the run with SIGKILL at seeded random points, then reruns it to completion: the
// outcome must be the clean one. A point is a number of deliveries done, drawn over the clean run's deliveries but
// the last killMargin, plus a random delay of up to 5 ms after that one reports: points spread over the run whatever
// the machine's speed, and every one lands while the process still runs. The failpoints cover the run's end.
func TestCrashAtRandomPoints(t *testing.T) {
	t.Parallel()
	want := cleanRun(t)
	for i := range randomKills {
		rng := rand.New(rand.NewPCG(20261016, uint64(i))) // #nosec G404 -- reproducible test schedule
		after := rng.IntN(len(want.Deliveries) - killMargin)
		delay := time.Duration(rng.Int64N(int64(5 * time.Millisecond)))
		t.Run(fmt.Sprintf("%d-done+%s", after, delay.Round(time.Microsecond)), func(t *testing.T) {
			t.Parallel()
			db := dbPath(t)
			ctx, cancel := context.WithTimeout(t.Context(), commandTimeout)
			defer cancel()
			cmd, _, stderr := command(ctx, db, nil, runOnce(fixtureV1)...)
			w := &doneWatcher{w: stderr, target: after, reached: make(chan struct{})}
			cmd.Stderr = w
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan struct{})
			go func() {
				select {
				case <-w.reached:
					time.Sleep(delay)
					_ = cmd.Process.Kill()
				case <-exited:
				}
			}()
			err := cmd.Wait()
			close(exited)
			if code := exitCode(t, err); code != -1 {
				t.Fatalf("the run ended before its kill point: exit %d\n%s", code, lastLines(stderr.String(), 8))
			}
			mustExit(t, 1, db, nil, runOnce(fixtureV1)...)
			wantSame(t, readSnapshot(t, db), want)
		})
	}
}

// doneWatcher passes a run's stderr through to w and closes reached once the engine has logged target deliveries
// done (at once for a target of 0). Only the command's copying goroutine writes to it.
type doneWatcher struct {
	w       io.Writer
	target  int
	done    int
	partial []byte
	reached chan struct{}
	closed  bool
}

func (d *doneWatcher) Write(p []byte) (int, error) {
	if d.closed {
		return d.w.Write(p)
	}
	d.partial = append(d.partial, p...)
	for {
		line, rest, ok := bytes.Cut(d.partial, []byte("\n"))
		if !ok {
			break
		}
		if bytes.Contains(line, []byte(`"msg":"delivery done"`)) {
			d.done++
		}
		d.partial = rest
	}
	if d.done >= d.target {
		d.closed = true
		close(d.reached)
	}
	return d.w.Write(p)
}
