package proposals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
	"github.com/monolithiclab/millstream/internal/store"
)

// inProcess runs the fake action in this process in both builds: these tests are of the validation, not of the exec
// runtime, and the patched packages hold no binary.
var inProcess = map[string]exec.Launcher{pipeline.KindExec: exec.FuncLauncher{F: exectest.Serve}}

// inSandbox stands for the container sandbox: it runs the proposed actions as the base launchers do (in-process),
// recording the directories it was given.
type inSandbox struct{ dirs [][]string }

func (s *inSandbox) Launchers(_ context.Context, _ *pipeline.Package, dirs []string, base map[string]exec.Launcher,
) (map[string]exec.Launcher, func(), error) {
	s.dirs = append(s.dirs, dirs)
	return base, func() {}, nil
}

// fakeBacktest answers a fixed verdict and records what it was asked.
type fakeBacktest struct {
	passed bool
	err    error
	got    []BacktestRequest
}

func (f *fakeBacktest) Backtest(_ context.Context, req BacktestRequest) (BacktestResult, error) {
	f.got = append(f.got, req)
	return BacktestResult{Passed: f.passed, Report: json.RawMessage(`{"agreement":0.99}`)}, f.err
}

// fakeHoldout serves fixed holdout pairs and records the boundaries it was asked for.
type fakeHoldout struct {
	pairs []distill.Pair
	err   error
	got   []time.Time
}

func (f *fakeHoldout) Holdout(_ context.Context, _, step string, from time.Time) ([]distill.Pair, error) {
	if step != "score" {
		return nil, errors.New("no step " + step)
	}
	f.got = append(f.got, from)
	return f.pairs, f.err
}

// holdout returns pairs the fake action's rules mode answers: covered ones whose truth is its output but for the
// first wrong, then abstained ones; each item cost the agent a cent.
func holdout(covered, wrong, abstained int) *fakeHoldout {
	var ps []distill.Pair
	for i := range covered + abstained {
		in := fmt.Sprintf(`{"mode":"rules","score":%d}`, i)
		if i >= covered {
			in = `{"mode":"rules","abstain":true}`
		}
		truth := i
		if i < wrong {
			truth = -1
		}
		ps = append(ps, distill.Pair{Item: fmt.Sprintf("it_%d", i), Key: fmt.Sprintf("k%d", i), CostUSD: 0.01,
			Input:   json.RawMessage(in),
			Outputs: []distill.Output{{Key: fmt.Sprintf("k%d", i), Payload: json.RawMessage(fmt.Sprintf(`{"score":%d}`, truth))}}})
	}
	return &fakeHoldout{pairs: ps}
}

// fewCovered are thresholds a handful of holdout items can meet.
var fewCovered = distill.Thresholds{MinCovered: 4}

func checker(_ context.Context, pkg *pipeline.Package) []pipeline.Diagnostic {
	return pipeline.Check(pkg, pipeline.Resolver{}, func(string) (string, bool) { return "", false },
		pipeline.CheckOptions{})
}

// Validation runs check, the base revision's evals of each affected action before and after (real evals of the fake
// action), and the backtest: a distill whose code breaks a base case fails however its own new evals
// score, and a check error stops it before the evals and the backtest.
func TestValidate(t *testing.T) { testValidate(t, false) }

// TestValidateDistill runs TestValidate's distill cases, split off to keep each test within its time budget.
func TestValidateDistill(t *testing.T) { testValidate(t, true) }

// testValidate runs the cases of distill proposals, or of the other kinds.
func testValidate(t *testing.T, distillCases bool) {
	t.Helper()
	base := basePackage(t)
	newEval := newFile("actions/coded/evals/mine.yaml", "config: { mode: rules }", "input: {}", "assert: [output.score == 1]")
	tests := []struct {
		name     string
		kind     store.ProposalKind
		patch    string
		backtest *fakeBacktest
		passed   bool
		check    func(t *testing.T, r Report)
		holdout  *fakeHoldout
	}{
		{"distill", store.ProposalDistill, distillPatch("coded", newEval), &fakeBacktest{passed: true}, true,
			func(t *testing.T, r Report) {
				d := r.Distill
				if len(r.Evals) != 1 || r.Evals[0].After != "./actions/coded" || len(r.Evals[0].Cases) != 1 ||
					!r.Evals[0].Passed || r.Evals[0].Cases[0].Before == nil || len(r.NewEvals) != 1 ||
					r.NewEvals[0].Cases[0].Before != nil || !r.NewEvals[0].Passed || r.Backtest.Skipped == "" ||
					d == nil || !d.Passed || d.Step != "score" || d.Action != "./actions/coded" ||
					d.Fallback != "./actions/scorer" || d.Holdout != 10 || d.Covered != 9 || d.Agreement != 1 ||
					fmt.Sprintf("%.2f", d.AgentCostSavedUSD) != "0.09" {
					t.Fatalf("report %+v, distill %+v", r, d)
				}
			}, holdout(9, 0, 1)},
		{"distill disagrees", store.ProposalDistill, distillPatch("coded"), &fakeBacktest{passed: true}, false,
			func(t *testing.T, r Report) {
				if d := r.Distill; d.Passed || d.Agreement != 0.9 || len(d.Disagreements) != 1 ||
					d.Disagreements[0] != "it_0" || d.Failures[0] != "agreement" || !r.Evals[0].Passed {
					t.Fatalf("distill %+v", d)
				}
			}, holdout(10, 1, 0)},
		{"distill without history", store.ProposalDistill, distillPatch("coded"), &fakeBacktest{passed: true}, false,
			func(t *testing.T, r Report) {
				if d := r.Distill; d.Passed || !strings.Contains(d.Error, "no history") {
					t.Fatalf("distill %+v", d)
				}
			}, &fakeHoldout{err: errors.New("no history")}},
		{"distill regresses", store.ProposalDistill, distillPatch("coded-fallback"), &fakeBacktest{passed: true}, false,
			func(t *testing.T, r Report) {
				c := r.Evals[0].Cases[0]
				if c.Case != "disagree" || !c.Before.Passed || c.After.Passed || r.Evals[0].Passed {
					t.Fatalf("cases %+v", r.Evals[0].Cases)
				}
			}, holdout(9, 0, 1)},
		{"backtest errs", store.ProposalConfig, configPatch, &fakeBacktest{err: errors.New("no sample")}, false,
			func(t *testing.T, r Report) {
				if !strings.Contains(r.Backtest.Skipped, "no sample") {
					t.Fatalf("backtest %+v", r.Backtest)
				}
			}, nil},
		{"no backtest runner", store.ProposalConfig, configPatch, nil, false, func(t *testing.T, r Report) {
			if r.Backtest.Skipped == "" {
				t.Fatalf("backtest %+v", r.Backtest)
			}
		}, nil},
		{"check fails", store.ProposalTopology, brokenPatch, &fakeBacktest{passed: true}, false, nil, nil},
		{"distill without a sandbox", store.ProposalDistill, distillPatch("coded", newEval), &fakeBacktest{passed: true}, false,
			func(t *testing.T, r Report) {
				if c := r.Evals[0].Cases[0]; !c.Before.Passed || !strings.Contains(c.After.Error, "container sandbox") ||
					!strings.Contains(r.NewEvals[0].Cases[0].After.Error, "container sandbox") ||
					!strings.Contains(r.Distill.Error, "container sandbox") {
					t.Fatalf("report %+v", r)
				}
			}, holdout(9, 0, 1)},
	}
	scratch := t.TempDir()
	for _, tt := range tests {
		if (tt.kind == store.ProposalDistill) != distillCases {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			p, err := Prepare(tt.kind, base, tt.patch)
			if err != nil {
				t.Fatal(err)
			}
			box := &inSandbox{}
			v := Validator{Check: checker, Evals: Evals{Launchers: inProcess, Sandbox: box}, Scratch: scratch,
				Distill: fewCovered}
			if tt.holdout != nil {
				v.Holdout = tt.holdout
			}
			if tt.name == "distill without a sandbox" {
				v.Evals.Sandbox = nil
			}
			if tt.backtest != nil {
				v.Backtest = tt.backtest
			}
			r, err := v.Validate(context.Background(), "listings", p)
			if err != nil {
				t.Fatal(err)
			}
			if r.Passed != tt.passed || r.BaseRevision != base.Hash || r.PatchSHA256 != p.SHA256 {
				t.Fatalf("report %+v, want passed %v", r, tt.passed)
			}
			if tt.name == "check fails" && (r.Check.Passed || r.Backtest.Skipped != "check failed" ||
				len(tt.backtest.got) != 0) {
				t.Fatalf("check fails: %+v", r)
			}
			if tt.holdout != nil && tt.holdout.err == nil && tt.name != "distill without a sandbox" &&
				(len(tt.holdout.got) != 1 || tt.holdout.got[0].Format(time.RFC3339) != "2026-10-01T00:00:00Z") {
				t.Fatalf("holdout asked %v", tt.holdout.got)
			}
			if tt.kind == store.ProposalDistill && len(tt.backtest.got) != 0 {
				t.Fatalf("a distill backtested: %+v", tt.backtest.got)
			}
			if tt.backtest != nil && tt.name != "check fails" && tt.kind != store.ProposalDistill && (len(tt.backtest.got) != 1 ||
				tt.backtest.got[0].BaseRevision != base.Hash || tt.backtest.got[0].Patch != tt.patch) {
				t.Fatalf("backtest asked %+v", tt.backtest.got)
			}
			if wantBox := tt.kind == store.ProposalDistill && v.Evals.Sandbox != nil; wantBox != (len(box.dirs) == 1) ||
				wantBox && !strings.HasPrefix(box.dirs[0][0], "actions/coded") {
				t.Fatalf("sandboxed %q", box.dirs)
			}
			if tt.check != nil {
				tt.check(t, r)
			}
		})
	}
}

// TestMain validates a distill once before the tests: the YAML decoder, the pipeline's check, the eval assertions'
// CEL environment and the in-process action are a process's set-up, which the first test would otherwise pay
// (scripts/test-times leaves TestMain out). A failure is left to the tests to report.
func TestMain(m *testing.M) {
	if dir, err := os.MkdirTemp("", "millstream-proposals-warm-"); err == nil {
		if base, err := pipeline.FromFiles("", baseFiles()); err == nil {
			if p, err := Prepare(store.ProposalDistill, base, distillPatch("coded")); err == nil {
				v := Validator{Check: checker, Evals: Evals{Launchers: inProcess, Sandbox: &inSandbox{}}, Scratch: dir}
				_, _ = v.Validate(context.Background(), "listings", p)
			}
		}
		_ = os.RemoveAll(dir)
	}
	os.Exit(m.Run())
}

// TestEvalsFallBack: after a distill, a base eval case the code abstains on is answered by the step's fallback, as
// the step would answer it.
func TestEvalsFallBack(t *testing.T) {
	files := baseFiles()
	files["actions/scorer/action.yaml"] = []byte(strings.Join(manifest("scorer-fallback"), "\n") + "\n")
	files["actions/scorer/evals/disagree.yaml"] = []byte("config: { mode: rules }\ninput: { abstain: true, score: 3 }\n" +
		"assert: [output.score == 3]\n")
	base, err := pipeline.FromFiles("", files)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Prepare(store.ProposalDistill, base, distillPatch("coded"))
	if err != nil {
		t.Fatal(err)
	}
	v := Validator{Check: checker, Evals: Evals{Launchers: inProcess, Sandbox: &inSandbox{}}, Scratch: t.TempDir(),
		Holdout: holdout(4, 0, 0), Distill: fewCovered}
	r, err := v.Validate(context.Background(), "listings", p)
	if err != nil {
		t.Fatal(err)
	}
	if c := r.Evals[0].Cases[0]; !r.Passed || !c.Before.Passed || !c.After.Passed || !c.After.Fallback {
		t.Fatalf("report %+v, cases %+v", r, r.Evals[0].Cases)
	}
}

// TestDistillSpec: a distill whose spec is missing or wrong, or whose step does not keep the agent as its fallback
// on abstain, fails its holdout measure before anything runs.
func TestDistillSpec(t *testing.T) {
	base := basePackage(t)
	spec := func(lines ...string) string { return newFile("actions/coded/distill.yaml", lines...) }
	good := []string{"step: score", "agent: ./actions/scorer", "holdoutFrom: 2026-10-01T00:00:00Z"}
	moved := edit("pipeline.yaml", 6, "  score:", "    action: ./actions/scorer", "    action: ./actions/coded",
		"    in: trigger.adhoc")
	manifestFile := newFile("actions/coded/action.yaml", manifest("coded")...)
	for name, c := range map[string]struct{ patch, want string }{
		"no spec":       {moved + manifestFile, "distill.yaml is missing"},
		"no fallback":   {moved + manifestFile + spec(good...), "must keep ./actions/scorer as its fallback"},
		"unknown step":  {moved + manifestFile + spec("step: nope", good[1], good[2]), "which the pipeline lacks"},
		"another agent": {moved + manifestFile + spec(good[0], "agent: ./actions/other", good[2]), "not the agent"},
		"bad spec":      {moved + manifestFile + spec("step: score"), "required"},
	} {
		p, err := Prepare(store.ProposalDistill, base, c.patch)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, _, err := distillSpec(p); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestUnreferencedCodeSandboxed: a patch adding an exec action no step names, with evals, still runs it only in the
// sandbox: without one its new evals fail unrun, never on the host's launcher; with one, the sandbox is given its
// directory, as it is given a directory a step names.
func TestUnreferencedCodeSandboxed(t *testing.T) {
	base := basePackage(t)
	// The pipeline change quotes the score step's action, which runs the same action: nothing names actions/evil.
	unreferenced := edit("pipeline.yaml", 6, "  score:", "    action: ./actions/scorer", `    action: "./actions/scorer"`,
		"    in: trigger.adhoc") + newFile("actions/evil/action.yaml", manifest("evil")...) +
		newFile("actions/evil/evals/run.yaml", "config: { mode: rules }", "input: {}", "assert: [true]") +
		newFile("actions/evil/distill.yaml", "step: score", "agent: ./actions/scorer", "holdoutFrom: 2026-10-01T00:00:00Z")
	for _, c := range []struct {
		name, patch, dir string
	}{{"unreferenced", unreferenced, "actions/evil/"}, {"referenced", distillPatch("coded"), "actions/coded/"}} {
		p, err := Prepare(store.ProposalDistill, base, c.patch)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		host := 0
		launchers := map[string]exec.Launcher{pipeline.KindExec: exec.FuncLauncher{F: func(ctx context.Context,
			args, env []string, in io.Reader, out, errw io.Writer,
		) int {
			host++
			return exectest.Serve(ctx, args, env, in, out, errw)
		}}}
		v := Validator{Check: checker, Evals: Evals{Launchers: launchers}, Scratch: t.TempDir()}
		r, err := v.Validate(context.Background(), "listings", p)
		if err != nil {
			t.Fatal(err)
		}
		if c.name == "unreferenced" && (len(r.NewEvals) != 1 || len(r.NewEvals[0].Cases) != 1 ||
			!strings.Contains(r.NewEvals[0].Cases[0].After.Error, "container sandbox")) {
			t.Fatalf("%s: new evals %+v", c.name, r.NewEvals)
		}
		// The base revision's scorer runs on the host before; nothing of the proposal does.
		if r.Passed || host > len(r.Evals) {
			t.Fatalf("%s: passed %v, the host ran %d processes, evals %+v", c.name, r.Passed, host, r.Evals)
		}
		box := &inSandbox{}
		v.Evals.Sandbox = box
		if _, err := v.Validate(context.Background(), "listings", p); err != nil {
			t.Fatal(err)
		}
		if len(box.dirs) != 1 || !slices.Equal(box.dirs[0], []string{c.dir}) {
			t.Fatalf("%s: sandboxed %q, want %s", c.name, box.dirs, c.dir)
		}
	}
}
