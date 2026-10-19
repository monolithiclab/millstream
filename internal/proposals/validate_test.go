package proposals

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
	"github.com/monolithiclab/millstream/internal/store"
)

// inProcess runs the fake action in this process in both builds: these tests are of the validation, not of the exec
// runtime, and the patched packages hold no binary.
var inProcess = map[string]exec.Launcher{pipeline.KindExec: exec.FuncLauncher{F: exectest.Serve}}

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

func checker(_ context.Context, pkg *pipeline.Package) []pipeline.Diagnostic {
	return pipeline.Check(pkg, pipeline.Resolver{}, func(string) (string, bool) { return "", false },
		pipeline.CheckOptions{})
}

// Validation runs check, the base revision's evals of each affected action before and after (real evals of the fake
// action), and the backtest: a distill whose code breaks a base case fails however its own new evals
// score, and a check error stops it before the evals and the backtest.
func TestValidate(t *testing.T) {
	base := basePackage(t)
	newEval := newFile("actions/coded/evals/mine.yaml", "config: { mode: rules }", "input: {}", "assert: [output.score == 1]")
	tests := []struct {
		name     string
		kind     store.ProposalKind
		patch    string
		backtest *fakeBacktest
		passed   bool
		check    func(t *testing.T, r Report)
	}{
		{"distill", store.ProposalDistill, distill("coded", newEval), &fakeBacktest{passed: true}, true,
			func(t *testing.T, r Report) {
				if len(r.Evals) != 1 || r.Evals[0].After != "./actions/coded" || len(r.Evals[0].Cases) != 1 ||
					!r.Evals[0].Passed || r.Evals[0].Cases[0].Before == nil || len(r.NewEvals) != 1 ||
					r.NewEvals[0].Cases[0].Before != nil || !r.NewEvals[0].Passed || string(r.Backtest.Report) == "" {
					t.Fatalf("report %+v", r)
				}
			}},
		{"distill regresses", store.ProposalDistill, distill("coded-fallback"), &fakeBacktest{passed: true}, false,
			func(t *testing.T, r Report) {
				c := r.Evals[0].Cases[0]
				if c.Case != "disagree" || !c.Before.Passed || c.After.Passed || r.Evals[0].Passed {
					t.Fatalf("cases %+v", r.Evals[0].Cases)
				}
			}},
		{"backtest errs", store.ProposalConfig, configPatch, &fakeBacktest{err: errors.New("no sample")}, false,
			func(t *testing.T, r Report) {
				if !strings.Contains(r.Backtest.Skipped, "no sample") {
					t.Fatalf("backtest %+v", r.Backtest)
				}
			}},
		{"no backtest runner", store.ProposalConfig, configPatch, nil, false, func(t *testing.T, r Report) {
			if r.Backtest.Skipped == "" {
				t.Fatalf("backtest %+v", r.Backtest)
			}
		}},
		{"check fails", store.ProposalConfig, brokenPatch, &fakeBacktest{passed: true}, false, nil},
	}
	scratch := t.TempDir()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Prepare(tt.kind, base, tt.patch)
			if err != nil {
				t.Fatal(err)
			}
			v := Validator{Check: checker, Evals: Evals{Launchers: inProcess}, Scratch: scratch}
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
			if tt.backtest != nil && tt.name != "check fails" && (len(tt.backtest.got) != 1 ||
				tt.backtest.got[0].BaseRevision != base.Hash || tt.backtest.got[0].Patch != tt.patch) {
				t.Fatalf("backtest asked %+v", tt.backtest.got)
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
			if p, err := Prepare(store.ProposalDistill, base, distill("coded")); err == nil {
				v := Validator{Check: checker, Evals: Evals{Launchers: inProcess}, Scratch: dir}
				_, _ = v.Validate(context.Background(), "listings", p)
			}
		}
		_ = os.RemoveAll(dir)
	}
	os.Exit(m.Run())
}
