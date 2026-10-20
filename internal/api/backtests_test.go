package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// fakeBacktests runs a backtest by answering what it is given once release is closed, and refuses an empty patch
// as the engine's Prepare would.
type fakeBacktests struct {
	release chan struct{}
	rep     *backtest.Report
	err     error
}

func (f *fakeBacktests) Prepare(req backtest.Request) (*patch.Patch, error) {
	if len(req.Patch) == 0 {
		return nil, errors.Join(errors.New("a patch is required"), store.ErrInvalid)
	}
	return &patch.Patch{}, nil
}

func (f *fakeBacktests) Run(context.Context, backtest.Request) (*backtest.Report, error) {
	<-f.release
	return f.rep, f.err
}

// TestBacktests: POST starts a backtest and answers 202 with its Location; GET reads it running, then done with its
// report, or failed with the error's code (an internal one's message withheld); a refusal is answered at once, an
// unknown id is notFound, and a server without backtests answers unavailable.
func TestBacktests(t *testing.T) {
	f := readFixture(t)
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	rep := &backtest.Report{Pipeline: "listings", BaseRevision: "abc", PatchHash: "def", PatchedRevision: "ghi",
		Source: backtest.SourceLocal, ChangedSteps: []string{"score"}, Sampled: 2, Strata: map[string]int{"done": 2},
		Runs: 1, Before: backtest.Metrics{Compared: 2, Agreed: 2, Agreement: 1},
		Steps: []backtest.StepReport{{Step: "score"}}, Divergence: []backtest.Divergence{{PackageHash: "x", Sampled: 1}},
		Warnings: []string{"diverged"}, StartedAt: now, FinishedAt: now}
	for _, c := range []struct {
		name  string
		err   error
		state string
		code  string
	}{
		{"done", nil, backtest.JobDone, ""},
		{"refused", backtest.ErrBudget, backtest.JobFailed, codeConflict},
		{"failed", errors.New("disk on fire"), backtest.JobFailed, codeInternal},
	} {
		fake := &fakeBacktests{release: make(chan struct{}), rep: rep, err: c.err}
		if c.err != nil {
			fake.rep = nil
		}
		jobs := backtest.NewJobs(context.Background(), fake, func() time.Time { return now })
		srv, err := New(Options{Store: f.st, Clock: f.clock, Backtests: jobs})
		if err != nil {
			t.Fatal(err)
		}
		rec := do(t, srv, request{method: http.MethodPost, target: "/api/v1/backtests",
			body: `{"pipeline":"listings","baseRevision":"abc","patch":"--- a/x\n+++ b/x\n"}`})
		var started Backtest
		if err := json.Unmarshal(rec.Body.Bytes(), &started); err != nil || rec.Code != http.StatusAccepted ||
			started.State != backtest.JobRunning || rec.Header().Get("Location") != "/api/v1/backtests/"+started.ID {
			t.Fatalf("%s: POST = %d %s", c.name, rec.Code, rec.Body)
		}
		if got := get[Backtest](t, srv, "/api/v1/backtests/"+started.ID); got.State != backtest.JobRunning {
			t.Errorf("%s: running = %+v", c.name, got)
		}
		close(fake.release)
		jobs.Wait()
		got := get[Backtest](t, srv, "/api/v1/backtests/"+started.ID)
		switch {
		case got.State != c.state || got.FinishedAt == nil:
			t.Errorf("%s: finished = %+v", c.name, got)
		case c.code == "" && (got.Report == nil || got.Report.Before.Agreement != 1 || got.Error != nil):
			t.Errorf("%s: report = %+v", c.name, got)
		case c.code != "" && (got.Error == nil || got.Error.Code != c.code || got.Report != nil ||
			c.code == codeInternal && got.Error.Message != "internal error"):
			t.Errorf("%s: error = %+v", c.name, got.Error)
		}
	}
	jobs := backtest.NewJobs(context.Background(), &fakeBacktests{}, nil)
	srv, err := New(Options{Store: f.st, Backtests: jobs})
	if err != nil {
		t.Fatal(err)
	}
	if rec := do(t, srv, request{method: http.MethodPost, target: "/api/v1/backtests",
		body: `{"pipeline":"listings","baseRevision":"abc","patch":""}`}); rec.Code != http.StatusBadRequest {
		t.Errorf("POST without a patch = %d", rec.Code)
	}
	if rec := do(t, srv, request{target: "/api/v1/backtests/bt_nope"}); rec.Code != http.StatusNotFound {
		t.Errorf("GET of an unknown backtest = %d", rec.Code)
	}
	bare, err := New(Options{Store: f.st})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []request{
		{method: http.MethodPost, target: "/api/v1/backtests", body: `{"pipeline":"p","baseRevision":"a","patch":"x"}`},
		{target: "/api/v1/backtests/bt_1"},
	} {
		if rec := do(t, bare, r); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s %s without backtests = %d", r.method, r.target, rec.Code)
		}
	}
}

// fakeTrainer serves a fixed training set, or refuses.
type fakeTrainer struct {
	fakeBacktests
	t   backtest.Training
	err error
}

func (f *fakeTrainer) Training(_ context.Context, pipelineName, step string) (backtest.Training, error) {
	f.t.Pipeline, f.t.Step = pipelineName, step
	return f.t, f.err
}

// TestDistillTraining: GET .../distill-training answers the training side of a step's split, never the holdout's
// pairs; a refusal keeps its code, and a server whose runner serves no training sets, or that has none, is
// unavailable.
func TestDistillTraining(t *testing.T) {
	f := readFixture(t)
	from := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	trainer := &fakeTrainer{t: backtest.Training{Agent: "./actions/score", Source: backtest.SourceLocal,
		HoldoutFrom: from, Pairs: 3, Holdout: 1, Withheld: 1,
		Train: []distill.Pair{{Item: "it_1", Key: "A", Input: json.RawMessage(`{"id":"A"}`),
			Outputs: []distill.Output{{Key: "A", Payload: json.RawMessage(`{"id":"A","tier":"low"}`)}}, Edited: true},
			{Item: "it_2", Key: "B", Input: json.RawMessage(`{"id":"B"}`),
				Outputs: []distill.Output{{Payload: json.RawMessage(`{"id":"B","tier":"high"}`)}}}},
		Lessons: pipeline.Lessons{Rules: []pipeline.LessonRule{{Text: "Tier by price per m²."}}}}}
	const target = "/api/v1/pipelines/listings/steps/score/distill-training"
	srv, err := New(Options{Store: f.st, Clock: f.clock, Backtests: backtest.NewJobs(context.Background(), trainer, nil)})
	if err != nil {
		t.Fatal(err)
	}
	got := get[DistillTraining](t, srv, target)
	if got.Pipeline != "listings" || got.Step != "score" || len(got.Training) != 2 || !got.HoldoutFrom.Equal(from) ||
		got.Holdout != 1 || got.WithheldExamples != 1 || !got.Training[0].Edited || len(got.Lessons.Rules) != 1 {
		t.Fatalf("training %+v", got)
	}
	trainer.err = errors.Join(errors.New("not an agent step"), store.ErrInvalid)
	if rec := do(t, srv, request{method: http.MethodGet, target: target}); rec.Code != http.StatusBadRequest {
		t.Errorf("refused = %d %s", rec.Code, rec.Body)
	}
	plain, err := New(Options{Store: f.st, Clock: f.clock,
		Backtests: backtest.NewJobs(context.Background(), &fakeBacktests{}, nil)})
	if err != nil {
		t.Fatal(err)
	}
	none, err := New(Options{Store: f.st, Clock: f.clock})
	if err != nil {
		t.Fatal(err)
	}
	for name, s := range map[string]*Server{"no trainer": plain, "no backtests": none} {
		if rec := do(t, s, request{method: http.MethodGet, target: target}); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s = %d %s", name, rec.Code, rec.Body)
		}
	}
}
