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
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// fakeTrainer serves a fixed training set, or refuses.
type fakeTrainer struct {
	t   backtest.Training
	err error
}

func (f *fakeTrainer) Training(_ context.Context, pipelineName, step string) (backtest.Training, error) {
	f.t.Pipeline, f.t.Step = pipelineName, step
	return f.t, f.err
}

// TestDistillTraining: GET .../distill-training answers the training side of a step's split, never the holdout's
// pairs; a refusal keeps its code, and a server with no training source is unavailable.
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
	srv, err := New(Options{Store: f.st, Clock: f.clock, Training: trainer})
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
	none, err := New(Options{Store: f.st, Clock: f.clock})
	if err != nil {
		t.Fatal(err)
	}
	if rec := do(t, none, request{method: http.MethodGet, target: target}); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("no training source = %d %s", rec.Code, rec.Body)
	}
}
