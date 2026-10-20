package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/distill"
	"github.com/monolithiclab/millstream/internal/pipeline"
)

// DistillTraining is what the drafting agent of a distill proposal may see of an agent step's history (spec section
// 11.5): the training pairs of the step's split by time and entity, the split's boundary, and the step's lessons
// with the examples derived from the holdout withheld. The holdout is never served.
type DistillTraining struct {
	Pipeline         string           `json:"pipeline"`
	Step             string           `json:"step"`
	Agent            string           `json:"agent"`
	Source           string           `json:"source"`
	HoldoutFrom      time.Time        `json:"holdoutFrom"`
	Pairs            int              `json:"pairs"`
	Holdout          int              `json:"holdout"`
	Training         []distill.Pair   `json:"training"`
	Lessons          pipeline.Lessons `json:"lessons"`
	WithheldExamples int              `json:"withheldExamples"`
}

// getDistillTraining answers an agent step's training set.
func (s *Server) getDistillTraining(w http.ResponseWriter, r *http.Request) error {
	if s.opts.Backtests == nil {
		return fmt.Errorf("distill training: not configured on this server: %w", errUnavailable)
	}
	t, err := s.opts.Backtests.Training(r.Context(), r.PathValue("pipeline"), r.PathValue("step"))
	if errors.Is(err, backtest.ErrNoTraining) {
		err = fmt.Errorf("%w: %w", err, errUnavailable)
	}
	if err != nil {
		return err
	}
	out := DistillTraining{Pipeline: t.Pipeline, Step: t.Step, Agent: t.Agent, Source: t.Source,
		HoldoutFrom: t.HoldoutFrom.UTC(), Pairs: t.Pairs, Holdout: t.Holdout, Training: t.Train, Lessons: t.Lessons,
		WithheldExamples: t.Withheld}
	if out.Training == nil {
		out.Training = []distill.Pair{}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}
