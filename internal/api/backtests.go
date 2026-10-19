package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/store"
)

// BacktestRequest asks for a backtest of a patch (spec section 11.3): the patch is the unified diff's text.
type BacktestRequest struct {
	Pipeline     string   `json:"pipeline"`
	BaseRevision string   `json:"baseRevision"`
	Patch        string   `json:"patch"`
	Sample       int      `json:"sample"`
	Steps        []string `json:"steps"`
	Source       string   `json:"source"`
}

// Backtest is a backtest's state, and its report once done.
type Backtest struct {
	ID         string           `json:"id"`
	Pipeline   string           `json:"pipeline"`
	State      string           `json:"state"`
	CreatedAt  time.Time        `json:"createdAt"`
	FinishedAt *time.Time       `json:"finishedAt,omitempty"`
	Error      *errorDetail     `json:"error,omitempty"`
	Report     *backtest.Report `json:"report,omitempty"`
}

// toBacktest is a job as the API answers it; a failure's detail is the error answer's, an internal one logged.
func (s *Server) toBacktest(r *http.Request, j backtest.Job) Backtest {
	b := Backtest{ID: j.ID, Pipeline: j.Pipeline, State: j.State, CreatedAt: j.CreatedAt, Report: j.Report}
	if !j.FinishedAt.IsZero() {
		b.FinishedAt = &j.FinishedAt
	}
	if j.Err != nil {
		_, code := errorCode(j.Err)
		msg := j.Err.Error()
		if code == codeInternal {
			s.log.Error("backtest", "id", j.ID, "path", r.URL.Path, "error", msg)
			msg = "internal error"
		}
		b.Error = &errorDetail{Code: code, Message: msg}
	}
	return b
}

// createBacktest starts a backtest and answers 202 with its id; what it can refuse at once it refuses.
func (s *Server) createBacktest(w http.ResponseWriter, r *http.Request) error {
	var req BacktestRequest
	if err := s.decodeBody(w, r, &req); err != nil {
		return err
	}
	if s.opts.Backtests == nil {
		return fmt.Errorf("backtests: not configured on this server: %w", errUnavailable)
	}
	job, err := s.opts.Backtests.Start(backtest.Request{Pipeline: req.Pipeline, BaseRevision: req.BaseRevision,
		Patch: []byte(req.Patch), Sample: req.Sample, Steps: req.Steps, Source: req.Source})
	if err != nil {
		return err
	}
	w.Header().Set("Location", Prefix+"/backtests/"+job.ID)
	writeJSON(w, http.StatusAccepted, s.toBacktest(r, job))
	return nil
}

// getBacktest answers a backtest's state, and its report once done.
func (s *Server) getBacktest(w http.ResponseWriter, r *http.Request) error {
	if s.opts.Backtests == nil {
		return fmt.Errorf("backtests: not configured on this server: %w", errUnavailable)
	}
	job, ok := s.opts.Backtests.Get(r.PathValue("backtest"))
	if !ok {
		return fmt.Errorf("backtest %s: %w", r.PathValue("backtest"), store.ErrNotFound)
	}
	writeJSON(w, http.StatusOK, s.toBacktest(r, job))
	return nil
}
