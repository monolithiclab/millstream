package api

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// The actions apply the store's units, or the engine's when variables must resolve against a revision (trigger,
// replay): the same code paths as the CLI commands.

func (s *Server) pausePipeline(w http.ResponseWriter, r *http.Request) error {
	return s.setPaused(w, r, "", true)
}

func (s *Server) resumePipeline(w http.ResponseWriter, r *http.Request) error {
	return s.setPaused(w, r, "", false)
}

func (s *Server) pauseStep(w http.ResponseWriter, r *http.Request) error {
	return s.setPaused(w, r, r.PathValue("step"), true)
}

func (s *Server) resumeStep(w http.ResponseWriter, r *http.Request) error {
	return s.setPaused(w, r, r.PathValue("step"), false)
}

// setPaused pauses or resumes the pipeline, or one of its active revision's steps.
func (s *Server) setPaused(w http.ResponseWriter, r *http.Request, step string, paused bool) error {
	name := r.PathValue("pipeline")
	_, p, err := s.pipelineState(r.Context(), name)
	if err != nil {
		return err
	}
	if _, ok := p.Steps[step]; step != "" && !ok {
		return fmt.Errorf("step %s of %s: %w", step, name, store.ErrNotFound)
	}
	if err := s.st.SetPaused(r.Context(), name, step, paused); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// TriggerRequest fires a trigger by hand.
type TriggerRequest struct {
	// Trigger defaults to the pipeline's first manual trigger, by name.
	Trigger   string          `json:"trigger"`
	Payload   json.RawMessage `json:"payload"`
	RequestID string          `json:"requestId"`
}

func (s *Server) triggerRun(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	var req TriggerRequest
	if err := decodeBody(w, r, &req); err != nil {
		return err
	}
	if s.opts.Engine == nil {
		return fmt.Errorf("trigger: no engine: %w", errUnavailable)
	}
	if req.Trigger == "" {
		_, p, err := s.pipelineState(r.Context(), name)
		if err != nil {
			return err
		}
		for _, t := range slices.Sorted(maps.Keys(p.Triggers)) {
			if p.Triggers[t].Manual != nil {
				req.Trigger = t
				break
			}
		}
		if req.Trigger == "" {
			return invalidf("pipeline %s has no manual trigger: name one", name)
		}
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		req.Payload = nil
	}
	id, created, err := s.opts.Engine.Fire(r.Context(), name, req.Trigger, req.RequestID, req.Payload)
	if err != nil {
		return err
	}
	return s.writeRun(w, r, name, id, created)
}

// writeRun answers a run an action started: 201 when it did, 200 when the request had already started it.
func (s *Server) writeRun(w http.ResponseWriter, r *http.Request, pipelineName, id string, created bool) error {
	run, err := s.st.Run(r.Context(), pipelineName, id)
	if err != nil {
		return err
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, toRun(run))
	return nil
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) error {
	if err := s.st.Cancel(r.Context(), r.PathValue("pipeline"), r.PathValue("run")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// ReplayRequest starts a replay run of a step over past items (spec section 5.7).
type ReplayRequest struct {
	Step string `json:"step"`
	// Since keeps the items published within this duration (7d, 12h).
	Since     string `json:"since"`
	Run       string `json:"run"`
	Where     string `json:"where"`
	Revision  int64  `json:"revision"`
	RequestID string `json:"requestId"`
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	var req ReplayRequest
	if err := decodeBody(w, r, &req); err != nil {
		return err
	}
	if req.Step == "" {
		return invalidf("step: required")
	}
	var since time.Duration
	if req.Since != "" {
		var err error
		if since, err = pipeline.ParseDuration(req.Since); err != nil {
			return invalidf("since: %s", err.Error())
		}
	}
	if s.opts.Engine == nil {
		return fmt.Errorf("replay: no engine: %w", errUnavailable)
	}
	id, created, err := s.opts.Engine.Replay(r.Context(), engine.ReplayRequest{
		Pipeline: name, Step: req.Step, Revision: store.RevisionID(req.Revision), Since: since, RunID: req.Run,
		Where: req.Where, RequestID: req.RequestID,
	})
	if err != nil {
		return err
	}
	return s.writeRun(w, r, name, id, created)
}

// DeadLetterSelection names dead letters: exactly one of IDs, Fingerprint (a group of the list) or All, among the
// pipeline's open ones.
type DeadLetterSelection struct {
	IDs         []string `json:"ids"`
	Fingerprint string   `json:"fingerprint"`
	All         bool     `json:"all"`
	// Migrate moves each run to the pipeline's active revision before replaying (replay only).
	Migrate bool `json:"migrate"`
}

// selectDeadLetters returns the open dead letters a selection names. An id that is not an open dead letter of the
// pipeline is notFound: the whole action is refused rather than half applied.
func (s *Server) selectDeadLetters(w http.ResponseWriter, r *http.Request) ([]store.DeadLetter, DeadLetterSelection,
	error,
) {
	name := r.PathValue("pipeline")
	var sel DeadLetterSelection
	if err := decodeBody(w, r, &sel); err != nil {
		return nil, sel, err
	}
	n := 0
	for _, set := range []bool{len(sel.IDs) > 0, sel.Fingerprint != "", sel.All} {
		if set {
			n++
		}
	}
	if n != 1 {
		return nil, sel, invalidf("name the dead letters with exactly one of ids, fingerprint and all")
	}
	if _, err := s.st.ActiveRevision(r.Context(), name); err != nil {
		return nil, sel, err
	}
	open, err := s.st.DeadLetters(r.Context(), store.DeadLetterFilter{Pipeline: name, State: store.DeadLetterOpen})
	if err != nil {
		return nil, sel, err
	}
	switch {
	case sel.All:
		return open, sel, nil
	case sel.Fingerprint != "":
		var out []store.DeadLetter
		for _, dl := range open {
			if fp, _ := deadLetterFingerprint(dl); fp == sel.Fingerprint {
				out = append(out, dl)
			}
		}
		if len(out) == 0 {
			return nil, sel, fmt.Errorf("open dead letters with fingerprint %s: %w", sel.Fingerprint, store.ErrNotFound)
		}
		return out, sel, nil
	}
	var out []store.DeadLetter
	for _, id := range sel.IDs {
		i := slices.IndexFunc(open, func(dl store.DeadLetter) bool { return dl.ID == id })
		if i < 0 {
			return nil, sel, fmt.Errorf("open dead letter %s: %w", id, store.ErrNotFound)
		}
		out = append(out, open[i])
	}
	return out, sel, nil
}

// replayDeadLetters replays the selected dead letters, one Dead-letter replay unit per run, as millstream replay
// --from dlq does for one run.
func (s *Server) replayDeadLetters(w http.ResponseWriter, r *http.Request) error {
	dls, sel, err := s.selectDeadLetters(w, r)
	if err != nil {
		return err
	}
	byRun := map[string][]string{}
	for _, dl := range dls {
		byRun[dl.RunID] = append(byRun[dl.RunID], dl.ID)
	}
	total := 0
	for _, run := range slices.Sorted(maps.Keys(byRun)) {
		n, err := s.st.ReplayDeadLetters(r.Context(), r.PathValue("pipeline"), run, byRun[run], sel.Migrate)
		if err != nil {
			return err
		}
		total += n
	}
	writeJSON(w, http.StatusOK, Count{Count: total})
	return nil
}

func (s *Server) discardDeadLetters(w http.ResponseWriter, r *http.Request) error {
	dls, sel, err := s.selectDeadLetters(w, r)
	if err != nil {
		return err
	}
	if sel.Migrate {
		return invalidf("migrate goes with replay, not discard")
	}
	ids := make([]string, len(dls))
	for i, dl := range dls {
		ids[i] = dl.ID
	}
	n, err := s.st.DiscardDeadLetters(r.Context(), r.PathValue("pipeline"), ids)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, Count{Count: n})
	return nil
}

// ReviewDecision decides a review (spec section 8).
type ReviewDecision struct {
	// Decision is approve or reject.
	Decision string `json:"decision"`
	// Version is the review's version the reviewer saw: a review escalated or decided since is a conflict. Zero
	// decides the version the review has now.
	Version int64 `json:"version"`
	// Patch is a JSON Patch (RFC 6902) applied to the item before an approval, within the editable pointers.
	Patch  json.RawMessage `json:"patch"`
	Reason string          `json:"reason"`
	Text   string          `json:"text"`
}

func (s *Server) decideReview(w http.ResponseWriter, r *http.Request) error {
	name, delivery := r.PathValue("pipeline"), r.PathValue("delivery")
	var req ReviewDecision
	if err := decodeBody(w, r, &req); err != nil {
		return err
	}
	if req.Decision != store.DecisionApprove && req.Decision != store.DecisionReject {
		return invalidf("decision %q: want approve or reject", req.Decision)
	}
	if len(req.Patch) > 0 && string(req.Patch) != "null" {
		if req.Decision == store.DecisionReject {
			return invalidf("patch goes with approve, not reject")
		}
		if _, err := review.ParsePatch(req.Patch); err != nil {
			return invalidf("patch: %s", err.Error())
		}
	} else {
		req.Patch = nil
	}
	reviewer, err := s.user(r)
	if err != nil {
		return err
	}
	if req.Version == 0 {
		rv, err := s.st.Review(r.Context(), name, delivery)
		if err != nil {
			return err
		}
		req.Version = rv.Version
	}
	if _, err := s.st.DecideReview(r.Context(), store.ReviewDecisionRequest{Pipeline: name, DeliveryID: delivery,
		Version: req.Version, Decision: req.Decision, Patch: req.Patch, Reviewer: reviewer, Reason: req.Reason,
		Text: req.Text}); err != nil {
		return err
	}
	return s.getReview(w, r)
}

// SampleDecision decides a review sample: agree or disagree with the rule that approved the item.
type SampleDecision struct {
	Decision string `json:"decision"`
	Reason   string `json:"reason"`
}

func (s *Server) decideSample(w http.ResponseWriter, r *http.Request) error {
	var req SampleDecision
	if err := decodeBody(w, r, &req); err != nil {
		return err
	}
	if req.Decision != store.SampleAgree && req.Decision != store.SampleDisagree {
		return invalidf("decision %q: want agree or disagree", req.Decision)
	}
	reviewer, err := s.user(r)
	if err != nil {
		return err
	}
	if err := s.st.DecideSample(r.Context(), store.SampleDecisionRequest{Pipeline: r.PathValue("pipeline"),
		DeliveryID: r.PathValue("delivery"), Decision: req.Decision, Reviewer: reviewer, Reason: req.Reason}); err != nil {
		return err
	}
	return s.getSample(w, r)
}

func (s *Server) acknowledgeInsight(w http.ResponseWriter, r *http.Request) error {
	return s.setInsightState(w, r, store.InsightAcknowledged)
}

func (s *Server) muteInsight(w http.ResponseWriter, r *http.Request) error {
	return s.setInsightState(w, r, store.InsightMuted)
}

func (s *Server) setInsightState(w http.ResponseWriter, r *http.Request, state store.InsightState) error {
	if err := s.st.SetInsightState(r.Context(), r.PathValue("pipeline"), r.PathValue("insight"), state); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
