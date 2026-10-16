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
	_, _, p, err := activePipeline(r.Context(), s.opts.Store, name)
	if err != nil {
		return err
	}
	if _, ok := p.Steps[step]; step != "" && !ok {
		return fmt.Errorf("step %s of %s: %w", step, name, store.ErrNotFound)
	}
	if err := s.opts.Store.SetPaused(r.Context(), name, step, paused); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) disablePipeline(w http.ResponseWriter, r *http.Request) error {
	if err := s.opts.Store.Disable(r.Context(), r.PathValue("pipeline")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// RollbackRequest names the revision a rollback reactivates; zero, or absent, means the one before the active one.
type RollbackRequest struct {
	Revision int64 `json:"revision"`
}

// rollbackPipeline reactivates an earlier revision and answers it, now active.
func (s *Server) rollbackPipeline(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	var req RollbackRequest
	if err := s.decodeBody(w, r, &req); err != nil {
		return err
	}
	if req.Revision < 0 {
		return invalidf("revision %d: want a revision number", req.Revision)
	}
	id, err := s.opts.Store.Rollback(r.Context(), name, store.RevisionID(req.Revision))
	if err != nil {
		return err
	}
	rev, err := s.opts.Store.Revision(r.Context(), id)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, toRevision(rev, id))
	return nil
}

// ApplyRequest applies packages as new revisions (millstream apply): each package's files, by slash-separated path,
// as base64 in JSON.
type ApplyRequest struct {
	Packages []PackageFiles `json:"packages"`
	// Force lets through what only --force accepts: breaking an importer's contract.
	Force   bool   `json:"force"`
	Message string `json:"message"`
}

// PackageFiles is one package of an apply.
type PackageFiles struct {
	Files map[string][]byte `json:"files"`
}

// ApplyResponse is what an apply did: a revision per package, and the check's warnings.
type ApplyResponse struct {
	Revisions []AppliedRevision `json:"revisions"`
	Warnings  []Finding         `json:"warnings"`
}

// AppliedRevision is the revision of one package an apply applied; created is false when the package was already
// the pipeline's active one.
type AppliedRevision struct {
	Pipeline    string `json:"pipeline"`
	Revision    int64  `json:"revision"`
	PackageHash string `json:"packageHash"`
	Created     bool   `json:"created"`
}

// Finding is a check finding of a pipeline.
type Finding struct {
	Pipeline string `json:"pipeline"`
	Severity string `json:"severity"`
	Path     string `json:"path"`
	Message  string `json:"message"`
}

// applyPackages checks the packages against the whole deployment and applies each as a new revision. A check
// error refuses them all (invalid, the errors in its message).
func (s *Server) applyPackages(w http.ResponseWriter, r *http.Request) error {
	var req ApplyRequest
	if err := s.decodeBodyMax(w, r, &req, s.opts.MaxPackagesBody); err != nil {
		return err
	}
	if len(req.Packages) == 0 {
		return invalidf("packages: name at least one")
	}
	if s.opts.Engine == nil {
		return fmt.Errorf("apply: no engine: %w", errUnavailable)
	}
	pkgs := make([]*pipeline.Package, 0, len(req.Packages))
	for i, p := range req.Packages {
		pkg, err := s.opts.Engine.PackageFromFiles(p.Files)
		if err != nil {
			return invalidf("packages[%d]: %s", i, err.Error())
		}
		pkgs = append(pkgs, pkg)
	}
	applied, findings, err := s.opts.Engine.ApplyPackages(r.Context(), pkgs, engine.ApplyOptions{Force: req.Force,
		Message: req.Message})
	if err != nil {
		return err
	}
	out := ApplyResponse{Revisions: make([]AppliedRevision, 0, len(applied)), Warnings: make([]Finding, 0, len(findings))}
	for _, a := range applied {
		out.Revisions = append(out.Revisions, AppliedRevision{Pipeline: a.Pipeline, Revision: int64(a.Revision),
			PackageHash: a.PackageHash, Created: a.Created})
	}
	for _, f := range findings {
		out.Warnings = append(out.Warnings, Finding{Pipeline: f.Pipeline, Severity: f.Severity, Path: f.Path,
			Message: f.Message})
	}
	writeJSON(w, http.StatusOK, out)
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
	if err := s.decodeBody(w, r, &req); err != nil {
		return err
	}
	if s.opts.Engine == nil {
		return fmt.Errorf("trigger: no engine: %w", errUnavailable)
	}
	if req.Trigger == "" {
		_, _, p, err := activePipeline(r.Context(), s.opts.Store, name)
		if err != nil {
			return err
		}
		var ok bool
		if req.Trigger, ok = p.FirstManualTrigger(); !ok {
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
	run, err := s.opts.Store.Run(r.Context(), pipelineName, id)
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
	if err := s.opts.Store.Cancel(r.Context(), r.PathValue("pipeline"), r.PathValue("run")); err != nil {
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
	if err := s.decodeBody(w, r, &req); err != nil {
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
	if err := s.decodeBody(w, r, &sel); err != nil {
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
	if _, err := s.opts.Store.ActiveRevision(r.Context(), name); err != nil {
		return nil, sel, err
	}
	open, err := s.opts.Store.DeadLetters(r.Context(), store.DeadLetterFilter{Pipeline: name, State: store.DeadLetterOpen})
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
		n, err := s.opts.Store.ReplayDeadLetters(r.Context(), r.PathValue("pipeline"), run, byRun[run], sel.Migrate)
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
	n, err := s.opts.Store.DiscardDeadLetters(r.Context(), r.PathValue("pipeline"), ids)
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
	// Version is the review's version the reviewer saw, required: a review escalated or decided since is a conflict.
	Version int64 `json:"version"`
	// Patch is a JSON Patch (RFC 6902) applied to the item before an approval, within the editable pointers.
	Patch  json.RawMessage `json:"patch"`
	Reason string          `json:"reason"`
	Text   string          `json:"text"`
}

func (s *Server) decideReview(w http.ResponseWriter, r *http.Request) error {
	name, delivery := r.PathValue("pipeline"), r.PathValue("delivery")
	var req ReviewDecision
	if err := s.decodeBody(w, r, &req); err != nil {
		return err
	}
	if string(req.Patch) == "null" { // the store checks the decision and the edit
		req.Patch = nil
	}
	reviewer, err := s.user(r)
	if err != nil {
		return err
	}
	if req.Version < 1 {
		return invalidf("version: required, the version of the review you saw")
	}
	if _, err := s.opts.Store.DecideReview(r.Context(), store.ReviewDecisionRequest{Pipeline: name, DeliveryID: delivery,
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
	if err := s.decodeBody(w, r, &req); err != nil {
		return err
	}
	if req.Decision != store.SampleAgree && req.Decision != store.SampleDisagree {
		return invalidf("decision %q: want agree or disagree", req.Decision)
	}
	reviewer, err := s.user(r)
	if err != nil {
		return err
	}
	if err := s.opts.Store.DecideSample(r.Context(), store.SampleDecisionRequest{Pipeline: r.PathValue("pipeline"),
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
	if err := s.opts.Store.SetInsightState(r.Context(), r.PathValue("pipeline"), r.PathValue("insight"), state); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
