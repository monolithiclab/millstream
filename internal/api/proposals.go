package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/store"
)

// Proposals (spec section 11.3): reviewable patches of a pipeline's package, created and validated by the engine,
// accepted on their base revision only, rejected with a reason. A proposal's id is unique across pipelines, so its
// resources sit under /proposals, filtered by pipeline.
//
// The proposals-only token (POST /proposal-token, answered once, kept as its SHA-256) is what the improve package
// calls the API with: a request bearing it may list, read, create and revalidate proposals and run backtests, and
// nothing else, whether or not the server requires auth.token. Accepting and rejecting stay a person's.

// Proposal is a proposal as the API shows it.
type Proposal struct {
	ID           string          `json:"id"`
	Pipeline     string          `json:"pipeline"`
	Kind         string          `json:"kind"`
	State        string          `json:"state"`
	BaseRevision string          `json:"baseRevision"`
	Patch        string          `json:"patch"`
	PatchSHA256  string          `json:"patchSha256"`
	Paths        []string        `json:"paths"`
	Steps        []string        `json:"steps"`
	Rationale    string          `json:"rationale"`
	Insights     []string        `json:"insights"`
	Fingerprint  string          `json:"fingerprint"`
	Report       json.RawMessage `json:"report"`
	CreatedBy    string          `json:"createdBy"`
	DecidedBy    string          `json:"decidedBy,omitempty"`
	Reason       string          `json:"reason,omitempty"`
	CreatedAt    time.Time       `json:"createdAt"`
	UpdatedAt    time.Time       `json:"updatedAt"`
	DecidedAt    *time.Time      `json:"decidedAt,omitempty"`
}

// ToProposal is the API's view of a stored proposal: the store gives its lists non-nil and its report null before
// the first validation.
func ToProposal(p store.Proposal) Proposal {
	out := Proposal{ID: p.ID, Pipeline: p.Pipeline, Kind: string(p.Kind), State: string(p.State),
		BaseRevision: p.BaseRevision, Patch: p.Patch, PatchSHA256: p.PatchSHA256, Paths: p.Paths,
		Steps: p.Steps, Rationale: p.Rationale, Insights: p.Insights,
		Fingerprint: p.Fingerprint, Report: p.Report, CreatedBy: p.CreatedBy, DecidedBy: p.DecidedBy, Reason: p.Reason,
		CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt}
	if !p.DecidedAt.IsZero() {
		out.DecidedAt = &p.DecidedAt
	}
	return out
}

// ProposalRequest creates a proposal on a pipeline's active revision.
type ProposalRequest struct {
	Pipeline    string   `json:"pipeline"`
	Kind        string   `json:"kind"`
	Patch       string   `json:"patch"`
	Rationale   string   `json:"rationale"`
	Insights    []string `json:"insights"`
	Fingerprint string   `json:"fingerprint"`
}

// ProposalDecision accepts a proposal (patchSha256, the hash of the patch the decider saw) or rejects one (reason).
type ProposalDecision struct {
	PatchSHA256 string `json:"patchSha256"`
	Reason      string `json:"reason"`
}

// ProposalToken is a proposals-only token, shown once.
type ProposalToken struct {
	Token string `json:"token"`
}

var proposalStates = []store.ProposalState{store.ProposalDrafting, store.ProposalValidating, store.ProposalReady,
	store.ProposalAccepted, store.ProposalRejected, store.ProposalStale}

// proposalRoutes are the operations a request bearing the proposals-only token may call.
var proposalRoutes = map[string]bool{
	"GET /openapi.yaml":                     true,
	"GET /proposals":                        true,
	"POST /proposals":                       true,
	"GET /proposals/{proposal}":             true,
	"POST /proposals/{proposal}/revalidate": true,
	"POST /backtests":                       true, // spec section 11.3: the backtest endpoint the validate step calls
	"GET /backtests/{backtest}":             true,
}

// proposalToken reports whether r bears the proposals-only token: its bearer token's SHA-256 is the one the store
// keeps, compared in constant time.
func (s *Server) proposalToken(r *http.Request) bool {
	token, ok := bearerToken(r)
	if !ok || !strings.HasPrefix(token, engine.ProposalTokenPrefix) {
		return false // not one: no store read
	}
	want, err := s.opts.Store.ProposalTokenHash(r.Context())
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(engine.TriggerTokenHash(token)), []byte(want)) == 1
}

// credentialKey carries how a request authenticated to the handlers.
type credentialKey struct{}

func credentialOf(r *http.Request) credential {
	c, _ := r.Context().Value(credentialKey{}).(credential)
	return c
}

func withCredential(r *http.Request, c credential) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), credentialKey{}, c))
}

// proposer is who creates a proposal: the improve package for the proposals-only token, else the request's user.
func (s *Server) proposer(r *http.Request) (string, error) {
	if credentialOf(r) == proposalCredential {
		return "improve", nil
	}
	return s.user(r)
}

func (s *Server) listProposals(w http.ResponseWriter, r *http.Request) error {
	state, err := enumParam(r, "state", proposalStates...)
	if err != nil {
		return err
	}
	ps, err := s.opts.Store.Proposals(r.Context(), store.ProposalFilter{Pipeline: r.URL.Query().Get("pipeline"),
		State: state})
	if err != nil {
		return err
	}
	out := make([]Proposal, 0, len(ps))
	for _, p := range ps {
		out = append(out, ToProposal(p))
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getProposal(w http.ResponseWriter, r *http.Request) error {
	p, err := s.opts.Store.Proposal(r.Context(), r.PathValue("proposal"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, ToProposal(p))
	return nil
}

// createProposal creates a proposal and validates it before answering: 201 with the proposal, ready or drafting
// with its report.
func (s *Server) createProposal(w http.ResponseWriter, r *http.Request) error {
	var req ProposalRequest
	if err := s.decodeBodyMax(w, r, &req, s.opts.MaxPackagesBody); err != nil {
		return err
	}
	if req.Pipeline == "" || req.Patch == "" {
		return invalidf("a proposal names its pipeline, kind and patch")
	}
	if s.opts.Engine == nil {
		return fmt.Errorf("proposal: no engine: %w", errUnavailable)
	}
	by, err := s.proposer(r)
	if err != nil {
		return err
	}
	p, err := s.opts.Engine.CreateProposal(r.Context(), engine.ProposalRequest{Pipeline: req.Pipeline,
		Kind: store.ProposalKind(req.Kind), Patch: req.Patch, Rationale: req.Rationale, Insights: req.Insights,
		Fingerprint: req.Fingerprint, By: by})
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, ToProposal(p))
	return nil
}

// revalidateProposal validates a proposal again, rebasing a stale one onto the active revision first.
func (s *Server) revalidateProposal(w http.ResponseWriter, r *http.Request) error {
	if err := s.decodeBody(w, r, &struct{}{}); err != nil {
		return err
	}
	if s.opts.Engine == nil {
		return fmt.Errorf("revalidate: no engine: %w", errUnavailable)
	}
	p, err := s.opts.Engine.ValidateProposal(r.Context(), r.PathValue("proposal"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, ToProposal(p))
	return nil
}

func (s *Server) acceptProposal(w http.ResponseWriter, r *http.Request) error {
	var req ProposalDecision
	if err := s.decodeBody(w, r, &req); err != nil {
		return err
	}
	if req.PatchSHA256 == "" {
		return invalidf("an acceptance names the patchSha256 of the patch it accepts")
	}
	if s.opts.Engine == nil {
		return fmt.Errorf("accept: no engine: %w", errUnavailable)
	}
	by, err := s.user(r)
	if err != nil {
		return err
	}
	p, err := s.opts.Engine.AcceptProposal(r.Context(), r.PathValue("proposal"), req.PatchSHA256, by)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, ToProposal(p))
	return nil
}

func (s *Server) rejectProposal(w http.ResponseWriter, r *http.Request) error {
	var req ProposalDecision
	if err := s.decodeBody(w, r, &req); err != nil {
		return err
	}
	by, err := s.user(r)
	if err != nil {
		return err
	}
	p, err := s.opts.Store.RejectProposal(r.Context(), r.PathValue("proposal"), req.Reason, by)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, ToProposal(p))
	return nil
}

// mintProposalToken mints the proposals-only token, replacing the earlier one, and answers it once.
func (s *Server) mintProposalToken(w http.ResponseWriter, r *http.Request) error {
	if err := s.decodeBody(w, r, &struct{}{}); err != nil {
		return err
	}
	if s.opts.Engine == nil {
		return fmt.Errorf("proposal token: no engine: %w", errUnavailable)
	}
	token, err := s.opts.Engine.MintProposalToken(r.Context())
	if err != nil {
		return err
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusCreated, ProposalToken{Token: token})
	return nil
}
