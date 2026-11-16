package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Proposals (spec section 11.3): reviewable patches of a pipeline's package. The engine checks a patch against its
// kind's path rules and validates it; the units here keep the states honest. A proposal is open while drafting,
// validating or ready; it becomes stale when its base revision is no longer the pipeline's active one, which every
// unit changing the active revision applies (staleProposals), and when an accepted proposal overlaps it.

const proposalColumns = `id, pipeline, kind, base_revision, patch, patch_sha256, paths, steps, rationale, insights,
	fingerprint, state, report, created_by, decided_by, reason, rejected_severity, created_at, updated_at, decided_at`

func scanProposal(row interface{ Scan(...any) error }) (store.Proposal, error) {
	var (
		p                         store.Proposal
		paths, steps, ins, report string
		created, updated, decided TimeCol
	)
	if err := row.Scan(&p.ID, &p.Pipeline, &p.Kind, &p.BaseRevision, &p.Patch, &p.PatchSHA256, &paths, &steps,
		&p.Rationale, &ins, &p.Fingerprint, &p.State, &report, &p.CreatedBy, &p.DecidedBy, &p.Reason,
		&p.RejectedSeverity, &created, &updated, &decided); err != nil {
		return p, err
	}
	for _, l := range []struct {
		src string
		dst *[]string
	}{{paths, &p.Paths}, {steps, &p.Steps}, {ins, &p.Insights}} {
		if err := json.Unmarshal([]byte(l.src), l.dst); err != nil {
			return p, fmt.Errorf("proposal %s: %w", p.ID, err)
		}
	}
	p.Report = json.RawMessage(report)
	p.CreatedAt, p.UpdatedAt, p.DecidedAt = created.Time, updated.Time, decided.Time
	return p, nil
}

// proposal reads a proposal through the unit's transaction.
func (w *wtx) proposal(id string) (store.Proposal, error) {
	p, err := scanProposal(w.queryRow(`SELECT `+proposalColumns+` FROM proposals WHERE id = ?`, id))
	if err != nil {
		return p, notFound(err, "proposal "+id)
	}
	return p, nil
}

// activeHash returns the package hash of a pipeline's active revision.
func (w *wtx) activeHash(pipelineName string) (string, error) {
	var hash string
	err := w.queryRow(`SELECT r.package_hash FROM pipelines p JOIN revisions r ON r.id = p.active_revision
		WHERE p.id = ?`, pipelineName).Scan(&hash)
	if err != nil {
		return "", notFound(err, "pipeline "+pipelineName)
	}
	return hash, nil
}

// setProposalState moves a proposal to state, with a proposal event carrying data besides its id and state.
func (w *wtx) setProposalState(id, pipelineName string, state store.ProposalState, kind string, data map[string]any,
) error {
	if _, err := w.exec(`UPDATE proposals SET state = ?, updated_at = ? WHERE id = ?`, state, w.ts(), id); err != nil {
		return err
	}
	if data == nil {
		data = map[string]any{}
	}
	data["proposal"], data["state"] = id, state
	return w.event(pipelineName, "", kind, data)
}

// staleProposals makes stale every open proposal of a pipeline whose base is not hash, its active revision's package
// now: every unit that changes a pipeline's active revision calls it.
func (w *wtx) staleProposals(pipelineName, hash string) error {
	ids, err := queryAll(w, scanString, `SELECT id FROM proposals WHERE pipeline = ? AND state IN (?, ?, ?)
		AND base_revision <> ? ORDER BY id`, pipelineName, store.ProposalDrafting, store.ProposalValidating,
		store.ProposalReady, hash)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := w.setProposalState(id, pipelineName, store.ProposalStale, store.EventProposalStale, map[string]any{"cause": "revision"}); err != nil {
			return err
		}
	}
	return nil
}

// CreateProposal creates a drafting proposal: see store.Store.
func (s *Store) CreateProposal(ctx context.Context, np store.NewProposal) (store.Proposal, error) {
	if np.ID == "" || np.Pipeline == "" || np.BaseRevision == "" || np.Patch == "" || np.PatchSHA256 == "" ||
		np.Fingerprint == "" || len(np.Paths) == 0 || !slices.Contains(store.ProposalKinds, np.Kind) {
		return store.Proposal{}, fmt.Errorf("create proposal: an id, a pipeline, a kind, a base, a patch with its "+
			"hash and paths, and a fingerprint are required: %w", store.ErrInvalid)
	}
	var out store.Proposal
	err := s.submit(ctx, unitCreateProposal, func(w *wtx) error {
		hash, err := w.activeHash(np.Pipeline)
		if err != nil {
			return err
		}
		if hash != np.BaseRevision {
			return fmt.Errorf("proposal base %.12s is not the active revision of %s (%.12s): rebase it: %w",
				np.BaseRevision, np.Pipeline, hash, store.ErrConflict)
		}
		if err := w.openFingerprint(np); err != nil {
			return err
		}
		if err := w.suppressed(np); err != nil {
			return err
		}
		n, err := w.affected(`INSERT INTO proposals (`+proposalColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			'null', ?, '', '', '', ?, ?, NULL) ON CONFLICT (id) DO NOTHING`, np.ID, np.Pipeline, np.Kind,
			np.BaseRevision, np.Patch, np.PatchSHA256, jsonList(np.Paths), jsonList(np.Steps), np.Rationale,
			jsonList(np.Insights), np.Fingerprint, store.ProposalDrafting, np.CreatedBy, w.ts(), w.ts())
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("proposal %s exists: %w", np.ID, store.ErrConflict)
		}
		if err := w.event(np.Pipeline, "", store.EventProposalCreated, map[string]any{"proposal": np.ID,
			"kind": np.Kind, "state": store.ProposalDrafting}); err != nil {
			return err
		}
		out, err = w.proposal(np.ID)
		return err
	})
	if err != nil {
		return store.Proposal{}, fmt.Errorf("create proposal: %w", err)
	}
	return out, nil
}

// openFingerprint refuses a proposal answering insights whose fingerprint an open proposal of the pipeline
// (drafting, validating or ready) answering insights has already: one idea, one open proposal, however many runs
// drafted it at once. A proposal answering no insight, an alternative a person drafts, neither is refused nor holds
// another back (spec section 11.3).
func (w *wtx) openFingerprint(np store.NewProposal) error {
	if len(np.Insights) == 0 {
		return nil
	}
	var id string
	err := w.queryRow(`SELECT id FROM proposals WHERE pipeline = ? AND fingerprint = ? AND state IN (?, ?, ?)
		AND insights <> '[]' ORDER BY id LIMIT 1`, np.Pipeline, np.Fingerprint, store.ProposalDrafting,
		store.ProposalValidating, store.ProposalReady).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("the open proposal %s has this fingerprint already: %w", id, store.ErrConflict)
}

// suppressed refuses a proposal whose fingerprint a rejection suppresses: until an insight it answers weighs twice
// what the rejected one's did at its rejection (spec section 11.3).
func (w *wtx) suppressed(np store.NewProposal) error {
	if len(np.Insights) == 0 {
		return nil
	}
	rejected, err := queryAll(w, scanPair, `SELECT id, rejected_severity FROM proposals WHERE pipeline = ?
		AND fingerprint = ? AND state = ? ORDER BY id`, np.Pipeline, np.Fingerprint, store.ProposalRejected)
	if err != nil || len(rejected) == 0 {
		return err
	}
	_, now, err := w.maxSeverity(np.Pipeline, np.Insights)
	if err != nil {
		return err
	}
	for _, r := range rejected {
		if was := store.SeverityWeight(r[1]); now < 2*was {
			return fmt.Errorf("the fingerprint of proposal %s, rejected at severity %s, is suppressed until its "+
				"insight's severity doubles: %w", r[0], r[1], store.ErrConflict)
		}
	}
	return nil
}

// maxSeverity returns the highest severity among a pipeline's insights named, with its weight; an insight gone
// weighs nothing.
func (w *wtx) maxSeverity(pipelineName string, ids []string) (string, int, error) {
	severity, weight := "", 0
	for _, id := range ids {
		var sev string
		err := w.queryRow(`SELECT severity FROM insights WHERE pipeline = ? AND id = ?`, pipelineName, id).Scan(&sev)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", 0, err
		}
		if store.SeverityWeight(sev) > weight {
			severity, weight = sev, store.SeverityWeight(sev)
		}
	}
	return severity, weight, nil
}

// StartValidation moves a proposal to validating: see store.Store.
func (s *Store) StartValidation(ctx context.Context, id string, takeOver time.Time) (store.Proposal, error) {
	var out store.Proposal
	err := s.submit(ctx, unitStartValidation, func(w *wtx) error {
		p, err := w.proposal(id)
		if err != nil {
			return err
		}
		switch {
		case p.State == store.ProposalDrafting, p.State == store.ProposalReady, p.State == store.ProposalStale:
		case p.State == store.ProposalValidating && p.UpdatedAt.Before(takeOver):
		default:
			return fmt.Errorf("proposal %s is %s: only a drafting, ready or stale one is validated: %w", id, p.State,
				store.ErrConflict)
		}
		if _, err := w.exec(`UPDATE proposals SET state = ?, updated_at = ? WHERE id = ?`, store.ProposalValidating,
			w.ts(), id); err != nil {
			return err
		}
		out, err = w.proposal(id)
		return err
	})
	if err != nil {
		return store.Proposal{}, fmt.Errorf("start validation: %w", err)
	}
	return out, nil
}

// ResetValidations moves the validating proposals back to drafting: see store.Store.
func (s *Store) ResetValidations(ctx context.Context) ([]string, error) {
	var ids []string
	err := s.submit(ctx, unitResetValidations, func(w *wtx) error {
		rows, err := w.query(`SELECT id, pipeline FROM proposals WHERE state = ? ORDER BY id`, store.ProposalValidating)
		if err != nil {
			return err
		}
		var found [][2]string
		for rows.Next() {
			var id, pipelineName string
			if err := rows.Scan(&id, &pipelineName); err != nil {
				_ = rows.Close()
				return err
			}
			found = append(found, [2]string{id, pipelineName})
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		ids = ids[:0]
		for _, f := range found {
			if err := w.setProposalState(f[0], f[1], store.ProposalDrafting, store.EventProposalValidated,
				map[string]any{"passed": false, "interrupted": true}); err != nil {
				return err
			}
			ids = append(ids, f[0])
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reset validations: %w", err)
	}
	return ids, nil
}

// RecordValidation records a validating proposal's report: see store.Store.
func (s *Store) RecordValidation(ctx context.Context, v store.Validation) (store.Proposal, error) {
	if !json.Valid(v.Report) {
		return store.Proposal{}, fmt.Errorf("record validation of %s: the report is not JSON: %w", v.ID,
			store.ErrInvalid)
	}
	var out store.Proposal
	err := s.submit(ctx, unitRecordValidation, func(w *wtx) error {
		p, err := w.proposal(v.ID)
		if err != nil {
			return err
		}
		if p.State != store.ProposalValidating {
			return fmt.Errorf("proposal %s is %s, not validating: %w", v.ID, p.State, store.ErrConflict)
		}
		if r := v.Rebase; r != nil {
			if _, err := w.exec(`UPDATE proposals SET base_revision = ?, patch = ?, patch_sha256 = ?, paths = ?,
				steps = ? WHERE id = ?`, r.BaseRevision, r.Patch, r.PatchSHA256, jsonList(r.Paths), jsonList(r.Steps),
				v.ID); err != nil {
				return err
			}
			p.BaseRevision = r.BaseRevision
		}
		hash, err := w.activeHash(p.Pipeline)
		if err != nil {
			return err
		}
		state := store.ProposalDrafting
		switch {
		case hash != p.BaseRevision:
			state = store.ProposalStale
		case v.Passed:
			state = store.ProposalReady
		}
		if _, err := w.exec(`UPDATE proposals SET report = ? WHERE id = ?`, string(v.Report), v.ID); err != nil {
			return err
		}
		if err := w.setProposalState(p.ID, p.Pipeline, state, store.EventProposalValidated, map[string]any{"passed": v.Passed,
			"rebased": v.Rebase != nil}); err != nil {
			return err
		}
		out, err = w.proposal(v.ID)
		return err
	})
	if err != nil {
		return store.Proposal{}, fmt.Errorf("record validation: %w", err)
	}
	return out, nil
}

// AcceptProposal is the Proposal acceptance unit: see store.Store.
func (s *Store) AcceptProposal(ctx context.Context, id, patchSHA256, by string) (store.Proposal, error) {
	var (
		out   store.Proposal
		moved bool
	)
	err := s.submit(ctx, unitAcceptProposal, func(w *wtx) error {
		moved = false
		p, err := w.proposal(id)
		if err != nil {
			return err
		}
		switch {
		case p.State != store.ProposalReady:
			return fmt.Errorf("proposal %s is %s: only a ready one is accepted: %w", id, p.State, store.ErrConflict)
		case p.PatchSHA256 != patchSHA256:
			return fmt.Errorf("proposal %s: patch %.12s is not the validated one (%.12s): %w", id, patchSHA256,
				p.PatchSHA256, store.ErrConflict)
		}
		hash, err := w.activeHash(p.Pipeline)
		if err != nil {
			return err
		}
		if hash != p.BaseRevision {
			moved = true
			if err := w.setProposalState(p.ID, p.Pipeline, store.ProposalStale, store.EventProposalStale,
				map[string]any{"cause": "revision"}); err != nil {
				return err
			}
			out, err = w.proposal(id)
			return err
		}
		if _, err := w.exec(`UPDATE proposals SET decided_by = ?, decided_at = ? WHERE id = ?`, by, w.ts(),
			id); err != nil {
			return err
		}
		if err := w.setProposalState(p.ID, p.Pipeline, store.ProposalAccepted, store.EventProposalAccepted,
			map[string]any{"by": by}); err != nil {
			return err
		}
		if err := w.staleOverlapping(p); err != nil {
			return err
		}
		out, err = w.proposal(id)
		return err
	})
	if err != nil {
		return store.Proposal{}, fmt.Errorf("accept proposal: %w", err)
	}
	if moved {
		return out, fmt.Errorf("accept proposal %s: the active revision of %s is no longer its base: it is stale: %w",
			id, out.Pipeline, store.ErrConflict)
	}
	return out, nil
}

// staleOverlapping makes stale every other open proposal of p's pipeline that touches one of p's paths or steps
// (spec section 11.3): two proposals validated on the same base are never combined unvalidated.
func (w *wtx) staleOverlapping(p store.Proposal) error {
	open, err := queryAll(w, func(rows *sql.Rows) ([3]string, error) {
		var r [3]string
		return r, rows.Scan(&r[0], &r[1], &r[2])
	}, `SELECT id, paths, steps FROM proposals WHERE pipeline = ? AND id <> ? AND state IN (?, ?, ?) ORDER BY id`,
		p.Pipeline, p.ID, store.ProposalDrafting, store.ProposalValidating, store.ProposalReady)
	if err != nil {
		return err
	}
	for _, o := range open {
		var paths, steps []string
		if err := errors.Join(json.Unmarshal([]byte(o[1]), &paths), json.Unmarshal([]byte(o[2]), &steps)); err != nil {
			return fmt.Errorf("proposal %s: %w", o[0], err)
		}
		if !shares(paths, p.Paths) && !shares(steps, p.Steps) {
			continue
		}
		if err := w.setProposalState(o[0], p.Pipeline, store.ProposalStale, store.EventProposalStale,
			map[string]any{"cause": "accepted", "by": p.ID}); err != nil {
			return err
		}
	}
	return nil
}

// shares reports whether two lists have an element in common.
func shares(a, b []string) bool {
	return slices.ContainsFunc(a, func(x string) bool { return slices.Contains(b, x) })
}

// RejectProposal rejects a proposal with its reason: see store.Store.
func (s *Store) RejectProposal(ctx context.Context, id, reason, by string) (store.Proposal, error) {
	if reason == "" {
		return store.Proposal{}, fmt.Errorf("reject proposal %s: a reason is required: %w", id, store.ErrInvalid)
	}
	var out store.Proposal
	err := s.submit(ctx, unitRejectProposal, func(w *wtx) error {
		p, err := w.proposal(id)
		if err != nil {
			return err
		}
		if !p.State.Open() && p.State != store.ProposalStale {
			return fmt.Errorf("proposal %s is %s already: %w", id, p.State, store.ErrConflict)
		}
		severity, _, err := w.maxSeverity(p.Pipeline, p.Insights)
		if err != nil {
			return err
		}
		if _, err := w.exec(`UPDATE proposals SET decided_by = ?, decided_at = ?, reason = ?, rejected_severity = ?
			WHERE id = ?`, by, w.ts(), reason, severity, id); err != nil {
			return err
		}
		if err := w.setProposalState(p.ID, p.Pipeline, store.ProposalRejected, store.EventProposalRejected,
			map[string]any{"by": by}); err != nil {
			return err
		}
		out, err = w.proposal(id)
		return err
	})
	if err != nil {
		return store.Proposal{}, fmt.Errorf("reject proposal: %w", err)
	}
	return out, nil
}

// appliedProposals records proposalApplied for each accepted proposal of the pipeline a new revision carries whose
// base is parent, the revision active when the unit runs: one applied over another revision is left out.
func (w *wtx) appliedProposals(pipelineName string, rev store.RevisionID, hash, parent string,
	applied []string,
) error {
	for _, id := range applied {
		var n int
		if err := w.queryRow(`SELECT COUNT(*) FROM proposals WHERE id = ? AND pipeline = ? AND state = ?
			AND base_revision = ?`, id, pipelineName, store.ProposalAccepted, parent).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			continue
		}
		if err := w.event(pipelineName, "", store.EventProposalApplied, map[string]any{"id": id, "revision": rev,
			"packageHash": hash, "parent": parent}); err != nil {
			return err
		}
	}
	return nil
}

// RecordPublished records an accepted proposal's publication: see store.Store.
func (s *Store) RecordPublished(ctx context.Context, id, publisher, where string) error {
	if id == "" || publisher == "" {
		return fmt.Errorf("record published: a proposal and a publisher: %w", store.ErrInvalid)
	}
	err := s.submit(ctx, unitRecordPublished, func(w *wtx) error {
		p, err := w.proposal(id)
		if err != nil {
			return err
		}
		if p.State != store.ProposalAccepted {
			return fmt.Errorf("proposal %s is %s: only an accepted one is published: %w", id, p.State, store.ErrConflict)
		}
		return w.event(p.Pipeline, "", store.EventProposalPublished, map[string]any{"id": id, "publisher": publisher,
			"where": where})
	})
	if err != nil {
		return fmt.Errorf("record published: %w", err)
	}
	return nil
}

// SetProposalToken replaces the proposals-only token's hash.
func (s *Store) SetProposalToken(ctx context.Context, hash string) error {
	if hash == "" {
		return fmt.Errorf("set proposal token: no hash: %w", store.ErrInvalid)
	}
	return s.submit(ctx, unitSetProposalToken, func(w *wtx) error {
		_, err := w.exec(`INSERT INTO proposal_token (id, hash, created_at) VALUES (1, ?, ?)
			ON CONFLICT (id) DO UPDATE SET hash = excluded.hash, created_at = excluded.created_at`, hash, w.ts())
		return err
	})
}

// ProposalTokenHash returns the proposals-only token's hash.
func (s *Store) ProposalTokenHash(ctx context.Context) (string, error) {
	var hash string
	err := s.read.Row(ctx, `SELECT hash FROM proposal_token WHERE id = 1`).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("the proposal token: %w", store.ErrNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("the proposal token: %w", err)
	}
	return hash, nil
}

// Proposal returns a proposal.
func (s *Store) Proposal(ctx context.Context, id string) (store.Proposal, error) {
	p, err := scanProposal(s.read.Row(ctx, `SELECT `+proposalColumns+` FROM proposals WHERE id = ?`, id))
	if err != nil {
		return p, notFound(err, "proposal "+id)
	}
	return p, nil
}

// Proposals returns the proposals f selects, newest first.
func (s *Store) Proposals(ctx context.Context, f store.ProposalFilter) ([]store.Proposal, error) {
	var w where
	w.eq("pipeline", f.Pipeline)
	w.eq("state", string(f.State))
	out, err := scan(ctx, s.read, `SELECT `+proposalColumns+` FROM proposals`+w.String()+
		` ORDER BY created_at DESC, id DESC`, w.args, func(rows *sql.Rows) (store.Proposal, error) {
		return scanProposal(rows)
	})
	if err != nil {
		return nil, fmt.Errorf("proposals: %w", err)
	}
	return out, nil
}
