package improve

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/protocol"
)

// Draft is the draft agent's answer, the contract of its output schema: the kind (none when the evidence supports
// no change), the rationale a reviewer reads first, the insights it answers, and the change, as the whole new
// content of each file it changes (Files) or as a patch in the internal/patch diff format against the base revision.
type Draft struct {
	Kind      string      `json:"kind"`
	Rationale string      `json:"rationale"`
	Insights  []string    `json:"insights"`
	Files     []DraftFile `json:"files"`
	Patch     string      `json:"patch"`
}

// DraftFile is a file a draft changes, with its whole new content.
type DraftFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// Outcome is validate's output, and what propose and publish pass on: what became of a draft.
type Outcome struct {
	// Outcome is proposed (created; State says whether it passed its validation), suppressed (people rejected the idea and its evidence has not
	// changed enough), refused (the patch breaks its kind's rules) or skipped (the draft proposes nothing).
	Outcome   string   `json:"outcome"`
	Pipeline  string   `json:"pipeline,omitempty"`
	Kind      string   `json:"kind,omitempty"`
	Proposal  string   `json:"proposal,omitempty"`
	State     string   `json:"state,omitempty"`
	Message   string   `json:"message,omitempty"`
	Rationale string   `json:"rationale,omitempty"`
	Insights  []string `json:"insights,omitempty"`
	// Published and Location are publish's: whether a publisher took the proposal, and where it put it.
	Published bool   `json:"published,omitempty"`
	Location  string `json:"location,omitempty"`
}

// Outcomes of validate.
const (
	OutcomeProposed   = "proposed"
	OutcomeSuppressed = "suppressed"
	OutcomeRefused    = "refused"
	OutcomeSkipped    = "skipped"
)

// validate answers a draft: its evidence is read back through the API (the draft's parent item), the draft becomes
// a patch against the evidence's base revision, and the proposal is created, which the engine validates (check,
// evals, a backtest holding a filter to improve.filter.maxFalseDrop) before it answers.
func validate(ctx context.Context, a *action, it protocol.Item) ([]output, error) {
	var d Draft
	if err := decode(it, &d); err != nil {
		return nil, err
	}
	ev, err := a.evidenceOf(ctx, it.ID)
	if err != nil {
		return nil, err
	}
	o, err := a.propose(ctx, ev, d)
	if err != nil {
		return nil, err
	}
	o.Pipeline, o.Kind, o.Rationale = ev.Pipeline, d.Kind, d.Rationale
	return []output{{Key: it.Key, Payload: o}}, nil
}

// propose creates the proposal a draft makes, and says what became of it.
func (a *action) propose(ctx context.Context, ev Evidence, d Draft) (Outcome, error) {
	switch {
	case d.Kind == "none":
		return Outcome{Outcome: OutcomeSkipped, Message: "the draft proposes no change"}, nil
	case d.Kind != ev.Kind:
		return Outcome{Outcome: OutcomeRefused, Message: fmt.Sprintf("a %s draft for a %s idea", d.Kind, ev.Kind)}, nil
	}
	text, err := a.patchOf(ctx, ev, d)
	if err != nil {
		return Outcome{Outcome: OutcomeRefused, Message: err.Error()}, nil
	}
	insights := slices.Clone(ev.Insights)
	for _, id := range d.Insights {
		if !slices.Contains(insights, id) {
			insights = append(insights, id)
		}
	}
	var p api.Proposal
	err = a.api.do(ctx, http.MethodPost, "/proposals", api.ProposalRequest{Pipeline: ev.Pipeline, Kind: d.Kind,
		Patch: text, Rationale: d.Rationale, Insights: insights, Fingerprint: ev.Fingerprint}, &p)
	var ae *apiError
	switch {
	case errors.As(err, &ae) && ae.status == http.StatusConflict:
		return Outcome{Outcome: OutcomeSuppressed, Message: ae.msg, Insights: insights}, nil
	case errors.As(err, &ae) && ae.status == http.StatusBadRequest:
		return Outcome{Outcome: OutcomeRefused, Message: ae.msg, Insights: insights}, nil
	case err != nil:
		return Outcome{}, err
	}
	return Outcome{Outcome: OutcomeProposed, Proposal: p.ID, State: p.State, Insights: insights}, nil
}

// evidenceOf reads back the evidence a draft was made from: the parent of the draft's item, in this pipeline.
func (a *action) evidenceOf(ctx context.Context, item string) (Evidence, error) {
	var ev Evidence
	self := cmp.Or(a.cfg.Pipeline, Name)
	it, err := a.api.item(ctx, self, item)
	if err != nil {
		return ev, err
	}
	if len(it.Parents) != 1 {
		return ev, fmt.Errorf("draft %s has %d parents, want its evidence", item, len(it.Parents))
	}
	parent, err := a.api.item(ctx, self, it.Parents[0])
	if err != nil {
		return ev, err
	}
	if err := json.Unmarshal(parent.Payload, &ev); err != nil {
		return ev, fmt.Errorf("evidence %s: %w", parent.ID, err)
	}
	return ev, nil
}

// patchOf returns the draft's change as a patch against the evidence's base revision: its patch as given, or the
// diff of its files against the base's, with the base's Millstream-Base trailer.
func (a *action) patchOf(ctx context.Context, ev Evidence, d Draft) (string, error) {
	if d.Patch != "" {
		return d.Patch, nil
	}
	if len(d.Files) == 0 {
		return "", errors.New("the draft changes no file")
	}
	files, err := a.api.files(ctx, ev.Pipeline, 0)
	if err != nil {
		return "", err
	}
	if files.PackageHash != ev.BaseRevision {
		return "", fmt.Errorf("the active revision of %s moved from %.12s to %.12s since the evidence was gathered",
			ev.Pipeline, ev.BaseRevision, files.PackageHash)
	}
	before, after := map[string][]byte{}, map[string][]byte{}
	for _, f := range d.Files {
		if err := patch.CheckPath(f.Path); err != nil {
			return "", err
		}
		if old, ok := files.Files[f.Path]; ok {
			before[f.Path] = []byte(old)
		} else if slices.Contains(files.Binary, f.Path) {
			return "", fmt.Errorf("%s is not a text file", f.Path)
		}
		after[f.Path] = []byte(f.Content)
	}
	if maps.EqualFunc(before, after, func(x, y []byte) bool { return string(x) == string(y) }) {
		return "", errors.New("the draft's files are the base revision's")
	}
	text, err := patch.Format(ev.BaseRevision, before, after)
	return string(text), err
}
