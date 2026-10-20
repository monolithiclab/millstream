package improve

import (
	"context"
	"fmt"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/store"
)

// propose records an outcome of validate: a created proposal is read back, its state and rationale as the engine
// keeps them, with the insights it answers; any other outcome passes as it came.
func propose(ctx context.Context, a *action, it protocol.Item) ([]output, error) {
	var o Outcome
	if err := decode(it, &o); err != nil {
		return nil, err
	}
	if o.Proposal != "" {
		p, err := a.api.proposal(ctx, o.Proposal)
		if err != nil {
			return nil, err
		}
		o.State, o.Rationale, o.Insights, o.Kind = p.State, p.Rationale, p.Insights, p.Kind
		a.log.Info("proposed", "proposal", p.ID, "pipeline", p.Pipeline, "kind", p.Kind, "state", p.State)
	}
	return []output{{Key: it.Key, Payload: o}}, nil
}

// publish hands accepted proposals to the engine's publish action, POST /proposals/{id}/publish (spec section 11.7),
// which publishes each once as improve.publish says and, asked again, answers where it went: the proposal of an
// outcome when it is accepted, and on a weekly trigger item every accepted proposal. With improve.publish unset an
// accepted proposal stays accepted, unpublished, and says why.
func publish(ctx context.Context, a *action, it protocol.Item) ([]output, error) {
	var in struct {
		Outcome
		ScheduledAt string `json:"scheduledAt"`
	}
	if err := decode(it, &in); err != nil {
		return nil, err
	}
	if in.Outcome.Outcome != "" { // an outcome of this run
		if in.State != string(store.ProposalAccepted) {
			return []output{{Key: it.Key, Payload: in.Outcome}}, nil
		}
		p, err := a.api.proposal(ctx, in.Proposal)
		if err != nil {
			return nil, err
		}
		o, err := a.publishOne(ctx, p)
		return []output{{Key: it.Key, Payload: o}}, err
	}
	accepted, err := a.api.proposals(ctx, "", string(store.ProposalAccepted))
	if err != nil {
		return nil, err
	}
	var out []output
	for _, p := range accepted {
		o, err := a.publishOne(ctx, p)
		if err != nil {
			return nil, err
		}
		out = append(out, output{Key: p.ID, Payload: o})
	}
	return out, nil
}

// publishOne hands one accepted proposal to the engine's publish action, after checking that its patch is the
// validated one, which the engine checks again.
func (a *action) publishOne(ctx context.Context, p api.Proposal) (Outcome, error) {
	o := Outcome{Outcome: OutcomeProposed, Pipeline: p.Pipeline, Kind: p.Kind, Proposal: p.ID, State: p.State,
		Rationale: p.Rationale, Insights: p.Insights}
	if a.cfg.Publisher == "" {
		o.Message = "improve.publish is not set: the proposal stays accepted, unpublished"
		return o, nil
	}
	if got := patch.Hash([]byte(p.Patch)); got != p.PatchSHA256 {
		return o, fmt.Errorf("proposal %s: its patch hashes to %.12s, not the validated %.12s", p.ID, got,
			p.PatchSHA256)
	}
	res, err := a.api.publish(ctx, p.ID, p.PatchSHA256)
	if err != nil {
		return o, fmt.Errorf("publish %s through %s: %w", p.ID, a.cfg.Publisher, err)
	}
	o.Published, o.Location, o.Message = true, res.Location, "published by "+res.Publisher
	if res.Again {
		o.Message = "published already by " + res.Publisher
	}
	return o, nil
}
