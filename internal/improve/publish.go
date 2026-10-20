package improve

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
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

// Publisher publishes an accepted proposal (spec sections 11.3 and 11.7): apply, patch-file, git-pr-gh and
// git-pr-glab register themselves under those names. The publish step calls it for each accepted proposal of its
// run, and on the weekly run for every accepted proposal, so a publisher must recognise one it published already
// (by its id) and answer it as published again, not publish it twice. It must refuse a patch whose SHA-256 is not
// PatchSHA256: the reviewed patch is the published one.
type Publisher interface {
	Publish(ctx context.Context, req PublishRequest) (PublishResult, error)
}

// PublishRequest is an accepted proposal to publish, as the API answers it, and the publish step's config.
type PublishRequest struct {
	Proposal api.Proposal
	// Publisher is improve.publish's value, the name the publisher was found under.
	Publisher string
	// API and Token reach the API with the proposals-only token, which cannot accept, reject or apply.
	API, Token string
}

// PublishResult is what a publisher did: Location is where the proposal went (a revision, a file, a pull request).
type PublishResult struct {
	Published bool
	Location  string
	Message   string
}

var (
	publishersMu sync.Mutex
	publishers   = map[string]Publisher{}
)

// RegisterPublisher registers a publisher under the name improve.publish gives it.
func RegisterPublisher(name string, p Publisher) {
	publishersMu.Lock()
	defer publishersMu.Unlock()
	publishers[name] = p
}

// publisher returns the publisher of a name: from Options.Publishers when set, else those registered.
func (a *action) publisher(name string) (Publisher, bool) {
	if a.opts.Publishers != nil {
		p, ok := a.opts.Publishers[name]
		return p, ok
	}
	publishersMu.Lock()
	defer publishersMu.Unlock()
	p, ok := publishers[name]
	return p, ok
}

// publish hands accepted proposals to the publisher improve.publish names: the proposal of an outcome when it is
// accepted, and on a weekly trigger item every accepted proposal. Without a publisher (improve.publish unset, or
// a publisher this build lacks) an accepted proposal stays accepted, unpublished, and says why.
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

// publishOne hands one accepted proposal to the publisher.
func (a *action) publishOne(ctx context.Context, p api.Proposal) (Outcome, error) {
	o := Outcome{Outcome: OutcomeProposed, Pipeline: p.Pipeline, Kind: p.Kind, Proposal: p.ID, State: p.State,
		Rationale: p.Rationale, Insights: p.Insights}
	name := a.cfg.Publisher
	pub, ok := a.publisher(name)
	switch {
	case name == "":
		o.Message = "improve.publish is not set: the proposal stays accepted, unpublished"
		return o, nil
	case !ok:
		o.Message = fmt.Sprintf("this build has no publisher %q: the proposal stays accepted, unpublished", name)
		return o, nil
	}
	res, err := pub.Publish(ctx, PublishRequest{Proposal: p, Publisher: name, API: a.cfg.API, Token: a.cfg.Token})
	if err != nil {
		return o, fmt.Errorf("publish %s through %s: %w", p.ID, name, err)
	}
	o.Published, o.Location, o.Message = res.Published, res.Location, cmp.Or(res.Message, o.Message)
	return o, nil
}

// The publish actions run in the engine, behind POST /proposals/{id}/publish (spec section 11.7): the publish step
// reaches them by name through routePublisher, which every name of pipeline.PublishActions maps to.
func init() {
	for _, name := range pipeline.PublishActions {
		RegisterPublisher(name, routePublisher{})
	}
}

// routePublisher hands an accepted proposal to the engine's publish action, which publishes it once and, asked
// again, answers where it went.
type routePublisher struct {
	http *http.Client // nil means http.DefaultClient
}

// Publish implements Publisher.
func (r routePublisher) Publish(ctx context.Context, req PublishRequest) (PublishResult, error) {
	p := req.Proposal
	if got := patch.Hash([]byte(p.Patch)); got != p.PatchSHA256 {
		return PublishResult{}, fmt.Errorf("proposal %s: its patch hashes to %.12s, not the validated %.12s", p.ID,
			got, p.PatchSHA256)
	}
	c := &client{base: strings.TrimSuffix(req.API, "/") + api.Prefix, token: req.Token,
		http: cmp.Or(r.http, http.DefaultClient)}
	var out api.PublishedProposal
	if err := c.do(ctx, http.MethodPost, "/proposals/"+url.PathEscape(p.ID)+"/publish",
		api.PublishRequest{PatchSHA256: p.PatchSHA256}, &out); err != nil {
		return PublishResult{}, err
	}
	msg := "published by " + out.Publisher
	if out.Again {
		msg = "published already by " + out.Publisher
	}
	return PublishResult{Published: true, Location: out.Location, Message: msg}, nil
}
