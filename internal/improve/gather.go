package improve

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"reflect"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// Evidence is gather-evidence's output and the draft agent's input: one idea for one pipeline, with what the agent
// reads (Brief) and what validate needs back (the rest).
type Evidence struct {
	Pipeline string `json:"pipeline"`
	// BaseRevision is the package hash of the revision the brief's files come from.
	BaseRevision string `json:"baseRevision"`
	Kind         string `json:"kind"`
	// Fingerprint names the idea, so that a rejection suppresses it and later drafts see the reasons.
	Fingerprint string         `json:"fingerprint"`
	Insights    []string       `json:"insights"`
	Hint        map[string]any `json:"hint,omitempty"`
	Brief       string         `json:"brief"`
	// Distill is a distill idea's step, agent, new action and holdout boundary.
	Distill *DistillEvidence `json:"distill,omitempty"`
}

// idea is a proposal an insight calls for: its kind, and the hint or pattern behind it.
type idea struct {
	insight *api.Insight // nil for a lessons file near its limit
	kind    string
	hint    map[string]any
	step    string
}

// Limits of what a brief holds.
const (
	maxExamples = 5
	// nearLimit is the share of lessons.maxTokens past which a lessons file is merged and pruned.
	nearLimit = 0.8
)

// gather answers one trigger item: an insight's run (its payload the insight, or a manual firing naming one) gathers
// that insight; a weekly run or a manual firing naming none gathers every open opportunity, every other open insight
// at or above minSeverity, and the lessons files near lessons.maxTokens. Ideas with an open proposal are left out.
func gather(ctx context.Context, a *action, it protocol.Item) ([]output, error) {
	var in struct {
		InsightID string `json:"insightId"` // an insight trigger's
		Insight   string `json:"insight"`   // a manual firing's
		Pipeline  string `json:"pipeline"`
	}
	if err := decode(it, &in); err != nil {
		return nil, err
	}
	in.InsightID = cmp.Or(in.Insight, in.InsightID)
	files := map[string]api.RevisionFiles{} // each pipeline's active revision, read once
	var ideas []idea
	if in.InsightID != "" {
		ins, err := a.api.insights(ctx, in.Pipeline, "")
		if err != nil {
			return nil, err
		}
		i := slices.IndexFunc(ins, func(x api.Insight) bool { return x.ID == in.InsightID })
		if i < 0 {
			return nil, fmt.Errorf("insight %s: no such insight", in.InsightID)
		}
		ideas = ideasOf(&ins[i])
	} else {
		var err error
		if ideas, err = a.openIdeas(ctx, in.Pipeline, files); err != nil {
			return nil, err
		}
	}
	return a.evidence(ctx, ideas, files)
}

// openIdeas are the ideas of every open opportunity and every other open insight at or above minSeverity, of one
// pipeline or of all but this one, and the lessons files near their limit. A review pattern's hints can appear after
// it opened, which records no event: this is what finds them.
func (a *action) openIdeas(ctx context.Context, only string, files map[string]api.RevisionFiles) ([]idea, error) {
	ins, err := a.api.insights(ctx, only, string(store.InsightOpen))
	if err != nil {
		return nil, err
	}
	minSeverity := cmp.Or(a.cfg.MinSeverity, pipeline.SeverityWarning)
	var ideas []idea
	for i := range ins {
		in := &ins[i]
		if in.Pipeline != a.cfg.Pipeline && (slices.Contains(pipeline.InsightOpportunities, in.Kind) ||
			pipeline.SeverityRank(in.Severity) >= pipeline.SeverityRank(minSeverity)) {
			ideas = append(ideas, ideasOf(in)...)
		}
	}
	var ps []api.Pipeline
	if err := a.api.get(ctx, "/pipelines", &ps); err != nil {
		return nil, err
	}
	for _, p := range ps {
		if (only != "" && p.Name != only) || p.Name == a.cfg.Pipeline {
			continue
		}
		f, err := a.api.files(ctx, p.Name, p.Revision)
		if err != nil {
			return nil, err
		}
		files[p.Name] = f
		ideas = append(ideas, a.lessonsNearLimit(p.Name, f)...)
	}
	return ideas, nil
}

// ideasOf are the proposals an insight calls for (spec section 11.6): a reviewPattern's hints (lessons, config for
// autoApprove and uncovered reasons, topology for a filter), and config for a failure cluster's retries or a slow
// step's timeouts and concurrency, and distill for a distillable agent step (spec section 11.5).
func ideasOf(in *api.Insight) []idea {
	switch in.Kind {
	case pipeline.InsightReviewPattern:
		var ev struct {
			Proposals []map[string]any `json:"proposals"`
		}
		_ = json.Unmarshal(in.Evidence, &ev)
		var out []idea
		for _, h := range ev.Proposals {
			kind, _ := h["kind"].(string)
			if kind == "reasons" { // new reason codes are a change of the review step's config
				kind = string(store.ProposalConfig)
			}
			step, _ := h["step"].(string)
			if after, ok := h["after"].(string); ok && kind == string(store.ProposalTopology) {
				step = after
			}
			out = append(out, idea{insight: in, kind: kind, hint: h, step: step})
		}
		return out
	case pipeline.InsightFailureCluster, pipeline.InsightSlowStep:
		return []idea{{insight: in, kind: string(store.ProposalConfig), step: in.Scope}}
	case pipeline.InsightDistillable:
		return []idea{{insight: in, kind: string(store.ProposalDistill), step: in.Scope}}
	}
	return nil
}

// lessonsNearLimit are the lessons files of a pipeline past nearLimit of lessons.maxTokens.
func (a *action) lessonsNearLimit(pipelineName string, files api.RevisionFiles) []idea {
	limit := cmp.Or(a.cfg.LessonsMaxTokens, pipeline.DefaultLessonsMaxTokens)
	var out []idea
	for _, p := range slices.Sorted(maps.Keys(files.Files)) {
		dir, file := path.Split(p)
		if dir != "lessons/" || path.Ext(file) != ".yaml" {
			continue
		}
		if tokens := pipeline.LessonsTokens([]byte(files.Files[p])); float64(tokens) >= nearLimit*float64(limit) {
			out = append(out, idea{kind: string(store.ProposalLessons), step: strings.TrimSuffix(file, ".yaml"),
				hint: map[string]any{"kind": "lessons", "pattern": "lessonsNearLimit", "path": p, "tokens": tokens,
					"maxTokens": limit, "pipeline": pipelineName}})
		}
	}
	return out
}

// evidence turns ideas into outputs, reading each pipeline's files (unless files holds them) and proposals once.
func (a *action) evidence(ctx context.Context, ideas []idea, files map[string]api.RevisionFiles) ([]output, error) {
	type pipelineData struct {
		files     api.RevisionFiles
		proposals []api.Proposal
	}
	byPipeline := map[string]*pipelineData{}
	var out []output
	for _, id := range ideas {
		name := id.pipeline()
		pd := byPipeline[name]
		if pd == nil {
			f, ok := files[name]
			if !ok {
				var err error
				if f, err = a.api.files(ctx, name, 0); err != nil {
					return nil, err
				}
			}
			ps, err := a.api.proposals(ctx, name, "")
			if err != nil {
				return nil, err
			}
			pd = &pipelineData{files: f, proposals: ps}
			byPipeline[name] = pd
		}
		fp := id.fingerprint()
		var rejected []api.Proposal
		open := false
		for _, p := range pd.proposals {
			switch {
			case p.Fingerprint != fp:
			case p.State == string(store.ProposalRejected):
				rejected = append(rejected, p)
			case p.State == string(store.ProposalDrafting) || p.State == string(store.ProposalValidating) ||
				p.State == string(store.ProposalReady):
				open = true
			}
		}
		if open {
			a.log.Info("an open proposal answers this idea already", "pipeline", name, "fingerprint", fp)
			continue
		}
		if id.kind == string(store.ProposalTopology) && !a.withinFalseDrop(id.hint) {
			a.log.Info("the filter would drop too many approvals", "pipeline", name, "hint", id.hint)
			continue
		}
		var examples []example
		if ptr, ok := id.hint["pointer"].(string); ok && id.kind == string(store.ProposalLessons) {
			var err error
			if examples, err = a.examples(ctx, id.insight, ptr); err != nil {
				return nil, err
			}
		}
		ev := Evidence{Pipeline: name, BaseRevision: pd.files.PackageHash, Kind: id.kind, Fingerprint: fp,
			Insights: []string{}, Hint: id.hint}
		if id.kind == string(store.ProposalDistill) {
			de, b, err := a.distillEvidence(ctx, id, pd.files)
			if err != nil {
				return nil, err
			}
			if de == nil {
				continue
			}
			ev.Distill, ev.Brief = de, fence(b+rejections(rejected))
		} else {
			ev.Brief = fence(brief(id, pd.files, examples, rejected))
		}
		if id.insight != nil {
			ev.Insights = []string{id.insight.ID}
		}
		out = append(out, output{Key: fp, Payload: ev})
	}
	return out, nil
}

func (id idea) pipeline() string {
	if id.insight != nil {
		return id.insight.Pipeline
	}
	name, _ := id.hint["pipeline"].(string)
	return name
}

// fingerprint names the idea: the pipeline, the kind, and what the hint says, never an id that changes from one
// window to the next.
func (id idea) fingerprint() string {
	h := sha256.New()
	enc := json.NewEncoder(h)
	_ = enc.Encode([]any{id.pipeline(), id.kind, id.step, id.hint["pattern"], id.hint["pointer"],
		id.hint["condition"], id.hint["path"]})
	if id.insight != nil && id.hint == nil {
		_ = enc.Encode(id.insight.Kind)
	}
	return "improve-" + hex.EncodeToString(h.Sum(nil))[:16]
}

// withinFalseDrop checks a filter hint against improve.filter.maxFalseDrop, as the engine's validation checks its
// backtest: the approvals it would have dropped.
func (a *action) withinFalseDrop(hint map[string]any) bool {
	falseDrops, _ := hint["falseDrops"].(float64)
	approvals, _ := hint["approvals"].(float64)
	return falseDrops <= cmp.Or(a.cfg.MaxFalseDrop, backtest.DefaultMaxFalseDrop)*approvals
}

// example is an edit a reviewer made: the item as it reached the review and the value they wrote at the pointer.
type example struct {
	Input  any `json:"input"`
	Before any `json:"before"`
	After  any `json:"after"`
}

// examples reads the edits of a reviewPattern's sampled items at pointer: each item as the review received it, and
// its output from the review, the insight's scope.
func (a *action) examples(ctx context.Context, in *api.Insight, pointer string) ([]example, error) {
	var ev struct {
		Samples []string `json:"samples"`
	}
	_ = json.Unmarshal(in.Evidence, &ev)
	var out []example
	for _, id := range ev.Samples {
		if len(out) == maxExamples {
			break
		}
		before, err := a.api.item(ctx, in.Pipeline, id)
		if err != nil {
			return nil, err
		}
		if before.Tombstone != "" {
			continue // its payload is gone
		}
		lin, err := a.api.lineage(ctx, in.Pipeline, id)
		if err != nil {
			return nil, err
		}
		i := slices.IndexFunc(lin.Down, func(n api.LineageNode) bool {
			return n.Step == in.Scope && slices.Contains(n.Parents, id)
		})
		if i < 0 {
			continue // rejected
		}
		after, err := a.api.item(ctx, in.Pipeline, lin.Down[i].ID)
		if err != nil {
			return nil, err
		}
		var b, c any
		if json.Unmarshal(before.Payload, &b) != nil || json.Unmarshal(after.Payload, &c) != nil {
			continue
		}
		bv, _ := review.Get(b, pointer)
		cv, err := review.Get(c, pointer)
		if err != nil || reflect.DeepEqual(bv, cv) {
			continue
		}
		out = append(out, example{Input: b, Before: bv, After: cv})
	}
	return out, nil
}
