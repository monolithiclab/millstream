package insights

import (
	"cmp"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/schema"
	"github.com/monolithiclab/millstream/internal/store"
)

// Distillation criteria (spec section 11.5), beside improve.distill.minDeliveries.
const (
	// maxTurns is the p95 of turns per delivery a candidate stays within.
	maxTurns = 2
	// minBoundedShare is the share of the output schema's fields that must be bounded or copied from the input.
	minBoundedShare = 0.75
	// minReviewAgreement is the reviewer agreement a reviewed candidate needs.
	minReviewAgreement = 0.95
	// maxShortString is the longest maxLength of a string field that still counts as bounded (a code, not prose).
	maxShortString = 64
	// monthPerWindow scales the window's cost to a month.
	monthPerWindow = float64(30*24*time.Hour) / float64(Window)
)

// distillables flags the agent steps that have become candidates for code (spec section 11.5): at least
// improve.distill.minDeliveries deliveries in the window, a bounded output schema (at least three quarters of its
// fields enums, constants, booleans, numbers with both bounds, short or formatted strings, or fields every sampled
// output copies from its input), a p95 of at most two turns per delivery and no tools or MCP servers, and, when
// people review its outputs, at least 95% agreement: decisions on items it published where nobody rejected the item
// or edited a field attributed to it. Candidates are ranked by the agent cost a month of the window's traffic
// would save; each is an info insight.
func distillables(d *Data) []store.Insight {
	type found struct {
		step string
		ev   map[string]any
		save float64
	}
	var cands []found
	for _, step := range d.candidates() {
		info := d.Steps[step]
		if info.Tools > 0 {
			continue
		}
		fields := d.boundedFields(step, info.Output)
		total := len(fields.Bounded) + len(fields.Copied) + len(fields.Free)
		if total == 0 {
			continue
		}
		bounded := float64(len(fields.Bounded)+len(fields.Copied)) / float64(total)
		turns, deliveries, cost := d.agentLoad(step)
		if bounded < minBoundedShare || len(turns) == 0 {
			continue
		}
		p95 := Percentile(turns, 0.95)
		if p95 > maxTurns {
			continue
		}
		agree, decided := d.reviewAgreement(step)
		ev := map[string]any{"deliveries": deliveries, "turnsP95": p95, "tools": 0,
			"boundedShare": round(bounded), "fields": fields, "reviewedDecisions": decided,
			"reviewerAgreement": nil, "costUsd": round6(cost), "monthlySavingUsd": round6(cost * monthPerWindow)}
		if decided > 0 {
			share := float64(agree) / float64(decided)
			if share < minReviewAgreement {
				continue
			}
			ev["reviewerAgreement"] = round(share)
		}
		cands = append(cands, found{step: step, ev: ev, save: cost})
	}
	slices.SortStableFunc(cands, func(a, b found) int { return cmp.Or(cmp.Compare(b.save, a.save), cmp.Compare(a.step, b.step)) })
	var out []store.Insight
	for i, c := range cands {
		c.ev["rank"] = i + 1
		c.ev["samples"] = nonNil(d.outputSamples(c.step))
		out = append(out, d.insight(pipeline.InsightDistillable, c.step, SeverityInfo, c.ev, c.step))
	}
	return out
}

// agentLoad returns a step's turns per done attempt in the window, its deliveries created in the window, and its
// cost over the attempts that ended in it.
func (d *Data) agentLoad(step string) (turns []float64, deliveries int, cost float64) {
	for _, dl := range d.Deliveries {
		if dl.Step == step && d.inWindow(dl.CreatedAt) {
			deliveries++
		}
	}
	for _, a := range d.Attempts {
		if a.Step != step || !d.inWindow(a.TS) {
			continue
		}
		cost += a.CostUSD
		if a.Outcome != store.EventDeliveryDone {
			continue
		}
		for _, m := range a.Metrics {
			if m.Name == agent.MetricTurns {
				turns = append(turns, m.Value)
			}
		}
	}
	return turns, deliveries, cost
}

// reviewAgreement counts the decisions by a person in the window on items step published (their nearest ancestor
// of that step), and those that agree with it: approved without an edit attributed to it.
func (d *Data) reviewAgreement(step string) (agree, decided int) {
	for _, r := range d.Decisions {
		if r.Reviewer != review.ReviewerPerson || !d.inWindow(r.TS) {
			continue
		}
		if !d.producedBy(r.Item, step) {
			continue
		}
		decided++
		ok := r.Approved()
		for _, p := range r.Edited {
			if author, _ := d.author(r.Item, p); author == step {
				ok = false
			}
		}
		if ok {
			agree++
		}
	}
	return agree, decided
}

// outputSamples returns ids of items step published that the projector holds, at most MaxSamples, in order.
func (d *Data) outputSamples(step string) []string {
	var ids []string
	for id, it := range d.Items {
		if it.Step == step {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids[:min(len(ids), MaxSamples)]
}

// fields are an output schema's leaf fields by class (boundedFields).
type fields struct {
	Bounded []string `json:"bounded"`
	Copied  []string `json:"copied"`
	Free    []string `json:"free"`
}

// boundedFields sorts the leaf fields of an output schema into bounded (by the schema), copied (an unbounded field
// every sampled output of step holding it copies from its input, at least one doing so) and free.
func (d *Data) boundedFields(step string, output []byte) fields {
	out := fields{Bounded: []string{}, Copied: []string{}, Free: []string{}}
	var root any
	if len(output) == 0 || json.Unmarshal(output, &root) != nil {
		return out
	}
	walkSchema(root, root, "", 0, func(p string, bounded bool) {
		switch {
		case bounded:
			out.Bounded = append(out.Bounded, p)
		case d.copied(step, p):
			out.Copied = append(out.Copied, p)
		default:
			out.Free = append(out.Free, p)
		}
	})
	slices.Sort(out.Bounded)
	slices.Sort(out.Copied)
	slices.Sort(out.Free)
	return out
}

// copied reports whether every sampled output of step holding the field at p has its input's value there, and at
// least one holds it.
func (d *Data) copied(step, p string) bool {
	if strings.Contains(p, "/*") {
		return false
	}
	n := 0
	for id, it := range d.Items {
		if it.Step != step || len(it.Parents) != 1 {
			continue
		}
		if _, _, present := d.value(id, p); !present {
			continue
		}
		if diff, held := d.differs(id, it.Parents[0], p); diff || !held {
			return false
		}
		n++
	}
	return n > 0
}

// walkSchema visits the leaf fields of a JSON Schema with whether the schema bounds each: objects recurse into their
// properties and arrays into their items ("/*"), local $refs are followed, and anyOf or oneOf is bounded when every
// branch is.
func walkSchema(root, s any, p string, depth int, visit func(string, bool)) {
	m, ok := s.(map[string]any)
	if !ok || depth > maxDepth/8 {
		visit(p, false)
		return
	}
	if ref, ok := m["$ref"].(string); ok {
		target, err := schema.Resolve(root, ref)
		if err != nil {
			visit(p, false)
			return
		}
		walkSchema(root, target, p, depth+1, visit)
		return
	}
	if props, ok := m["properties"].(map[string]any); ok && len(props) > 0 {
		for _, k := range slices.Sorted(maps.Keys(props)) {
			walkSchema(root, props[k], p+"/"+schema.EscapeToken(k), depth+1, visit)
		}
		return
	}
	if items, ok := m["items"]; ok && schemaType(m) == "array" {
		walkSchema(root, items, p+"/*", depth+1, visit)
		return
	}
	visit(p, boundedLeaf(root, m, depth))
}

// boundedLeaf reports whether a leaf schema bounds its values: an enum or const, a boolean, a number with a
// minimum and a maximum, a string with a format, a pattern or a short maxLength, or anyOf/oneOf of bounded leaves.
func boundedLeaf(root any, m map[string]any, depth int) bool {
	if _, ok := m["enum"]; ok {
		return true
	}
	if _, ok := m["const"]; ok {
		return true
	}
	for _, k := range []string{"anyOf", "oneOf"} {
		if branches, ok := m[k].([]any); ok && len(branches) > 0 {
			all := true
			for _, b := range branches {
				walkSchema(root, b, "", depth+1, func(_ string, bounded bool) { all = all && bounded })
			}
			return all
		}
	}
	has := func(keys ...string) bool {
		for _, k := range keys {
			if _, ok := m[k]; ok {
				return true
			}
		}
		return false
	}
	switch schemaType(m) {
	case "boolean", "null":
		return true
	case "number", "integer":
		return has("minimum", "exclusiveMinimum") && has("maximum", "exclusiveMaximum")
	case "string":
		n, ok := m["maxLength"].(float64)
		return has("format", "pattern") || ok && n <= maxShortString
	}
	return false
}

// schemaType is a schema's type, the first that is not null when it lists several.
func schemaType(m map[string]any) string {
	switch t := m["type"].(type) {
	case string:
		return t
	case []any:
		for _, x := range t {
			if s, ok := x.(string); ok && s != "null" {
				return s
			}
		}
	}
	return ""
}
