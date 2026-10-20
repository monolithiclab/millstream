// Package distill holds the rules of the distill proposal (spec section 11.5) that the improve package and the
// engine share: the input and output pairs of an agent step, their split into a training set and a holdout set by
// time and by entity, the lesson examples the holdout withholds, and the thresholds the candidate code meets on the
// holdout.
//
// The split is the engine's: it serves the training set (GET .../steps/{step}/distill-training) and never the
// holdout, and validates a distill proposal on the holdout it recomputes from the boundary the proposal records
// (HoldoutFrom). An entity is every pair that shares an input item, an item key or a lineage root: the retries and
// dead-letter replays of an item, and the later items of the same key, stay on its side. Entities fall on the
// holdout side by when they were first seen, so pairs recorded after the split only ever join the holdout, unless
// their entity was already in training.
package distill

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// HoldoutShare is the share of the pairs the most recently first-seen entities make up, the holdout.
const HoldoutShare = 0.5

// The thresholds' defaults (improve.distill, spec section 14).
const (
	DefaultMinAgreement = 0.98
	DefaultMinCoverage  = 0.6
	// MinCovered is the fewest holdout items the code must cover for its agreement to count.
	MinCovered = 200
)

// Pair is one delivery of the agent step: its input and the truth, the reviewer's edit of the agent's output where
// there is one, else the agent's output.
type Pair struct {
	// Item is the input item's id, Key its key, Root its lineage's trigger item.
	Item string `json:"-"`
	Key  string `json:"key"`
	Root string `json:"-"`
	// At is when the input item was published.
	At      time.Time       `json:"-"`
	Input   json.RawMessage `json:"input"`
	Outputs []Output        `json:"outputs"`
	// Edited says a reviewer's edit is part of the truth; Reviews are those reviews' delivery ids.
	Edited  bool     `json:"edited,omitempty"`
	Reviews []string `json:"-"`
	// CostUSD is what the agent spent on the delivery.
	CostUSD float64 `json:"-"`
}

// Output is one output of a pair: its key and payload.
type Output struct {
	Key     string          `json:"key,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// Split is a step's pairs split by entity and time: Holdout the entities first seen at or after HoldoutFrom.
type Split struct {
	Train, Holdout []Pair
	HoldoutFrom    time.Time
}

// ErrNoSplit is returned when the pairs cannot be split: too few entities, or all first seen at the same instant.
var ErrNoSplit = errors.New("the step's history cannot be split into a training and a holdout set")

// SplitPairs splits pairs by entity and time: the most recently first-seen entities making up at least share of the
// pairs are the holdout, and every entity first seen at the same instant as the oldest of them too.
func SplitPairs(pairs []Pair, share float64) (Split, error) {
	ents := entities(pairs)
	slices.SortStableFunc(ents, func(a, b entity) int { return b.first.Compare(a.first) }) // newest first
	want := share * float64(len(pairs))
	var from time.Time
	n := 0
	for _, e := range ents {
		if float64(n) >= want && e.first.Before(from) {
			break
		}
		n += len(e.pairs)
		from = e.first
	}
	s := Split{HoldoutFrom: from}
	s.Train, s.Holdout = partition(pairs, ents, from)
	if len(s.Train) == 0 || len(s.Holdout) == 0 {
		return Split{}, ErrNoSplit
	}
	return s, nil
}

// Holdout returns the pairs of the entities first seen at or after from: the holdout of a split made at from,
// together with the entities first seen since.
func Holdout(pairs []Pair, from time.Time) []Pair {
	_, holdout := partition(pairs, entities(pairs), from)
	return holdout
}

// partition splits pairs by their entity's first sighting against from.
func partition(pairs []Pair, ents []entity, from time.Time) (train, holdout []Pair) {
	for _, e := range ents {
		for _, i := range e.pairs {
			if e.first.Before(from) {
				train = append(train, pairs[i])
			} else {
				holdout = append(holdout, pairs[i])
			}
		}
	}
	return train, holdout
}

// entity is a set of pairs (their indexes) sharing an item, a key or a root, with when it was first seen.
type entity struct {
	pairs []int
	first time.Time
}

// entities groups pairs into entities, in the order of their first pair.
func entities(pairs []Pair) []entity {
	parent := make([]int, len(pairs))
	for i := range parent {
		parent[i] = i
	}
	find := func(i int) int {
		for parent[i] != i {
			parent[i] = parent[parent[i]]
			i = parent[i]
		}
		return i
	}
	owner := map[string]int{}
	for i, p := range pairs {
		for _, tok := range []string{"i\x00" + p.Item, "k\x00" + p.Key, "r\x00" + p.Root} {
			if strings.HasSuffix(tok, "\x00") {
				continue // no key, or no root
			}
			if j, ok := owner[tok]; ok {
				parent[find(i)] = find(j)
			} else {
				owner[tok] = i
			}
		}
	}
	byRoot := map[int]*entity{}
	var order []int
	for i, p := range pairs {
		r := find(i)
		e := byRoot[r]
		if e == nil {
			e = &entity{first: p.At}
			byRoot[r] = e
			order = append(order, r)
		}
		e.pairs = append(e.pairs, i)
		if p.At.Before(e.first) {
			e.first = p.At
		}
	}
	out := make([]entity, len(order))
	for i, r := range order {
		out[i] = *byRoot[r]
	}
	return out
}

// Withheld returns the indexes of the lesson examples derived from a holdout pair, which must not reach the drafting
// agent: an example whose source is a review of the pair's truth (review/<delivery id>) or the pair's input item
// (item/<id>), or whose input matches the pair's input on every field the example keeps.
func Withheld(examples []pipeline.LessonExample, holdout []Pair) []int {
	if len(examples) == 0 {
		return nil
	}
	sources := map[string]bool{}
	inputs := make([]any, 0, len(holdout))
	for _, p := range holdout {
		sources["item/"+p.Item] = true
		for _, r := range p.Reviews {
			sources["review/"+r] = true
		}
		if v, err := pipeline.DecodeValue(string(p.Input)); err == nil {
			inputs = append(inputs, v)
		}
	}
	var out []int
	for i, ex := range examples {
		if sources[ex.Source] {
			out = append(out, i)
			continue
		}
		in := jsonValue(ex.Input)
		if slices.ContainsFunc(inputs, func(v any) bool { return excerpt(in, v) }) {
			out = append(out, i)
		}
	}
	return out
}

// excerpt reports whether ex is an excerpt of v, both as JSON decodes them: an object whose every field v holds
// with an equal value (as excerpts, recursively), or a value equal to v. An empty object is an excerpt of nothing.
func excerpt(ex, v any) bool {
	em, ok := ex.(map[string]any)
	if !ok {
		return reflect.DeepEqual(ex, v)
	}
	vm, ok := v.(map[string]any)
	if !ok || len(em) == 0 {
		return false
	}
	for k, x := range em {
		y, ok := vm[k]
		if !ok || !excerpt(x, y) {
			return false
		}
	}
	return true
}

// jsonValue is v as pipeline.DecodeValue reads it back from JSON, so that a value read from YAML compares with a
// decoded payload.
func jsonValue(v any) any {
	data, err := json.Marshal(v)
	if err != nil {
		return v
	}
	out, err := pipeline.DecodeValue(string(data))
	if err != nil {
		return v
	}
	return out
}

// Canon is a JSON document's canonical text, object keys sorted and numbers as pipeline.DecodeValue reads them;
// what does not decode stays as it is.
func Canon(raw json.RawMessage) string {
	v, err := pipeline.DecodeValue(string(raw))
	if err != nil {
		return string(raw)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return string(raw)
	}
	return string(b)
}

// Agree reports whether the code's outputs on an item are the truth's: the same keys (an output without one takes
// the input item's key, as the engine keys it) with canonically equal payloads, in any order.
func Agree(truth, got []Output, itemKey string) bool {
	canon := func(os []Output) []string {
		out := make([]string, len(os))
		for i, o := range os {
			out[i] = cmp.Or(o.Key, itemKey) + "\x00" + Canon(o.Payload)
		}
		slices.Sort(out)
		return out
	}
	return slices.Equal(canon(truth), canon(got))
}

// Thresholds are what the code must meet on the holdout (improve.distill): agreement on the items it covers,
// coverage (the share it does not abstain on) and the covered items it takes for either to count.
type Thresholds struct {
	MinAgreement float64 `json:"minAgreement"`
	MinCoverage  float64 `json:"minCoverage"`
	MinCovered   int     `json:"minCovered"`
}

// WithDefaults fills in the defaults of the thresholds left zero.
func (t Thresholds) WithDefaults() Thresholds {
	t.MinAgreement = cmp.Or(t.MinAgreement, DefaultMinAgreement)
	t.MinCoverage = cmp.Or(t.MinCoverage, DefaultMinCoverage)
	t.MinCovered = cmp.Or(t.MinCovered, MinCovered)
	return t
}

// Result is the code's answer on one holdout item.
type Result struct {
	Item string
	// Abstained: the code declined the item, which goes to the agent. Otherwise it covered it, and Agreed says
	// whether its outputs are the truth's; an error is a covered item it got wrong.
	Abstained bool
	Agreed    bool
	Error     string
	// CostUSD is what the agent spent on the item, saved when the code covers it.
	CostUSD float64
}

// Verdict is the code's measure on the holdout against the thresholds.
type Verdict struct {
	Holdout   int     `json:"holdout"`
	Covered   int     `json:"covered"`
	Agreed    int     `json:"agreed"`
	Coverage  float64 `json:"coverage"`
	Agreement float64 `json:"agreement"`
	// AgentCostUSD is the agent's cost over the holdout, AgentCostSavedUSD over the items the code covers.
	AgentCostUSD      float64    `json:"agentCostUsd"`
	AgentCostSavedUSD float64    `json:"agentCostSavedUsd"`
	Thresholds        Thresholds `json:"thresholds"`
	Passed            bool       `json:"passed"`
	// Failures say which thresholds it missed; Disagreements name up to MaxDisagreements items it got wrong.
	Failures      []string `json:"failures"`
	Disagreements []string `json:"disagreements"`
}

// MaxDisagreements is how many disagreeing items a verdict names.
const MaxDisagreements = 10

// Evaluate measures the code's results on the holdout against the thresholds (spec section 11.5): agreement of at
// least MinAgreement on the covered items, coverage of at least MinCoverage, and at least MinCovered covered items.
func Evaluate(results []Result, t Thresholds) Verdict {
	t = t.WithDefaults()
	v := Verdict{Holdout: len(results), Thresholds: t, Failures: []string{}, Disagreements: []string{}}
	for _, r := range results {
		v.AgentCostUSD += r.CostUSD
		if r.Abstained {
			continue
		}
		v.Covered++
		v.AgentCostSavedUSD += r.CostUSD
		if r.Agreed {
			v.Agreed++
		} else if len(v.Disagreements) < MaxDisagreements {
			v.Disagreements = append(v.Disagreements, r.Item)
		}
	}
	if v.Holdout > 0 {
		v.Coverage = float64(v.Covered) / float64(v.Holdout)
	}
	if v.Covered > 0 {
		v.Agreement = float64(v.Agreed) / float64(v.Covered)
	}
	if v.Covered < t.MinCovered {
		v.Failures = append(v.Failures, "covered")
	}
	if v.Coverage < t.MinCoverage {
		v.Failures = append(v.Failures, "coverage")
	}
	if v.Agreement < t.MinAgreement {
		v.Failures = append(v.Failures, "agreement")
	}
	v.Passed = len(v.Failures) == 0
	return v
}

// Spec is what the improve package records with a distilled action, actions/<name>/distill.yaml: the step it
// replaces, the agent it keeps as fallback and the split's boundary, from which the engine recomputes the holdout.
type Spec struct {
	Step        string    `yaml:"step"`
	Agent       string    `yaml:"agent"`
	HoldoutFrom time.Time `yaml:"holdoutFrom"`
}

// SpecFile is the name of a distilled action's Spec file.
const SpecFile = "distill.yaml"

// ParseSpec reads a Spec file.
func ParseSpec(data []byte) (Spec, error) {
	var s Spec
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	err := dec.Decode(&s)
	switch {
	case err != nil:
		return s, err
	case s.Step == "" || s.Agent == "" || s.HoldoutFrom.IsZero():
		return s, errors.New("step, agent and holdoutFrom are required")
	}
	return s, nil
}

// Format writes a Spec file.
func (s Spec) Format() []byte {
	return []byte("# How this action was distilled (spec section 11.5), written by the improve pipeline: the step it\n" +
		"# replaces, the agent kept as its fallback, and the boundary of the holdout it was validated on.\n" +
		"step: " + s.Step + "\nagent: " + s.Agent + "\nholdoutFrom: " + s.HoldoutFrom.UTC().Format(time.RFC3339Nano) +
		"\n")
}
