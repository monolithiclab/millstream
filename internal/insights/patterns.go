package insights

import (
	"cmp"
	"encoding/json"
	"maps"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/monolithiclab/millstream/internal/schema"
)

// Learning from reviews (spec section 11.6): each edit a person made is attributed to the step that authored the
// field, and a review step's decisions are matched against the patterns of the spec's table, each giving a hint of
// the proposal the improve pipeline drafts.

// Pattern thresholds.
const (
	// minRecurringEdits is how many edits of one field attributed to one step make a recurring edit.
	minRecurringEdits = 3
	// minUniversal and universalShare: at least this many decisions by a person, approved without an edit at this
	// share or more, make a near-universal approval.
	minUniversal   = 20
	universalShare = 0.98
	// minUncovered is how many rejections with a reason text and no reason code make uncovered reasons.
	minUncovered = 3
	// maxFilterHints is how many filter candidates a reviewPattern lists; leafDepth how deep into an item's
	// payload they look for fields.
	maxFilterHints = 3
	leafDepth      = 3
)

// attribution is a line of a reviewPattern's attribution: edits of a pointer attributed to a step, nil when
// unattributed.
type attribution struct {
	Pointer string  `json:"pointer"`
	Step    *string `json:"step"`
	Edits   int     `json:"edits"`
}

// attribute counts the edits of approvals by the pointer and the step attributed with each (spec section 11.6),
// most edits first, and the unattributed ones.
func (d *Data) attribute(people []Decision) (all []attribution, unattributed int) {
	edits := map[[2]string]int{} // pointer and step, empty when unattributed
	for _, r := range people {
		if !r.Approved() {
			continue
		}
		for _, p := range r.Edited {
			step, ok := d.author(r.Item, p)
			edits[[2]string{p, step}]++
			if !ok {
				unattributed++
			}
		}
	}
	for _, k := range slices.SortedFunc(maps.Keys(edits), func(a, b [2]string) int {
		return cmp.Or(cmp.Compare(edits[b], edits[a]), cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1]))
	}) {
		a := attribution{Pointer: k[0], Edits: edits[k]}
		if k[1] != "" {
			a.Step = &k[1]
		}
		all = append(all, a)
	}
	return all, unattributed
}

// hint is a proposal hint of a reviewPattern: the kind of proposal a pattern calls for and its figures.
type hint map[string]any

// hints matches a review step's decisions by a person against the patterns of spec section 11.6, in the table's
// order: filter candidates (topology), recurring edits (lessons), near-universal approval (config) and
// uncovered free-text reasons (reasons).
func (d *Data) hints(step string, people []Decision, edits []attribution) []hint {
	out := d.filterHints(people)
	for _, a := range edits {
		if a.Step != nil && a.Edits >= minRecurringEdits {
			out = append(out, hint{"kind": "lessons", "pattern": "recurringEdits", "step": *a.Step,
				"pointer": a.Pointer, "edits": a.Edits})
		}
	}
	clean, uncovered := 0, 0
	for _, r := range people {
		switch {
		case r.Approved() && len(r.Edited) == 0:
			clean++
		case !r.Approved() && r.Reason == "" && r.Text:
			uncovered++
		}
	}
	if n := len(people); n >= minUniversal && float64(clean)/float64(n) >= universalShare {
		out = append(out, hint{"kind": "config", "pattern": "nearUniversalApproval", "step": step,
			"decisions": n, "approvalRate": round(float64(clean) / float64(n))})
	}
	if uncovered >= minUncovered {
		out = append(out, hint{"kind": "reasons", "pattern": "uncoveredReasons", "step": step,
			"rejections": uncovered})
	}
	if out == nil {
		return []hint{}
	}
	return out
}

// verdict is a decided item's payload and whether a person approved it.
type verdict struct {
	item     string
	doc      any
	approved bool
}

// filterHints finds rejections predictable from one field of the decided items (spec section 11.6): a value most
// rejected items hold and at most improve.filter.maxFalseDrop of approved items do, or a number beyond every
// approved item's. Each candidate is a filter's CEL condition keeping what it would not drop, with the step whose
// output the field comes from (after: null when unattributed) and the costliest step after it on the items' lineage
// (before), which the filter would spare. Only fields every decided item holds are considered, so the condition
// never reads a missing field.
func (d *Data) filterHints(people []Decision) []hint {
	var vs []verdict
	approvals, rejections := 0, 0
	for _, r := range people {
		doc, held, _ := d.value(r.Item, "")
		if !held {
			continue
		}
		vs = append(vs, verdict{item: r.Item, doc: doc, approved: r.Approved()})
		if r.Approved() {
			approvals++
		} else {
			rejections++
		}
	}
	if approvals == 0 || rejections < minSamples {
		return nil
	}
	allowed := int(math.Floor(d.FilterMaxFalseDrop * float64(approvals)))
	type found struct {
		pointer string
		candidate
	}
	var cands []found
	for _, p := range commonLeaves(vs) {
		values := make([]any, len(vs))
		for i, v := range vs {
			values[i], _, _ = d.value(v.item, p)
		}
		for _, c := range conditions(p, vs, values, allowed) {
			if c.dropped >= minSamples {
				cands = append(cands, found{p, c})
			}
		}
	}
	slices.SortStableFunc(cands, func(a, b found) int {
		return cmp.Or(cmp.Compare(b.dropped, a.dropped), cmp.Compare(a.falseDrops, b.falseDrops),
			cmp.Compare(a.expr, b.expr))
	})
	spent := d.stepCosts()
	var out []hint
	for _, c := range cands[:min(len(cands), maxFilterHints)] {
		after, before := d.placement(c.first, c.pointer, spent)
		out = append(out, hint{"kind": "topology", "pattern": "predictableRejections", "pointer": c.pointer,
			"condition": c.expr, "after": after, "before": before, "rejections": c.dropped,
			"falseDrops": c.falseDrops, "approvals": approvals})
	}
	return out
}

// candidate is a filter condition and what it would have dropped of the decided items.
type candidate struct {
	expr                string
	dropped, falseDrops int
	first               string // a rejected item it drops
}

// conditions returns the filter conditions on the field at p: for a string or boolean, keeping the items without one
// value (when at most allowed approved items hold it); for a number, keeping those at most (or at least) every
// approved item's value.
func conditions(p string, vs []verdict, values []any, allowed int) []candidate {
	field := celField(p)
	var out []candidate
	if _, ok := values[0].(float64); ok {
		hi, lo := math.Inf(-1), math.Inf(1)
		for i, v := range vs {
			if x, ok := values[i].(float64); ok && v.approved {
				hi, lo = max(hi, x), min(lo, x)
			}
		}
		if math.IsInf(hi, 0) {
			return nil // no approved item holds a number there
		}
		above := candidate{expr: field + " <= " + celNumber(hi)}
		below := candidate{expr: field + " >= " + celNumber(lo)}
		for i, v := range vs {
			x, ok := values[i].(float64)
			switch {
			case !ok || v.approved:
			case x > hi:
				above.dropped, above.first = above.dropped+1, cmp.Or(above.first, v.item)
			case x < lo:
				below.dropped, below.first = below.dropped+1, cmp.Or(below.first, v.item)
			}
		}
		return append(out, above, below)
	}
	type tally struct {
		rejected, approved int
		first              string
	}
	byValue := map[string]*tally{}
	for i, v := range vs {
		lit, ok := celLiteral(values[i])
		if !ok {
			return nil // mixed types: no condition holds for every item
		}
		t := byValue[lit]
		if t == nil {
			t = &tally{}
			byValue[lit] = t
		}
		if v.approved {
			t.approved++
		} else {
			t.rejected++
			t.first = cmp.Or(t.first, v.item)
		}
	}
	for _, lit := range slices.Sorted(maps.Keys(byValue)) {
		if t := byValue[lit]; t.approved <= allowed {
			out = append(out, candidate{expr: field + " != " + lit, dropped: t.rejected, falseDrops: t.approved,
				first: t.first})
		}
	}
	return out
}

// placement returns where a filter on the field at p of item id would go: after the step the field comes from (nil
// when unattributed), and before the costliest step between it and the item on its lineage (nil when none spent).
func (d *Data) placement(id, p string, spent map[string]float64) (after, before any) {
	author, ok := d.author(id, p)
	if ok {
		after = author
	}
	best := 0.0
	for _, it := range d.lineage(id) {
		if ok && it.Step == author {
			break
		}
		if c := spent[it.Step]; c > best && d.Steps[it.Step].Builtin == "" {
			best, before = c, it.Step
		}
	}
	return after, before
}

// stepCosts is each step's cost over the deliveries that finished in the window.
func (d *Data) stepCosts() map[string]float64 {
	out := map[string]float64{}
	for _, dl := range d.Deliveries {
		if dl.CostUSD > 0 && d.inWindow(dl.FinishedAt) {
			out[dl.Step] += dl.CostUSD
		}
	}
	return out
}

// commonLeaves returns the pointers of the scalar fields, at most leafDepth deep, that every verdict's payload holds,
// in order.
func commonLeaves(vs []verdict) []string {
	count := map[string]int{}
	for _, v := range vs {
		walkLeaves(v.doc, "", 0, func(p string) { count[p]++ })
	}
	var out []string
	for p, n := range count {
		if n == len(vs) {
			out = append(out, p)
		}
	}
	slices.Sort(out)
	return out
}

func walkLeaves(doc any, prefix string, depth int, visit func(string)) {
	m, ok := doc.(map[string]any)
	if !ok || depth >= leafDepth {
		return
	}
	for k, v := range m {
		p := prefix + "/" + schema.EscapeToken(k)
		switch v.(type) {
		case string, bool, float64:
			visit(p)
		case map[string]any:
			walkLeaves(v, p, depth+1, visit)
		}
	}
}

// celIdent is a field name CEL reads with a dot; celReserved are the words it does not.
var (
	celIdent    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	celReserved = map[string]bool{"in": true, "as": true, "break": true, "const": true, "continue": true,
		"else": true, "for": true, "function": true, "if": true, "import": true, "let": true, "loop": true,
		"package": true, "namespace": true, "return": true, "var": true, "void": true, "while": true, "true": true,
		"false": true, "null": true}
)

// celField returns the CEL selecting the field at pointer p of a filter's item.
func celField(p string) string {
	var b strings.Builder
	b.WriteString("item")
	for _, t := range strings.Split(p, "/")[1:] {
		name := schema.UnescapeToken(t)
		if celIdent.MatchString(name) && !celReserved[name] {
			b.WriteString("." + name)
		} else {
			b.WriteString("[" + strconv.Quote(name) + "]")
		}
	}
	return b.String()
}

// celLiteral returns a string or a boolean as a CEL literal.
func celLiteral(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		b, _ := json.Marshal(x) // a JSON string is a valid CEL string literal
		return string(b), true
	case bool:
		return strconv.FormatBool(x), true
	}
	return "", false
}

// celNumber returns a number as a CEL double literal: JSON numbers are doubles in CEL.
func celNumber(x float64) string {
	s := strconv.FormatFloat(x, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}
