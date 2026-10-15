package schema

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Finding severities.
const (
	SeverityError   = "error"
	SeverityWarning = "warning"
)

// Finding is one result of a compatibility check. Pointer is a JSON pointer into the consumer schema, or into
// the producer schema for a keyword the producer uses outside the checked subset; the message names the side.
type Finding struct {
	Severity string
	Pointer  string
	Message  string
}

// Compatible checks that every value valid against the producer schema is valid against the consumer schema,
// within the subset of spec section 4.11:
//
//   - every property the consumer requires is declared and required by the producer;
//   - the producer's types are a subset of the consumer's, integer counting as number, null only where the
//     consumer allows it;
//   - a producer enum or const is a subset of the consumer's enum or const;
//   - objects and arrays are checked recursively, allOf is merged and same-document $ref is followed, recursion
//     included; extra producer properties are allowed.
//
// What cannot be proven (an unconstrained producer value against a constrained consumer) is a warning. A keyword
// outside the subset makes its subtree not statically checked, listed as a warning. Remote $ref and unreadable
// schemas are errors. No finding means compatible.
func Compatible(producer, consumer []byte) []Finding {
	c := &checker{visited: map[string]bool{}}
	var ok bool
	if c.prod, ok = c.load("producer", producer); !ok {
		return c.findings
	}
	if c.cons, ok = c.load("consumer", consumer); !ok {
		return c.findings
	}
	c.compare(newView(c.prod, ref{c.prod, ""}), newView(c.cons, ref{c.cons, ""}))
	return c.sorted()
}

type checker struct {
	prod, cons any
	visited    map[string]bool
	findings   []Finding
}

func (c *checker) add(severity, ptr, format string, args ...any) {
	c.findings = append(c.findings, Finding{Severity: severity, Pointer: ptr, Message: fmt.Sprintf(format, args...)})
}

// load parses one side's document and refuses references outside it.
func (c *checker) load(side string, doc []byte) (any, bool) {
	v, err := parse(doc)
	if err != nil {
		c.add(SeverityError, "", "%s schema: %v", side, err)
		return nil, false
	}
	return v, true
}

// compare checks one position: p is the producer's view of it, cv the consumer's.
func (c *checker) compare(p, cv *view) {
	key := p.key() + "\x01" + cv.key()
	if c.visited[key] {
		return // recursion through $ref: this pair is already being checked
	}
	c.visited[key] = true
	where := cv.loc()

	for _, b := range p.broken {
		c.add(SeverityError, where, "producer schema: %s", b)
	}
	for _, b := range cv.broken {
		c.add(SeverityError, where, "consumer schema: %s", b)
	}
	if len(p.outside) > 0 || len(cv.outside) > 0 {
		for _, at := range p.outside {
			c.add(SeverityWarning, at, "producer schema: %s is outside the checked subset; this value is not statically checked", lastToken(at))
		}
		for _, at := range cv.outside {
			c.add(SeverityWarning, at, "consumer schema: %s is outside the checked subset; this value is not statically checked", lastToken(at))
		}
		return
	}
	if len(p.broken) > 0 || len(cv.broken) > 0 || p.never {
		return // nothing more can be said, or the producer emits nothing here
	}
	if cv.never {
		c.add(SeverityError, where, "the consumer accepts no value here")
		return
	}

	pt, ct := p.effectiveTypes(), cv.effectiveTypes()
	c.compareTypes(where, pt, ct)
	c.compareEnums(where, p, cv)
	if admits(pt, "object") && admits(ct, "object") {
		c.compareObjects(p, cv)
	}
	if admits(pt, "array") && admits(ct, "array") && cv.items != nil {
		pItems := p.items
		if pItems == nil {
			pItems = []ref{{true, p.loc() + "/items"}}
		}
		c.compare(newView(c.prod, pItems...), newView(c.cons, cv.items...))
	}
}

func (c *checker) compareTypes(where string, pt, ct map[string]bool) {
	if ct == nil {
		return
	}
	if pt == nil {
		c.add(SeverityWarning, where, "cannot prove the type: the producer does not constrain it, the consumer accepts %s",
			typeList(ct))
		return
	}
	for _, t := range slices.Sorted(maps.Keys(pt)) {
		switch {
		case ct[t], t == "integer" && ct["number"]:
		case t == "null":
			c.add(SeverityError, where, "the producer may emit null, which the consumer does not allow")
		default:
			c.add(SeverityError, where, "the producer may emit %s, the consumer accepts %s", t, typeList(ct))
		}
	}
}

func (c *checker) compareEnums(where string, p, cv *view) {
	if !cv.hasEnum {
		return
	}
	if !p.hasEnum {
		c.add(SeverityWarning, where, "cannot prove the value is one of the consumer's enum %s: the producer does not constrain it",
			valueList(cv.enum))
		return
	}
	var extra []any
	for _, e := range p.enum {
		if !containsValue(cv.enum, e) {
			extra = append(extra, e)
		}
	}
	if len(extra) > 0 {
		c.add(SeverityError, where, "the producer may emit %s, not in the consumer's enum %s", valueList(extra),
			valueList(cv.enum))
	}
}

func (c *checker) compareObjects(p, cv *view) {
	for _, name := range slices.Sorted(maps.Keys(cv.required)) {
		at := cv.loc() + "/properties/" + escape(name)
		_, declared := p.props[name]
		switch {
		case p.required[name]:
		case declared:
			c.add(SeverityError, at, "the consumer requires property %q, which the producer declares but does not require", name)
		default:
			c.add(SeverityError, at, "the consumer requires property %q, which the producer does not declare", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(cv.props)) {
		pp, declared := p.props[name]
		if !declared && p.required[name] {
			// Required without a schema: always present, of any type.
			pp, declared = []ref{{true, p.loc() + "/properties/" + escape(name)}}, true
		}
		if declared {
			c.compare(newView(c.prod, pp...), newView(c.cons, cv.props[name]...))
		}
	}
}

// sorted returns the findings ordered by pointer, severity and message, without duplicates.
func (c *checker) sorted() []Finding {
	slices.SortFunc(c.findings, func(a, b Finding) int {
		return cmp.Or(cmp.Compare(a.Pointer, b.Pointer), cmp.Compare(a.Severity, b.Severity),
			cmp.Compare(a.Message, b.Message))
	})
	return slices.Compact(c.findings)
}

// admits reports whether a type set allows t; a nil set allows everything.
func admits(types map[string]bool, t string) bool {
	return types == nil || types[t]
}

func typeList(types map[string]bool) string {
	return strings.Join(slices.Sorted(maps.Keys(types)), " or ")
}

func valueList(values []any) string {
	parts := make([]string, len(values))
	for i, v := range values {
		b, _ := json.Marshal(v) // decoded JSON always marshals
		parts[i] = string(b)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func lastToken(ptr string) string {
	return ptr[strings.LastIndex(ptr, "/")+1:]
}
