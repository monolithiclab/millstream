package evidence

import (
	"bytes"
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/monolithiclab/millstream/internal/redact"
	"github.com/monolithiclab/millstream/internal/schema"
)

// HashPrefix starts every value a bundle redacted: the hex of the HMAC-SHA256 of the value's canonical JSON under the
// deployment's key, cut to 128 bits. The same value hashes alike in every bundle of a deployment, so values compare,
// and without the key, which never leaves the store, they cannot be read or guessed from a list of candidates.
const HashPrefix = "hmac:"

// redactor replaces the values at an item's redact pointers (spec section 11.2): those of its stream and of the
// streams of its ancestors, since steps carry fields along. A value found at a pointer is replaced there, whatever
// its type; a string of redact.MinLen bytes or more, or an object or an array, is also replaced wherever else it
// appears, other payloads, edits, error messages and transcripts included: a string inside a longer one is replaced
// in place.
type redactor struct {
	key       []byte
	values    map[string]string // canonical JSON of a composite value found at a pointer, to its hash
	strs      map[string]string // a string found at a pointer, to its hash
	text      *strings.Replacer
	composite bool
}

func newRedactor(key []byte) *redactor {
	return &redactor{key: key, values: map[string]string{}, strs: map[string]string{}}
}

// hash is the stable hash of a value's canonical JSON.
func (r *redactor) hash(canonical []byte) string {
	mac := hmac.New(sha256.New, r.key)
	mac.Write(canonical)
	return HashPrefix + hex.EncodeToString(mac.Sum(nil))[:32]
}

// hashValue hashes a decoded value.
func (r *redactor) hashValue(v any) string {
	b, _ := json.Marshal(v) // decoded from JSON: always encodes, keys sorted
	return r.hash(b)
}

// decode decodes a JSON document keeping numbers' text.
func decode(data []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var v any
	err := d.Decode(&v)
	return v, err
}

// collect records the values at an item's pointers; finish must follow the last call.
func (r *redactor) collect(ptrs []string, payload []byte) {
	if len(ptrs) == 0 {
		return
	}
	doc, err := decode(payload)
	if err != nil {
		return
	}
	for _, p := range ptrs {
		v, ok := at(doc, tokens(p))
		if !ok {
			continue
		}
		switch x := v.(type) {
		case string:
			r.collectStrings(x)
		case map[string]any, []any:
			b, _ := json.Marshal(x)
			r.values[string(b)] = r.hash(b)
			r.composite = true
			r.collectStrings(x)
		}
	}
}

// collectStrings records the strings inside a composite redacted value, which may travel on their own.
func (r *redactor) collectStrings(v any) {
	switch x := v.(type) {
	case string:
		if len(x) >= redact.MinLen {
			r.strs[x] = r.hashValue(x)
		}
	case map[string]any:
		for _, c := range x {
			r.collectStrings(c)
		}
	case []any:
		for _, c := range x {
			r.collectStrings(c)
		}
	}
}

// finish builds the text replacer of the strings collected: each string, and its JSON-escaped spelling, becomes its
// hash, longest first so that a string holding another is replaced whole.
func (r *redactor) finish() {
	var pairs [][2]string
	for s, h := range r.strs {
		pairs = append(pairs, [2]string{s, h})
		if b, _ := json.Marshal(s); string(b[1:len(b)-1]) != s {
			pairs = append(pairs, [2]string{string(b[1 : len(b)-1]), h})
		}
	}
	slices.SortFunc(pairs, func(a, b [2]string) int {
		return cmp.Or(cmp.Compare(len(b[0]), len(a[0])), strings.Compare(a[0], b[0]))
	})
	flat := make([]string, 0, 2*len(pairs))
	for _, p := range pairs {
		flat = append(flat, p[0], p[1])
	}
	r.text = strings.NewReplacer(flat...)
}

// String redacts the collected strings out of text.
func (r *redactor) String(s string) string {
	if len(r.strs) == 0 {
		return s
	}
	return r.text.Replace(s)
}

// Payload redacts an item's payload: its pointers, then the collected values anywhere in it.
func (r *redactor) Payload(ptrs []string, payload []byte) ([]byte, error) {
	if len(payload) == 0 || (len(ptrs) == 0 && len(r.strs) == 0 && !r.composite) {
		return payload, nil
	}
	doc, err := decode(payload)
	if err != nil {
		return nil, err
	}
	for _, p := range ptrs {
		doc = r.replaceAt(doc, p)
	}
	return json.Marshal(r.walk(doc))
}

// Patch redacts a review's edit (a JSON Patch) of an item with the pointers given: a value written at or under one
// of them is hashed, one written above a pointer has its part at the pointer hashed, and the collected values are
// redacted from every value.
func (r *redactor) Patch(ptrs []string, patch []byte) ([]byte, error) {
	if len(patch) == 0 {
		return patch, nil
	}
	var ops []map[string]any
	d := json.NewDecoder(bytes.NewReader(patch))
	d.UseNumber()
	if err := d.Decode(&ops); err != nil {
		return nil, err
	}
	for _, op := range ops {
		v, ok := op["value"]
		if !ok {
			continue
		}
		at, _ := op["path"].(string)
		for _, p := range ptrs {
			switch {
			case at == p || strings.HasPrefix(at, p+"/"):
				v = r.hashValue(v)
			case strings.HasPrefix(p, at+"/"):
				v = r.replaceAt(v, strings.TrimPrefix(p, at))
			}
		}
		op["value"] = r.walk(v)
	}
	return json.Marshal(ops)
}

// walk redacts the collected values anywhere in a decoded document.
func (r *redactor) walk(v any) any {
	switch x := v.(type) {
	case string:
		if h, ok := r.strs[x]; ok {
			return h
		}
		return r.String(x)
	case map[string]any:
		if h, ok := r.compositeHash(x); ok {
			return h
		}
		for k, c := range x {
			x[k] = r.walk(c)
		}
	case []any:
		if h, ok := r.compositeHash(x); ok {
			return h
		}
		for i := range x {
			x[i] = r.walk(x[i])
		}
	}
	return v
}

func (r *redactor) compositeHash(v any) (string, bool) {
	if !r.composite {
		return "", false
	}
	b, _ := json.Marshal(v)
	h, ok := r.values[string(b)]
	return h, ok
}

// replaceAt replaces the value at pointer p of doc with its hash, when it is there. Pointers start with /: the
// parser refuses others (pipeline.pointerSeq).
func (r *redactor) replaceAt(doc any, p string) any {
	tokens := tokens(p)
	parent, ok := at(doc, tokens[:len(tokens)-1])
	if !ok {
		return doc
	}
	last := schema.UnescapeToken(tokens[len(tokens)-1])
	switch x := parent.(type) {
	case map[string]any:
		if v, ok := x[last]; ok {
			x[last] = r.hashValue(v)
		}
	case []any:
		if i, err := strconv.Atoi(last); err == nil && i >= 0 && i < len(x) {
			x[i] = r.hashValue(x[i])
		}
	}
	return doc
}

// tokens splits a JSON pointer into its reference tokens, still escaped.
func tokens(p string) []string { return strings.Split(p, "/")[1:] }

// at returns the value of doc at a pointer's reference tokens.
func at(doc any, tokens []string) (any, bool) {
	cur := doc
	for _, tok := range tokens {
		tok = schema.UnescapeToken(tok)
		switch x := cur.(type) {
		case map[string]any:
			v, ok := x[tok]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(tok)
			if err != nil || i < 0 || i >= len(x) {
				return nil, false
			}
			cur = x[i]
		default:
			return nil, false
		}
	}
	return cur, true
}
