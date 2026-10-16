// Package schema is Millstream's JSON Schema support (2020-12): runtime validation of items, and the static
// compatibility check between a producer's output schema and a consumer's input schema (spec section 4.11).
// A schema may only reference its own document: a $ref anywhere else would make check depend on the network and
// revisions irreproducible, so it is refused at compile time and by the checker alike.
package schema

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/monolithiclab/millstream/internal/memo"
)

// docURL is the location the compiled document is registered under; nothing is ever fetched from it.
const docURL = "urn:millstream:schema"

// Schema is a compiled schema, safe for concurrent validation.
type Schema struct {
	compiled *jsonschema.Schema
}

// Compile compiles a JSON Schema document (draft 2020-12 unless it says otherwise). formats are asserted. Any
// $ref that is not a same-document fragment (# or #/...) is refused. A document compiles once per process: the same
// schemas come back (a step's output schemas for each review decision, an action's for each revision loaded), and a
// compiled schema is safe for concurrent validation, so callers share it.
func Compile(doc []byte) (*Schema, error) {
	c := compiled.Get(string(doc), func() compiledSchema {
		s, err := compile(doc)
		return compiledSchema{s, err}
	})
	return c.s, c.err
}

// compiled holds Compile's results: revisions' documents, but nothing bounds how many revisions a long-lived process
// applies.
var compiled = memo.New[string, compiledSchema](256)

type compiledSchema struct {
	s   *Schema
	err error
}

func compile(doc []byte) (*Schema, error) {
	v, err := parse(doc)
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	c.UseLoader(refuseLoader{})
	if err := c.AddResource(docURL, v); err != nil {
		return nil, fmt.Errorf("compile schema: %w", err)
	}
	s, err := c.Compile(docURL)
	if err != nil {
		return nil, fmt.Errorf("compile schema: %w", err)
	}
	return &Schema{compiled: s}, nil
}

// Validate validates a decoded JSON value (as produced by encoding/json) against the schema.
func (s *Schema) Validate(v any) error {
	if err := s.compiled.Validate(v); err != nil {
		return fmt.Errorf("validate: %w", err)
	}
	return nil
}

// Violation is one way a value fails a schema. Pointer is a JSON pointer into the value ("" for the root).
type Violation struct {
	Pointer string
	Message string
}

// printer renders validation messages; they are developer-facing, in English.
var printer = message.NewPrinter(language.English)

// Violations validates a decoded JSON value and returns each failure at the leaves of the error tree, sorted by
// pointer then message, so callers can place each one; nil means valid.
func (s *Schema) Violations(v any) []Violation {
	err := s.compiled.Validate(v)
	var ve *jsonschema.ValidationError
	if !errors.As(err, &ve) {
		if err != nil {
			return []Violation{{Message: err.Error()}}
		}
		return nil
	}
	var out []Violation
	var collect func(e *jsonschema.ValidationError)
	collect = func(e *jsonschema.ValidationError) {
		if len(e.Causes) == 0 {
			out = append(out, Violation{Pointer: pointer(e.InstanceLocation), Message: e.ErrorKind.LocalizedString(printer)})
		}
		for _, c := range e.Causes {
			collect(c)
		}
	}
	collect(ve)
	slices.SortFunc(out, func(a, b Violation) int {
		return cmp.Or(cmp.Compare(a.Pointer, b.Pointer), cmp.Compare(a.Message, b.Message))
	})
	return slices.Compact(out)
}

// pointer joins reference tokens into a JSON pointer.
func pointer(tokens []string) string {
	var b bytes.Buffer
	for _, t := range tokens {
		b.WriteByte('/')
		b.WriteString(EscapeToken(t))
	}
	return b.String()
}

// parse decodes a schema document and refuses any reference outside it: the one entry point for documents, so
// the same-document rule holds for compilation and for the compatibility check alike.
func parse(doc []byte) (any, error) {
	v, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return nil, fmt.Errorf("schema is not valid JSON: %w", err)
	}
	if err := checkRefs(v); err != nil {
		return nil, err
	}
	return v, nil
}

// refuseLoader refuses every URL: the only document is the one added as a resource. It backs up checkRefs for
// anything that would still trigger a load, such as a $schema naming an unknown metaschema.
type refuseLoader struct{}

func (refuseLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("loading %s is refused: a schema may only reference its own document", url)
}

// checkRefs refuses every $ref and $dynamicRef that is not "#" or "#/...".
func checkRefs(root any) error {
	return walk(root, "", func(obj map[string]any, ptr string) error {
		for _, kw := range []string{"$ref", "$dynamicRef"} {
			ref, ok := obj[kw].(string)
			if !ok {
				continue
			}
			if !isLocalRef(ref) {
				return fmt.Errorf("%s %q at %s is refused: only references into the same document (#/...) are allowed",
					kw, ref, pointerOrRoot(ptr))
			}
		}
		return nil
	})
}

func isLocalRef(ref string) bool {
	return ref == "#" || (len(ref) > 1 && ref[0] == '#' && ref[1] == '/')
}
