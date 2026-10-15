// Package schema is Millstream's JSON Schema support (2020-12): runtime validation of items, and the static
// compatibility check between a producer's output schema and a consumer's input schema (spec section 4.11).
// A schema may only reference its own document: a $ref anywhere else would make check depend on the network and
// revisions irreproducible, so it is refused at compile time and by the checker alike.
package schema

import (
	"bytes"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// docURL is the location the compiled document is registered under; nothing is ever fetched from it.
const docURL = "urn:millstream:schema"

// Schema is a compiled schema, safe for concurrent validation.
type Schema struct {
	compiled *jsonschema.Schema
}

// Compile compiles a JSON Schema document (draft 2020-12 unless it says otherwise). formats are asserted. Any
// $ref that is not a same-document fragment (# or #/...) is refused.
func Compile(doc []byte) (*Schema, error) {
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
