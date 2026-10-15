package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// DecodeValue decodes exactly one JSON value into plain Go values, numbers as int64 when integral and float64
// otherwise, matching how values are read from YAML. Expressions compare such values, so every JSON document that
// reaches CEL (variables, payloads) goes through it.
func DecodeValue(raw string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after the JSON value")
	}
	return normalizeNumbers(v)
}

// UnmarshalPipeline decodes a pipeline encoded with encoding/json, as the store keeps each revision's pipeline. The
// free-form values (variable defaults, config, schemas, insights, improve) get the same number types as when parsed
// from YAML, so a pipeline read back from the store resolves and interpolates exactly like the original.
func UnmarshalPipeline(data []byte) (*Pipeline, error) {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	var p Pipeline
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("decode pipeline: %w", err)
	}
	var err error
	norm := func(v any) any {
		n, e := normalizeNumbers(v)
		if e != nil && err == nil {
			err = e
		}
		return n
	}
	for k, v := range p.Vars {
		p.Vars[k] = norm(v)
	}
	for k, t := range p.Triggers {
		t.InputSchema = norm(t.InputSchema)
		p.Triggers[k] = t
	}
	for k, s := range p.Streams {
		s.Schema = norm(s.Schema)
		p.Streams[k] = s
	}
	for k, s := range p.Steps {
		if s.Config != nil {
			s.Config = norm(s.Config).(map[string]any)
		}
		p.Steps[k] = s
	}
	if p.Insights != nil {
		p.Insights = norm(p.Insights).(map[string]any)
	}
	if p.Improve != nil {
		p.Improve = norm(p.Improve).(map[string]any)
	}
	if err != nil {
		return nil, fmt.Errorf("decode pipeline: %w", err)
	}
	return &p, nil
}

func normalizeNumbers(v any) (any, error) {
	var err error
	switch t := v.(type) {
	case json.Number:
		if !strings.ContainsAny(string(t), ".eE") {
			if i, err := strconv.ParseInt(string(t), 10, 64); err == nil {
				return i, nil
			}
		}
		f, err := t.Float64()
		if err != nil {
			return nil, fmt.Errorf("number %s out of range", t)
		}
		return f, nil
	case []any:
		for i := range t {
			if t[i], err = normalizeNumbers(t[i]); err != nil {
				return nil, err
			}
		}
	case map[string]any:
		for k := range t {
			if t[k], err = normalizeNumbers(t[k]); err != nil {
				return nil, err
			}
		}
	}
	return v, nil
}
