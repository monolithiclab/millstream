package pipeline

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v3"
)

// pipelineName is the grammar of a pipeline identifier (spec section 4.2).
var pipelineName = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// ValidName reports whether name is a valid pipeline identifier: lowercase letters and digits, in groups joined by
// single dashes.
func ValidName(name string) bool { return pipelineName.MatchString(name) }

// Parse parses one pipeline file. Unknown keys, wrong shapes and missing required keys are errors carrying the
// key path where they occur.
func Parse(data []byte) (*Pipeline, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var doc yaml.Node
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("empty pipeline file")
		}
		return nil, fmt.Errorf("parse YAML: %w", err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("a pipeline file holds a single YAML document")
	}
	if len(doc.Content) == 0 {
		return nil, errors.New("empty pipeline file")
	}
	return parseRoot(doc.Content[0])
}

func parseRoot(n *yaml.Node) (*Pipeline, error) {
	f, err := mapping(n, "")
	if err != nil {
		return nil, err
	}
	if err := f.allow("pipeline", "description", "vars", "secrets", "triggers", "streams", "steps", "budget",
		"limits", "placement", "insights", "improve"); err != nil {
		return nil, err
	}
	p := &Pipeline{Vars: map[string]any{}, Streams: map[string]StreamDecl{}}
	if p.Name, err = f.requiredStr("pipeline"); err != nil {
		return nil, err
	}
	if !pipelineName.MatchString(p.Name) {
		return nil, errAt(f.get("pipeline"), "pipeline", "%q must be lowercase letters, digits and single dashes", p.Name)
	}
	var placement *Placement
	for _, err := range []error{
		optional(f, "description", &p.Description, str),
		optional(f, "vars", &p.Vars, rawMap),
		optional(f, "secrets", &p.Secrets, parseSecrets),
		optional(f, "triggers", &p.Triggers, parseTriggers),
		optional(f, "streams", &p.Streams, parseStreams),
		required(f, "steps", &p.Steps, parseSteps),
		optional(f, "budget", &p.Budget, parseBudget),
		optional(f, "limits", &p.Limits, parseLimits),
		optional(f, "placement", &placement, parsePlacement),
		optional(f, "insights", &p.Insights, rawMap),
		optional(f, "improve", &p.Improve, rawMap),
	} {
		if err != nil {
			return nil, err
		}
	}
	if placement != nil {
		p.Placement = *placement
	}
	if p.Triggers == nil {
		// A pipeline that imports a stream has runs without triggers: the import opens them (spec section 4.12).
		if len(p.Imports()) == 0 {
			return nil, errAt(f.node, "triggers", "is required unless the pipeline imports a stream")
		}
		p.Triggers = map[string]Trigger{}
	}
	return p, nil
}

func parseSecrets(n *yaml.Node, path string) ([]string, error) {
	names, err := strSeq(n, path)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(names))
	for i, s := range names {
		if seen[s] {
			return nil, errAt(deref(n).Content[i], indexPath(path, i), "duplicate secret %q", s)
		}
		seen[s] = true
	}
	return names, nil
}

func parseTriggers(n *yaml.Node, path string) (map[string]Trigger, error) {
	f, err := mapping(n, path)
	if err != nil {
		return nil, err
	}
	if len(f.keys) == 0 {
		return nil, errAt(f.node, f.path, "must declare at least one trigger")
	}
	out := make(map[string]Trigger, len(f.keys))
	for _, k := range f.keys {
		t, err := parseTrigger(f.vals[k.Value], f.sub(k.Value))
		if err != nil {
			return nil, err
		}
		out[k.Value] = t
	}
	return out, nil
}

func parseTrigger(n *yaml.Node, path string) (Trigger, error) {
	var t Trigger
	f, err := mapping(n, path)
	if err != nil {
		return t, err
	}
	kinds := 0
	for _, k := range []string{"cron", "manual", "webhook"} {
		if f.get(k) != nil {
			kinds++
		}
	}
	if kinds != 1 {
		return t, errAt(f.node, path, "must set exactly one of cron, manual or webhook")
	}
	switch {
	case f.get("cron") != nil:
		if err = f.allow("cron", "tz", "catchup", "inputSchema"); err == nil {
			t.Cron, err = parseCron(f)
		}
	case f.get("manual") != nil:
		if err = f.allow("manual", "inputSchema"); err == nil {
			err = optional(f, "manual", &t.Manual, parseManual)
		}
		if t.Manual == nil {
			t.Manual = &ManualSpec{}
		}
	default:
		if err = f.allow("webhook", "inputSchema"); err == nil {
			err = required(f, "webhook", &t.Webhook, parseWebhook)
		}
	}
	if err != nil {
		return t, err
	}
	return t, optional(f, "inputSchema", &t.InputSchema, value)
}

func parseCron(f *fields) (*CronSpec, error) {
	c := &CronSpec{Catchup: CatchupLatest}
	var err error
	if c.Expr, err = f.requiredStr("cron"); err != nil {
		return nil, err
	}
	if err := optional(f, "tz", &c.TZ, nonEmptyStr); err != nil {
		return nil, err
	}
	if err := optional(f, "catchup", &c.Catchup, str); err != nil {
		return nil, err
	}
	switch c.Catchup {
	case CatchupLatest, CatchupAll, CatchupNone:
		return c, nil
	default:
		return nil, errAt(f.get("catchup"), f.sub("catchup"), "must be latest, all or none")
	}
}

func parseManual(n *yaml.Node, path string) (*ManualSpec, error) {
	f, err := mapping(n, path)
	if err != nil {
		return nil, err
	}
	return &ManualSpec{}, f.allow()
}

func parseWebhook(n *yaml.Node, path string) (*WebhookSpec, error) {
	f, err := mapping(n, path)
	if err != nil {
		return nil, err
	}
	if err := f.allow("rate"); err != nil {
		return nil, err
	}
	w := &WebhookSpec{Rate: DefaultWebhookRate}
	return w, optional(f, "rate", &w.Rate, rate)
}

func parseStreams(n *yaml.Node, path string) (map[string]StreamDecl, error) {
	f, err := mapping(n, path)
	if err != nil {
		return nil, err
	}
	out := make(map[string]StreamDecl, len(f.keys))
	for _, k := range f.keys {
		d, err := parseStream(f.vals[k.Value], f.sub(k.Value))
		if err != nil {
			return nil, err
		}
		out[k.Value] = d
	}
	return out, nil
}

func parseStream(n *yaml.Node, path string) (StreamDecl, error) {
	var d StreamDecl
	f, err := mapping(n, path)
	if err != nil {
		return d, err
	}
	if f.get("pipeline") != nil || f.get("stream") != nil {
		// An imported stream's copies are the consumer's own items: it sets their retention (spec section 4.12).
		if err := f.allow("pipeline", "stream", "retention"); err != nil {
			return d, err
		}
		if err := optional(f, "retention", &d.Retention, duration); err != nil {
			return d, err
		}
		imp := &StreamImport{}
		if imp.Pipeline, err = f.requiredStr("pipeline"); err != nil {
			return d, err
		}
		if imp.Stream, err = f.requiredStr("stream"); err != nil {
			return d, err
		}
		d.Import = imp
		return d, nil
	}
	if err := f.allow("schema", "retention", "redact"); err != nil {
		return d, err
	}
	for _, err := range []error{
		optional(f, "schema", &d.Schema, value),
		optional(f, "retention", &d.Retention, duration),
		optional(f, "redact", &d.Redact, pointerSeq),
	} {
		if err != nil {
			return d, err
		}
	}
	return d, nil
}

// pointerSeq reads a list of JSON pointers into a payload, such as /seller/phone.
func pointerSeq(n *yaml.Node, path string) ([]string, error) {
	list, err := strSeq(n, path)
	for i, p := range list {
		if err == nil && !strings.HasPrefix(p, "/") {
			err = errAt(deref(n).Content[i], indexPath(path, i), "must be a JSON pointer such as /seller/phone")
		}
	}
	return list, err
}

func parseBudget(n *yaml.Node, path string) (Budget, error) {
	var b Budget
	f, err := mapping(n, path)
	if err != nil {
		return b, err
	}
	if err := f.allow("runUsd", "dayUsd"); err != nil {
		return b, err
	}
	if err := optional(f, "runUsd", &b.RunUSD, nonNegativeNumber); err != nil {
		return b, err
	}
	return b, optional(f, "dayUsd", &b.DayUSD, nonNegativeNumber)
}

func parseLimits(n *yaml.Node, path string) (Limits, error) {
	var l Limits
	f, err := mapping(n, path)
	if err != nil {
		return l, err
	}
	if err := f.allow("concurrency"); err != nil {
		return l, err
	}
	return l, optional(f, "concurrency", &l.Concurrency, positiveInt)
}

func parsePlacement(n *yaml.Node, path string) (*Placement, error) {
	f, err := mapping(n, path)
	if err != nil {
		return nil, err
	}
	if err := f.allow("labels"); err != nil {
		return nil, err
	}
	p := &Placement{Labels: map[string]string{}}
	if v := f.get("labels"); !isNull(v) {
		labels, err := mapping(v, f.sub("labels"))
		if err != nil {
			return nil, err
		}
		for _, k := range labels.keys {
			if p.Labels[k.Value], err = str(labels.vals[k.Value], labels.sub(k.Value)); err != nil {
				return nil, err
			}
		}
	}
	return p, nil
}

// rawMap decodes a mapping kept as plain values (config, vars, and the settings M1 does not interpret).
func rawMap(n *yaml.Node, path string) (map[string]any, error) {
	if deref(n).Kind != yaml.MappingNode {
		return nil, errAt(n, path, "must be a mapping")
	}
	v, err := value(n, path)
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}
