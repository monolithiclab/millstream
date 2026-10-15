package pipeline

import (
	"go.yaml.in/yaml/v3"
)

// stepKeys are the keys a step accepts (spec section 4.3).
var stepKeys = []string{"action", "in", "out", "dlq", "config", "concurrency", "rate", "placement", "retry",
	"timeout", "paused", "fallback", "lessons"}

func parseSteps(n *yaml.Node, path string) (map[string]Step, error) {
	f, err := mapping(n, path)
	if err != nil {
		return nil, err
	}
	if len(f.keys) == 0 {
		return nil, errAt(f.node, f.path, "must declare at least one step")
	}
	out := make(map[string]Step, len(f.keys))
	for _, k := range f.keys {
		s, err := parseStep(k.Value, f.vals[k.Value], f.sub(k.Value))
		if err != nil {
			return nil, err
		}
		out[k.Value] = s
	}
	return out, nil
}

func parseStep(name string, n *yaml.Node, path string) (Step, error) {
	s := Step{
		Name:        name,
		DLQ:         name + ".dlq",
		Config:      map[string]any{},
		Concurrency: DefaultConcurrency,
		Retry:       Retry{MaxAttempts: DefaultMaxAttempts, Backoff: DefaultBackoff, MaxBackoff: DefaultMaxBackoff},
		Timeout:     DefaultTimeout,
	}
	f, err := mapping(n, path)
	if err != nil {
		return s, err
	}
	if err := f.allow(stepKeys...); err != nil {
		return s, err
	}
	for _, err := range []error{
		required(f, "action", &s.Action, nonEmptyStr),
		required(f, "in", &s.In, strList),
		optional(f, "out", &s.Out, parseOut),
		optional(f, "dlq", &s.DLQ, nonEmptyStr),
		optional(f, "config", &s.Config, rawMap),
		optional(f, "concurrency", &s.Concurrency, positiveInt),
		optional(f, "rate", &s.Rate, rate),
		optional(f, "placement", &s.Placement, parsePlacement),
		optional(f, "retry", &s.Retry, parseRetry),
		optional(f, "timeout", &s.Timeout, duration),
		optional(f, "paused", &s.Paused, boolean),
		optional(f, "fallback", &s.Fallback, parseFallback),
		optional(f, "lessons", &s.Lessons, nonEmptyStr),
	} {
		if err != nil {
			return s, err
		}
	}
	return s, nil
}

// rate reads a step rate such as 10/min, kept as written once ParseRate accepts it.
func rate(n *yaml.Node, path string) (string, error) {
	s, err := str(n, path)
	if err != nil {
		return "", err
	}
	if _, _, err := ParseRate(s); err != nil {
		return "", errAt(n, path, "%v", err)
	}
	return s, nil
}

// parseOut reads the three shapes of out: a stream, a list of streams, or a list of route objects.
func parseOut(n *yaml.Node, path string) (Out, error) {
	var o Out
	n = deref(n)
	if n.Kind == yaml.ScalarNode {
		s, err := nonEmptyStr(n, path)
		o.Streams = []string{s}
		return o, err
	}
	if n.Kind != yaml.SequenceNode {
		return o, errAt(n, path, "must be a stream, a list of streams or a list of route objects")
	}
	if len(n.Content) == 0 {
		return o, errAt(n, path, "must not be empty; omit out for a terminal step")
	}
	for i, c := range n.Content {
		c = deref(c)
		at := indexPath(path, i)
		if c.Kind == yaml.MappingNode {
			r, err := parseRoute(c, at)
			if err != nil {
				return o, err
			}
			o.Routes = append(o.Routes, r)
		} else {
			s, err := nonEmptyStr(c, at)
			if err != nil {
				return o, err
			}
			o.Streams = append(o.Streams, s)
		}
		if len(o.Routes) > 0 && len(o.Streams) > 0 {
			return o, errAt(c, path, "a list holds either stream names or route objects, never both")
		}
	}
	return o, nil
}

func parseRoute(n *yaml.Node, path string) (Route, error) {
	var r Route
	f, err := mapping(n, path)
	if err != nil {
		return r, err
	}
	if err := f.allow("when", "out"); err != nil {
		return r, err
	}
	if r.When, err = f.requiredStr("when"); err != nil {
		return r, err
	}
	r.Out, err = f.requiredStr("out")
	return r, err
}

// parseRetry reads a retry policy over the step defaults.
func parseRetry(n *yaml.Node, path string) (Retry, error) {
	r := Retry{MaxAttempts: DefaultMaxAttempts, Backoff: DefaultBackoff, MaxBackoff: DefaultMaxBackoff}
	f, err := mapping(n, path)
	if err != nil {
		return r, err
	}
	if err := f.allow("maxAttempts", "backoff", "maxBackoff"); err != nil {
		return r, err
	}
	for _, err := range []error{
		optional(f, "maxAttempts", &r.MaxAttempts, positiveInt),
		optional(f, "backoff", &r.Backoff, duration),
		optional(f, "maxBackoff", &r.MaxBackoff, duration),
	} {
		if err != nil {
			return r, err
		}
	}
	if r.MaxBackoff < r.Backoff {
		return r, errAt(n, path, "maxBackoff must not be shorter than backoff")
	}
	return r, nil
}

func parseFallback(n *yaml.Node, path string) (*Fallback, error) {
	f, err := mapping(n, path)
	if err != nil {
		return nil, err
	}
	if err := f.allow("action", "when", "auditRate"); err != nil {
		return nil, err
	}
	fb := &Fallback{When: []string{"abstain"}}
	for _, err := range []error{
		required(f, "action", &fb.Action, nonEmptyStr),
		optional(f, "when", &fb.When, strList),
		optional(f, "auditRate", &fb.AuditRate, nonNegativeNumber),
	} {
		if err != nil {
			return nil, err
		}
	}
	for i, w := range fb.When {
		if w != "abstain" && w != "error" {
			return nil, errAt(f.get("when"), indexPath(f.sub("when"), i), "must be abstain or error")
		}
	}
	if fb.AuditRate > 1 {
		return nil, errAt(f.get("auditRate"), f.sub("auditRate"), "must be between 0 and 1")
	}
	return fb, nil
}
