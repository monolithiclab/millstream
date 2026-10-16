package pipeline

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/monolithiclab/millstream/internal/expr"
	"github.com/monolithiclab/millstream/internal/schema"
)

// Diagnostic severities: an error fails check, a warning does not.
const (
	SeverityError   = "error"
	SeverityWarning = "warning"
)

// Diagnostic is one finding of Check, located by a key path into the pipeline file (steps.score.config.token).
type Diagnostic struct {
	Severity string
	Path     string
	Message  string
}

// String formats d as "severity path: message".
func (d Diagnostic) String() string { return d.Severity + " " + d.Path + ": " + d.Message }

// HasErrors reports whether any diagnostic is an error.
func HasErrors(diags []Diagnostic) bool {
	return slices.ContainsFunc(diags, func(d Diagnostic) bool { return d.Severity == SeverityError })
}

// builtinKeys are the config keys each builtin accepts (spec sections 4.7 and 8).
var builtinKeys = map[string][]string{
	BuiltinFilter: {"expr"},
	BuiltinDedupe: {"key", "window"},
	BuiltinReview: {"inbox", "timeout", "onTimeout", "editable", "notify", "display", "reasons", "autoApprove",
		"sampleRate"},
}

// CheckOptions are the environment file's settings Check applies.
type CheckOptions struct {
	// LessonsMaxTokens is lessons.maxTokens; zero means DefaultLessonsMaxTokens.
	LessonsMaxTokens int
}

// Check validates a parsed package (spec section 4.8) and returns every finding, sorted by path, then severity,
// then message. Parsing already refused shape errors; Check covers what needs the whole package: references
// between steps, streams, triggers, secrets and actions, cycles, expressions, configs against action schemas,
// schema compatibility along every stream and lessons files. env resolves secrets as `run` would
// (MILLSTREAM_SECRET_<NAME>).
func Check(pkg *Package, actions ActionResolver, env func(string) (string, bool), opts CheckOptions) []Diagnostic {
	if opts.LessonsMaxTokens <= 0 {
		opts.LessonsMaxTokens = DefaultLessonsMaxTokens
	}
	c := &checker{pkg: pkg, p: pkg.Pipeline, env: env, opts: opts, actions: map[string]*Action{}}
	c.producers, c.consumers = c.p.streamIndex()
	c.stepNames = slices.Sorted(maps.Keys(c.p.Steps))
	c.checkSecrets()
	c.checkInsights()
	c.checkTriggers()
	c.checkStreams()
	for _, name := range c.stepNames {
		c.checkStep(name, actions)
	}
	c.checkCycles()
	c.checkCompatibility()
	c.checkExternalEffects()
	return SortDiagnostics(c.diags)
}

// SortDiagnostics sorts diagnostics by path, then severity, then message, and drops repeats: the order Check and
// CheckInstance return them in.
func SortDiagnostics(diags []Diagnostic) []Diagnostic {
	slices.SortFunc(diags, func(a, b Diagnostic) int {
		return cmp.Or(cmp.Compare(a.Path, b.Path), cmp.Compare(a.Severity, b.Severity), cmp.Compare(a.Message, b.Message))
	})
	return slices.Compact(diags)
}

type checker struct {
	pkg                  *Package
	p                    *Pipeline
	env                  func(string) (string, bool)
	opts                 CheckOptions
	actions              map[string]*Action // resolved action of each step whose reference resolved
	producers, consumers map[string][]string
	stepNames            []string
	diags                []Diagnostic
}

func (c *checker) add(severity, at, format string, args ...any) {
	c.diags = append(c.diags, Diagnostic{Severity: severity, Path: at, Message: fmt.Sprintf(format, args...)})
}

func (c *checker) errorf(at, format string, args ...any) { c.add(SeverityError, at, format, args...) }

func (c *checker) warnf(at, format string, args ...any) { c.add(SeverityWarning, at, format, args...) }

// checkSecrets fails on a declared secret the environment cannot resolve.
func (c *checker) checkSecrets() {
	for i, name := range c.p.Secrets {
		if _, ok := c.env(SecretEnv(name)); !ok {
			c.errorf(indexPath("secrets", i), "secret %s is not set: set %s", name, SecretEnv(name))
		}
	}
}

// checkInsights checks the insights key (spec section 11.1).
func (c *checker) checkInsights() {
	if _, err := c.p.InsightsConfig(); err != nil {
		c.errorf("insights", "%v", err)
	}
}

// checkTriggers warns on a trigger whose stream no step reads: its runs would do nothing.
func (c *checker) checkTriggers() {
	for _, name := range slices.Sorted(maps.Keys(c.p.Triggers)) {
		if _, read := c.consumers[TriggerStreamPrefix+name]; !read {
			c.warnf(joinPath("triggers", name), "no step reads %s%s", TriggerStreamPrefix, name)
		}
	}
}

// checkStreams checks the entries under streams: the reserved prefix, and declarations no step uses.
func (c *checker) checkStreams() {
	dlqs := map[string]bool{}
	for _, s := range c.p.Steps {
		dlqs[s.DLQ] = true
	}
	for _, name := range slices.Sorted(maps.Keys(c.p.Streams)) {
		d, at := c.p.Streams[name], joinPath("streams", name)
		if trigger, ok := strings.CutPrefix(name, TriggerStreamPrefix); ok {
			_, exists := c.p.Triggers[trigger]
			switch {
			case !exists:
				c.errorf(at, "the %s prefix is reserved for trigger streams and there is no trigger %s",
					TriggerStreamPrefix, trigger)
			case d.Import != nil || d.Schema != nil:
				c.errorf(at, "a trigger stream is written by its trigger only: set the trigger's inputSchema instead")
			}
			continue
		}
		_, read := c.consumers[name]
		_, written := c.producers[name]
		if !read && !written && !dlqs[name] {
			c.errorf(at, "stream %s is declared but no step reads or writes it", name)
		}
	}
}

// checkStep checks one step's references, expressions and config.
func (c *checker) checkStep(name string, actions ActionResolver) {
	s, at := c.p.Steps[name], joinPath("steps", name)
	if len(s.In) == 0 {
		c.errorf(joinPath(at, "in"), "is required")
	}
	for i, in := range s.In {
		c.checkInput(indexPath(joinPath(at, "in"), i), in)
	}
	c.checkOut(name, s, at)
	if s.DLQ != "" {
		c.checkWritable(joinPath(at, "dlq"), s.DLQ)
	}

	a, err := actions.Resolve(c.pkg, s.Action)
	switch {
	case errors.Is(err, ErrUnavailable):
		c.warnf(joinPath(at, "action"), "%v", err)
	case err != nil:
		c.errorf(joinPath(at, "action"), "%v", err)
	default:
		c.actions[name] = a
		if a.Unpinned {
			c.warnf(joinPath(at, "action"), "image %s (%s) is not pinned in %s: run millstream lock", a.Image,
				a.ImageDigest, LockFile)
		}
		if a.Agent != nil {
			c.checkAgent(joinPath(at, "action"), a)
		}
		if err := BudgetMaxCost(c.p.Budget, name, a); err != nil {
			c.errorf(joinPath(at, "action"), "%v", err)
		}
	}

	c.checkLessons(name, s, a)

	cfgAt := joinPath(at, "config")
	cfg, skipped := c.inferConfig(s.Config, cfgAt)
	switch {
	case a == nil:
	case a.Builtin:
		c.checkBuiltinConfig(a.Name, s.Config, cfgAt)
	case a.Config != nil:
		c.checkConfigSchema(a, cfg, skipped, cfgAt)
	case len(s.Config) > 0:
		c.warnf(cfgAt, "action %s declares no config schema: config is not checked", a.Ref)
	}
}

// checkAgent checks what an agent action needs from the pipeline: its API key secret declared, and a network to
// reach its model from the sandbox.
func (c *checker) checkAgent(at string, a *Action) {
	if name := a.Agent.APIKeySecret; name != "" && !slices.Contains(c.p.Secrets, name) {
		c.errorf(at, "action %s reads its API key from secret %s, which is not declared under secrets", a.Ref, name)
	}
	if len(a.Agent.Network) == 0 {
		c.warnf(at, "action %s declares no runtime.agent.network: in its sandbox it cannot reach its model; list "+
			"the model's endpoint there (api.anthropic.com)", a.Ref)
	}
}

// checkLessons checks a step's lessons file (spec section 11.4): it parses, and stays under lessons.maxTokens,
// since the adapter never truncates it. Lessons on a step whose action is not an agent are never used.
func (c *checker) checkLessons(name string, s Step, a *Action) {
	if s.Lessons == "" {
		return
	}
	at := joinPath(joinPath("steps", name), "lessons")
	_, data, err := StepLessons(c.pkg, s)
	if err != nil {
		c.errorf(at, "%v", err)
		return
	}
	if n := LessonsTokens(data); n > c.opts.LessonsMaxTokens {
		c.errorf(at, "%s is about %d tokens (characters / 4), over lessons.maxTokens %d: merge or prune its rules "+
			"and examples, it is never truncated", s.Lessons, n, c.opts.LessonsMaxTokens)
	}
	if a != nil && a.Kind != KindAgent {
		c.warnf(at, "lessons apply to agent steps only: %s is not used by action %s", s.Lessons, a.Ref)
	}
}

// checkInput checks one stream a step reads.
func (c *checker) checkInput(at, stream string) {
	if trigger, ok := strings.CutPrefix(stream, TriggerStreamPrefix); ok {
		if _, exists := c.p.Triggers[trigger]; !exists {
			c.errorf(at, "there is no trigger %s", trigger)
		}
		return
	}
	if c.p.Streams[stream].Import != nil {
		return
	}
	if _, written := c.producers[stream]; !written {
		c.errorf(at, "stream %s is read but no step writes it", stream)
	}
}

// checkOut checks the streams a step writes and its route expressions, and warns on outputs nobody reads.
func (c *checker) checkOut(name string, s Step, at string) {
	outAt := joinPath(at, "out")
	if len(s.Out.Streams) > 0 && len(s.Out.Routes) > 0 {
		c.errorf(outAt, "a list holds either stream names or route objects, never both")
	}
	targets := make([]string, 0, len(s.Out.Streams)+len(s.Out.Routes))
	paths := make([]string, 0, cap(targets))
	for i, stream := range s.Out.Streams {
		p := outAt
		if len(s.Out.Streams) > 1 {
			p = indexPath(outAt, i)
		}
		targets, paths = append(targets, stream), append(paths, p)
	}
	for i, r := range s.Out.Routes {
		p := indexPath(outAt, i)
		if _, err := expr.CompileRoute(r.When); err != nil {
			c.errorf(joinPath(p, "when"), "%v", err)
		}
		targets, paths = append(targets, r.Out), append(paths, joinPath(p, "out"))
	}
	for i, stream := range targets {
		if !c.checkWritable(paths[i], stream) {
			continue
		}
		if _, read := c.consumers[stream]; !read {
			c.warnf(paths[i], "stream %s is written by step %s but no step reads it", stream, name)
		}
	}
}

// checkWritable fails when a step writes a root stream, which only its trigger or import writes.
func (c *checker) checkWritable(at, stream string) bool {
	switch {
	case strings.HasPrefix(stream, TriggerStreamPrefix):
		c.errorf(at, "stream %s is a trigger stream: only its trigger writes it", stream)
	case c.p.Streams[stream].Import != nil:
		c.errorf(at, "stream %s is imported from pipeline %s: only the import writes it", stream,
			c.p.Streams[stream].Import.Pipeline)
	default:
		return true
	}
	return false
}

// inferConfig interpolates config as a run would, with every variable at its default, so its values get the
// types the action will see (spec section 4.8). A value that reads a secret or a variable without a default cannot
// be known before a run: it is kept as written and its JSON pointer returned in skipped, so schema findings on it
// are dropped. Expressions that do not compile, and names that are not declared, are errors.
func (c *checker) inferConfig(config map[string]any, at string) (out map[string]any, skipped []string) {
	defaults := map[string]any{}
	for k, v := range c.p.Vars {
		if v != nil {
			defaults[k] = v
		}
	}
	var walk func(v any, ptr, at string) any
	walk = func(v any, ptr, at string) any {
		switch t := v.(type) {
		case map[string]any:
			m := make(map[string]any, len(t))
			for k, child := range t {
				m[k] = walk(child, ptr+"/"+schema.EscapeToken(k), joinPath(at, k))
			}
			return m
		case []any:
			l := make([]any, len(t))
			for i, child := range t {
				l[i] = walk(child, ptr+"/"+strconv.Itoa(i), indexPath(at, i))
			}
			return l
		case string:
			r, known := c.interpolate(t, at, defaults)
			if !known {
				skipped = append(skipped, ptr)
			}
			return r
		default:
			return v
		}
	}
	out, _ = walk(config, "", at).(map[string]any)
	return out, skipped
}

// interpolate evaluates one config string with the defaults, reporting whether its value is known.
func (c *checker) interpolate(s, at string, defaults map[string]any) (any, bool) {
	refs, err := expr.TemplateRefs(s)
	if err != nil {
		c.errorf(at, "%v", err)
		return s, false
	}
	if !refs.HasExpr {
		return s, true
	}
	known := len(refs.Secrets) == 0 && !refs.AllSecrets && (!refs.AllVars || len(defaults) == len(c.p.Vars))
	for _, name := range refs.Secrets {
		if !slices.Contains(c.p.Secrets, name) {
			c.errorf(at, "secret %s is not declared under secrets", name)
		}
	}
	for _, name := range refs.Vars {
		def, declared := c.p.Vars[name]
		if !declared {
			c.errorf(at, "variable %s is not declared under vars", name)
		}
		known = known && def != nil
	}
	if !known {
		return s, false
	}
	v, err := expr.Interpolate(map[string]any{"v": s}, expr.InterpCtx{Vars: defaults})
	if err != nil {
		c.errorf(at, "with the default variables: %v", errors.Unwrap(err))
		return s, false
	}
	return v["v"], true
}

// checkConfigSchema validates the inferred config against the action's config schema.
func (c *checker) checkConfigSchema(a *Action, cfg map[string]any, skipped []string, at string) {
	sch, err := schema.Compile(a.Config)
	if err != nil {
		c.errorf(at, "action %s: config schema: %v", a.Ref, err)
		return
	}
	// Validation sees JSON values: round-trip so Go types (time.Time from run.startedAt) become what an action reads.
	v, err := jsonValue(cfg)
	if err != nil {
		c.errorf(at, "encode config: %v", err)
		return
	}
	for _, viol := range sch.Violations(v) {
		if slices.ContainsFunc(skipped, func(p string) bool {
			return viol.Pointer == p || strings.HasPrefix(viol.Pointer, p+"/")
		}) {
			continue
		}
		c.errorf(valuePath(at, v, viol.Pointer), "does not match the config schema of %s: %s", a.Ref, viol.Message)
	}
}

// checkBuiltinConfig checks a builtin's config keys and compiles its item expressions.
func (c *checker) checkBuiltinConfig(builtin string, cfg map[string]any, at string) {
	if builtin == BuiltinReview { // ParseReview checks every key, unknown ones included
		r, errs := ParseReview("", cfg)
		for _, e := range errs {
			c.errorf(joinPath(at, e.Key), "%s", e.Message)
		}
		if name := r.NotifySecret; name != "" && !slices.Contains(c.p.Secrets, name) {
			c.errorf(joinPath(at, "notify"), "secret %s is not declared under secrets", name)
		}
		return
	}
	for _, k := range slices.Sorted(maps.Keys(cfg)) {
		if !slices.Contains(builtinKeys[builtin], k) {
			c.errorf(joinPath(at, k), "unknown key for the %s builtin", builtin)
		}
	}
	compileStr := func(key string, required bool, compile func(string) error) {
		v, set := cfg[key]
		src, ok := v.(string)
		switch {
		case !set && required:
			c.errorf(joinPath(at, key), "is required by the %s builtin", builtin)
		case !set:
		case !ok || src == "":
			c.errorf(joinPath(at, key), "must be a CEL expression")
		default:
			if err := compile(src); err != nil {
				c.errorf(joinPath(at, key), "%v", err)
			}
		}
	}
	compileItem := func(src string) error { _, err := expr.CompileItem(src); return err }
	switch builtin {
	case BuiltinFilter:
		compileStr("expr", true, compileItem)
	case BuiltinDedupe:
		compileStr("key", true, func(src string) error { _, err := expr.CompileKey(src); return err })
		if w, set := cfg["window"]; set {
			if s, ok := w.(string); !ok {
				c.errorf(joinPath(at, "window"), "must be a duration such as 30d")
			} else if _, err := ParseDuration(s); err != nil {
				c.errorf(joinPath(at, "window"), "%v", err)
			}
		}
	}
}

// stepGraph returns, for each step, the steps reading a stream it writes (dead-letter streams included). Root
// streams are left out: a step writing one is already an error, and it must not also show up as a cycle.
func (c *checker) stepGraph() map[string][]string {
	g := map[string][]string{}
	for stream, writers := range c.producers {
		if strings.HasPrefix(stream, TriggerStreamPrefix) || c.p.Streams[stream].Import != nil {
			continue
		}
		for _, w := range writers {
			for _, r := range c.consumers[stream] {
				if !slices.Contains(g[w], r) {
					g[w] = append(g[w], r)
				}
			}
		}
	}
	for _, next := range g {
		slices.Sort(next)
	}
	return g
}

// checkCycles fails on every cycle in the step graph, reported once per strongly connected component.
func (c *checker) checkCycles() {
	for _, comp := range components(c.stepNames, c.stepGraph()) {
		c.errorf(joinPath("steps", comp[0]), "cycle in the step graph: %s", strings.Join(append(comp, comp[0]), " -> "))
	}
}

// components returns the strongly connected components of g that contain a cycle (several steps, or one step
// reading its own output), each starting at its smallest step and in sorted order (Tarjan's algorithm).
func components(nodes []string, g map[string][]string) [][]string {
	index, low, onStack := map[string]int{}, map[string]int{}, map[string]bool{}
	var stack []string
	var out [][]string
	var visit func(n string)
	visit = func(n string) {
		index[n], low[n] = len(index), len(index)
		stack, onStack[n] = append(stack, n), true
		for _, m := range g[n] {
			if _, seen := index[m]; !seen {
				visit(m)
				low[n] = min(low[n], low[m])
			} else if onStack[m] {
				low[n] = min(low[n], index[m])
			}
		}
		if low[n] != index[n] {
			return
		}
		var comp []string
		for {
			m := stack[len(stack)-1]
			stack, onStack[m] = stack[:len(stack)-1], false
			comp = append(comp, m)
			if m == n {
				break
			}
		}
		if len(comp) > 1 || slices.Contains(g[n], n) {
			slices.Sort(comp)
			out = append(out, comp)
		}
	}
	for _, n := range nodes {
		if _, seen := index[n]; !seen {
			visit(n)
		}
	}
	slices.SortFunc(out, func(a, b []string) int { return cmp.Compare(a[0], b[0]) })
	return out
}

// checkCompatibility checks, for every step with an input schema, each schema reaching each stream it reads
// against that input schema (spec section 4.11).
func (c *checker) checkCompatibility() {
	for _, name := range c.stepNames {
		a := c.actions[name]
		if a == nil || a.Input == nil {
			continue
		}
		for i, in := range c.p.Steps[name].In {
			origins := Origins(c.pkg, func(step string) *Action { return c.actions[step] }, in,
				func(at string, err error) { c.errorf(at, "%v", err) })
			for _, o := range origins {
				for _, f := range schema.Compatible(o.Schema, a.Input) {
					where := ""
					if f.Pointer != "" {
						where = " at " + f.Pointer
					}
					c.add(f.Severity, indexPath(joinPath(joinPath("steps", name), "in"), i),
						"%s on stream %s against the input schema of %s%s: %s", o.From, in, a.Ref, where, f.Message)
				}
			}
		}
	}
}

// checkExternalEffects warns when an agent step's output can reach a step with external effects without passing
// a review step that has no autoApprove (spec section 15.4). Advisory only.
func (c *checker) checkExternalEffects() {
	g := c.stepGraph()
	for _, name := range c.stepNames {
		if a := c.actions[name]; a == nil || a.Kind != KindAgent {
			continue
		}
		seen := map[string]bool{name: true}
		queue := slices.Clone(g[name])
		for len(queue) > 0 {
			next := queue[0]
			queue = queue[1:]
			if seen[next] {
				continue
			}
			seen[next] = true
			a := c.actions[next]
			if a == nil {
				continue
			}
			if a.Effects == EffectsExternal {
				c.warnf(joinPath("steps", name), "agent output can reach step %s, whose action has external effects, "+
					"without a review step that has no autoApprove in between", next)
			}
			if a.Builtin && a.Name == BuiltinReview && c.p.Steps[next].Config["autoApprove"] == nil {
				continue
			}
			queue = append(queue, g[next]...)
		}
	}
}

// jsonValue returns v as encoding/json would decode its JSON encoding.
func jsonValue(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	err = json.Unmarshal(b, &out)
	return out, err
}

// valuePath turns a JSON pointer into v into a key path under at: /filters/max is at.filters.max, /tags/1 at.tags[1].
func valuePath(at string, v any, ptr string) string {
	if ptr == "" {
		return at
	}
	for _, tok := range strings.Split(ptr[1:], "/") {
		tok = schema.UnescapeToken(tok)
		switch t := v.(type) {
		case []any:
			i, _ := strconv.Atoi(tok)
			at = indexPath(at, i)
			if i >= 0 && i < len(t) {
				v = t[i]
			}
		case map[string]any:
			at, v = joinPath(at, tok), t[tok]
		default:
			at = joinPath(at, tok)
		}
	}
	return at
}
