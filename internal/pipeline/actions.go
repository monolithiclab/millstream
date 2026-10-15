package pipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/monolithiclab/millstream/internal/schema"
)

// Action kinds (spec section 6.1).
const (
	KindExec      = "exec"
	KindContainer = "container"
	KindAgent     = "agent"
)

// Action effects: external means the action changes something outside Millstream (spec section 6.1).
const (
	EffectsNone     = "none"
	EffectsExternal = "external"
)

// Action process lifecycles (spec section 6.5).
const (
	LifecycleLong    = "long"
	LifecyclePerItem = "perItem"
)

// Builtins: steps the engine runs itself (spec section 4.7).
const (
	BuiltinFilter = "filter"
	BuiltinDedupe = "dedupe"
	BuiltinReview = "review"
)

// IsBuiltin reports whether an action reference names a builtin.
func IsBuiltin(ref string) bool {
	return ref == BuiltinFilter || ref == BuiltinDedupe || ref == BuiltinReview
}

// ManifestFile is the manifest at the root of an action directory.
const ManifestFile = "action.yaml"

// ErrUnavailable marks a reference this build or this machine recognises but cannot resolve, such as a container
// action without Docker or Podman. Check reports it as a warning rather than an unknown action.
var ErrUnavailable = errors.New("not available in this build")

// Action is a resolved action reference: a builtin, or an action read from its manifest.
type Action struct {
	// Ref is the reference as the step wrote it.
	Ref string
	// Builtin is true for filter, dedupe and review; the manifest fields are then empty.
	Builtin     bool
	Name        string
	Version     string
	Description string
	Kind        string // exec, container or agent
	Effects     string // none, external, or empty when the manifest does not say
	Lifecycle   string // long or perItem
	// Dir is the action directory, slash-separated and relative to the package.
	Dir string
	// Input, Output and Config are the action's JSON Schema documents, nil when the manifest declares none.
	Input, Output, Config []byte
	// Command is runtime.exec.command for exec actions, its program resolved to a path on disk when it is relative,
	// or runtime.container.command for container actions, run in the image as written (empty: the image's own).
	Command []string
	// Image is a container action's image reference (repository and tag): runtime.container.image for a local
	// action, the resolved version's image for a registry action.
	Image string
	// Pin is how the image runs, pinned (repository@digest, or the image id), and ImageDigest the digest the lock
	// file records.
	Pin, ImageDigest string
	// Network is true when a container action's manifest declares network destinations (runtime.container.network).
	Network bool
	// Unpinned is true when the lock file does not pin this container action: it runs the image found now.
	Unpinned bool
	// Agent is an agent action's runtime.agent, nil for other kinds.
	Agent *AgentRuntime
}

// ActionResolver turns a step's action reference into an Action. Implementations report a reference they know but
// cannot serve yet by wrapping ErrUnavailable.
type ActionResolver interface {
	Resolve(pkg *Package, ref string) (*Action, error)
}

// Resolver is the default ActionResolver: builtins, local actions under the package (./actions/x), and registry
// references (lab/x@1), which it hands to Registry. Container actions resolve their image through Images and pin it
// against the package's lock file.
type Resolver struct {
	// Platform is the <os>-<arch> of compiled local actions under bin/; empty means the running platform.
	Platform string
	// Registry resolves registry references; nil reports them as unavailable.
	Registry ActionResolver
	// Images inspects local container images; nil reports container actions as unavailable.
	Images Images
	// IgnoreLock resolves container images without comparing them to the lock file: millstream lock writes it.
	IgnoreLock bool
}

// registryRef is the grammar of a registry reference, namespace/name@range (spec section 6.2), and namespace the
// grammar of its namespace, which keys the environment file's registries.
var (
	registryRef = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*/[a-z0-9]+(-[a-z0-9]+)*@[^\s@]+$`)
	namespace   = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
)

// Resolve implements ActionResolver.
func (r Resolver) Resolve(pkg *Package, ref string) (*Action, error) {
	switch {
	case IsBuiltin(ref):
		return &Action{Ref: ref, Builtin: true, Name: ref}, nil
	case strings.HasPrefix(ref, "./"):
		return r.local(pkg, ref)
	case registryRef.MatchString(ref):
		if r.Registry == nil {
			return nil, fmt.Errorf("registry action %s: no registries are configured: %w", ref, ErrUnavailable)
		}
		return r.Registry.Resolve(pkg, ref)
	default:
		return nil, fmt.Errorf("unknown action %q: want a builtin (filter, dedupe, review), a local path "+
			"(./actions/x) or a registry reference (namespace/name@version)", ref)
	}
}

// local reads the manifest of the action directory ref names inside the package.
func (r Resolver) local(pkg *Package, ref string) (*Action, error) {
	dir := path.Clean(strings.TrimPrefix(ref, "./"))
	if dir == "." || dir == ".." || strings.HasPrefix(dir, "../") {
		return nil, fmt.Errorf("action %s: a local action must be a directory inside the package", ref)
	}
	manifest := dir + "/" + ManifestFile
	data, ok := pkg.Files[manifest]
	if !ok {
		return nil, fmt.Errorf("action %s: no %s in the package", ref, manifest)
	}
	a, schemas, err := parseManifest(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", manifest, err)
	}
	a.Ref, a.Dir = ref, dir
	if err := r.finish(pkg, a, schemas); err != nil {
		return nil, fmt.Errorf("%s: %w", manifest, err)
	}
	if a.Kind == KindContainer {
		if a.Image == "" {
			return nil, fmt.Errorf("%s: runtime.container.image: a local container action names its image", manifest)
		}
		if r.Images == nil {
			return nil, fmt.Errorf("action %s: container actions need Docker or Podman: %w", ref, ErrUnavailable)
		}
		if err := r.pin(pkg, a, a.Image); err != nil {
			return nil, fmt.Errorf("action %s: %w", ref, err)
		}
	}
	return a, nil
}

// ParseImageManifest reads a registry action's manifest from its image label: the manifest of a container action,
// with inline schemas only, since there is no package to read schema files from.
func ParseImageManifest(data []byte) (*Action, error) {
	a, schemas, err := parseManifest(data)
	if err != nil {
		return nil, err
	}
	if a.Kind != KindContainer {
		return nil, fmt.Errorf("kind %s: a registry action is a container action", a.Kind)
	}
	if err := (Resolver{}).finish(&Package{Files: map[string][]byte{}}, a, schemas); err != nil {
		return nil, err
	}
	return a, nil
}

// parseManifest reads action.yaml. Its schemas are returned as written (input, output, config: a {$ref: path} or
// an inline schema) for finish to resolve against the package.
func parseManifest(data []byte) (*Action, map[string]any, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, nil, fmt.Errorf("parse YAML: %w", err)
	}
	if len(doc.Content) == 0 {
		return nil, nil, errors.New("empty manifest")
	}
	f, err := mapping(doc.Content[0], "")
	if err != nil {
		return nil, nil, err
	}
	if err := f.allow("name", "version", "description", "kind", "effects", "input", "output", "config",
		"lifecycle", "runtime"); err != nil {
		return nil, nil, err
	}
	m, schemas := &Action{}, map[string]any{}
	var rt map[string]any
	for _, err := range []error{
		required(f, "name", &m.Name, nonEmptyStr),
		optional(f, "version", &m.Version, str),
		optional(f, "description", &m.Description, str),
		required(f, "kind", &m.Kind, oneOf(KindExec, KindContainer, KindAgent)),
		optional(f, "effects", &m.Effects, oneOf(EffectsNone, EffectsExternal)),
		optional(f, "lifecycle", &m.Lifecycle, oneOf(LifecycleLong, LifecyclePerItem)),
		optional(f, "runtime", &rt, rawMap),
	} {
		if err != nil {
			return nil, nil, err
		}
	}
	for _, k := range []string{"input", "output", "config"} {
		var v map[string]any
		if err := optional(f, k, &v, rawMap); err != nil {
			return nil, nil, err
		}
		if v != nil {
			schemas[k] = v
		}
	}
	if m.Lifecycle == "" {
		m.Lifecycle = LifecycleLong
		if m.Kind == KindAgent {
			m.Lifecycle = LifecyclePerItem
		}
	}
	switch m.Kind {
	case KindExec:
		if m.Command, err = execCommand(rt); err != nil {
			return nil, nil, err
		}
	case KindContainer:
		if err := containerRuntime(rt, m); err != nil {
			return nil, nil, err
		}
	case KindAgent:
		if m.Agent, err = agentRuntime(rt); err != nil {
			return nil, nil, err
		}
	}
	return m, schemas, nil
}

// containerRuntime reads runtime.container: image (a string), command (a list of strings) and network (a list of
// destinations), all optional here.
func containerRuntime(rt map[string]any, m *Action) error {
	c, _ := rt[KindContainer].(map[string]any)
	for _, k := range slices.Sorted(maps.Keys(c)) {
		if k != "image" && k != "command" && k != "network" {
			return fmt.Errorf("runtime.container.%s: unknown key", k)
		}
	}
	if v, ok := c["image"]; ok {
		s, ok := v.(string)
		if !ok || s == "" {
			return errors.New("runtime.container.image: must be a non-empty string")
		}
		m.Image = s
	}
	var err error
	if m.Command, err = stringList("runtime.container.command", c["command"]); err != nil {
		return err
	}
	network, err := stringList("runtime.container.network", c["network"])
	m.Network = len(network) > 0
	return err
}

// oneOf reads a string that must be one of allowed.
func oneOf(allowed ...string) func(*yaml.Node, string) (string, error) {
	return func(n *yaml.Node, p string) (string, error) {
		s, err := str(n, p)
		if err != nil {
			return "", err
		}
		for _, a := range allowed {
			if s == a {
				return s, nil
			}
		}
		return "", errAt(n, p, "must be one of %s", strings.Join(allowed, ", "))
	}
}

// execCommand reads runtime.exec.command: a non-empty list of strings.
func execCommand(rt map[string]any) ([]string, error) {
	execRT, _ := rt[KindExec].(map[string]any)
	cmd, err := stringList("runtime.exec.command", execRT["command"])
	if err == nil && len(cmd) == 0 {
		err = errors.New("runtime.exec.command: an exec action needs a command, a non-empty list of strings")
	}
	return cmd, err
}

// stringList reads an optional list of non-empty strings at the key path at.
func stringList(at string, v any) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: must be a list of strings", at)
	}
	out := make([]string, len(list))
	for i, e := range list {
		s, ok := e.(string)
		if !ok || s == "" {
			return nil, fmt.Errorf("%s[%d]: must be a non-empty string", at, i)
		}
		out[i] = s
	}
	return out, nil
}

// finish loads the manifest's schemas and resolves its command, or its agent's prompt, against the action
// directory.
func (r Resolver) finish(pkg *Package, m *Action, schemas map[string]any) error {
	for key, dst := range map[string]*[]byte{"input": &m.Input, "output": &m.Output, "config": &m.Config} {
		v, ok := schemas[key]
		if !ok {
			continue
		}
		doc, err := schemaDoc(pkg, m.Dir, v)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		if _, err := schema.Compile(doc); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		*dst = doc
	}
	if m.Kind == KindAgent {
		return finishAgent(pkg, m)
	}
	if m.Kind != KindExec || len(m.Command) == 0 {
		return nil
	}
	// Resolution (spec section 6.2): a bare name runs bin/<os>-<arch>/<name> when the action ships it, else the
	// program of that name on PATH; a path with a slash is a file of the action directory and must stay inside the
	// package; an absolute path is refused, since a package must not depend on the machine it runs on.
	prog := m.Command[0]
	switch {
	case path.IsAbs(prog) || filepath.IsAbs(prog):
		return fmt.Errorf("runtime.exec.command: %s is an absolute path: name a program on PATH or a file of the "+
			"package", prog)
	case !strings.Contains(prog, "/"):
		bin := "bin/" + r.platform() + "/" + prog
		if _, ok := pkg.Files[m.Dir+"/"+bin]; !ok {
			return nil
		}
		prog = bin
	}
	rel := path.Clean(path.Join(m.Dir, prog))
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return fmt.Errorf("runtime.exec.command: %s leaves the package", prog)
	}
	if _, ok := pkg.Files[rel]; !ok {
		return fmt.Errorf("runtime.exec.command: %s is not in the package", rel)
	}
	m.Command[0] = filepath.Join(pkg.Dir, filepath.FromSlash(rel))
	return nil
}

func (r Resolver) platform() string {
	if r.Platform != "" {
		return r.Platform
	}
	return runtime.GOOS + "-" + runtime.GOARCH
}

// schemaDoc returns a schema as a JSON document: an inline schema as written, or {$ref: path} read from the file
// at path, relative to base. A file may be JSON or YAML.
func schemaDoc(pkg *Package, base string, v any) ([]byte, error) {
	m, _ := v.(map[string]any)
	if ref, ok := m["$ref"].(string); ok && len(m) == 1 && !strings.HasPrefix(ref, "#") {
		p := path.Clean(path.Join(base, ref))
		data, ok := pkg.Files[p]
		if !ok {
			return nil, fmt.Errorf("schema file %s is not in the package", p)
		}
		var decoded any
		if err := yaml.Unmarshal(data, &decoded); err != nil {
			return nil, fmt.Errorf("schema file %s: %w", p, err)
		}
		v = decoded
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("encode schema: %w", err)
	}
	return buf.Bytes(), nil
}
