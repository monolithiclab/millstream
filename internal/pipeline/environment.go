package pipeline

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

// EnvironmentFile is the environment file's default name (spec section 14). It describes where packages run, so
// it is never part of a package, even when it sits at the root of one.
const EnvironmentFile = "millstream.yaml"

// Container runtimes (spec section 6.4).
const (
	RuntimeDocker = "docker"
	RuntimePodman = "podman"
)

// Environment is the environment file: the keys this build reads (spec section 14).
type Environment struct {
	// Registries maps a registry namespace to an OCI repository prefix (lab: ghcr.io/monolithiclab/actions).
	Registries map[string]string
	// ContainerRuntime is runtime.container: docker (the default) or podman.
	ContainerRuntime string
	// Prices are the token prices of agent models, by model (used from the agent adapter on).
	Prices map[string]Price
	// AllowUnsandboxed is agents.allowUnsandboxed: agents may run without a container sandbox.
	AllowUnsandboxed bool
	// AgentImage is agents.image: the image the agent sandbox runs (spec section 7.3). The adapter is mounted
	// into it, so it needs no Millstream of its own, only what the harness needs (CA certificates; the claude
	// CLI for claude-code). Empty means no sandbox can run.
	AgentImage string
	// AgentBinary is agents.linuxBinary: a Linux build of millstream for the sandbox's platform, mounted into it as
	// the adapter. Relative to the environment file; empty means the running binary, on Linux only.
	AgentBinary string
	// LessonsMaxTokens is lessons.maxTokens: the size limit of a lessons file, in approximate tokens (spec section
	// 11.4); zero means DefaultLessonsMaxTokens, which Check applies.
	LessonsMaxTokens int
	// UIBaseURL is ui.baseUrl: where the web UI is reached, for the links review notifications carry; empty means
	// the local default (review.DefaultBaseURL).
	UIBaseURL string
	// UIListen is ui.listen: the address millstream run serves the API and the web UI on; empty means
	// 127.0.0.1:7070. Until token authentication (M4) it must be a loopback address.
	UIListen string
}

// Price is a model's price per million tokens, in US dollars.
type Price struct {
	InPerMtok  float64
	OutPerMtok float64
}

// laterEnvironmentKeys are keys of the environment file in spec section 14 that this build does not read yet.
var laterEnvironmentKeys = []string{"environment", "store", "artifacts", "listen", "auth", "node", "secrets",
	"retention", "improve"}

// LoadEnvironment reads the environment file at path. A missing file is an empty environment when required is
// false, an error otherwise.
func LoadEnvironment(path string, required bool) (*Environment, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- the operator names the environment file
	if errors.Is(err, fs.ErrNotExist) && !required {
		return &Environment{ContainerRuntime: RuntimeDocker}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("environment file: %w", err)
	}
	env, err := ParseEnvironment(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if env.AgentBinary != "" && !filepath.IsAbs(env.AgentBinary) {
		env.AgentBinary = filepath.Join(filepath.Dir(path), env.AgentBinary)
	}
	return env, nil
}

// ParseEnvironment parses an environment file strictly: an unknown key, or a key of spec section 14 this build
// does not read yet, is an error located by its key path.
func ParseEnvironment(data []byte) (*Environment, error) {
	env := &Environment{ContainerRuntime: RuntimeDocker}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}
	if len(doc.Content) == 0 {
		return env, nil
	}
	f, err := mapping(doc.Content[0], "")
	if err != nil {
		return nil, err
	}
	for _, k := range f.keys {
		if slices.Contains(laterEnvironmentKeys, k.Value) {
			return nil, errAt(k, k.Value, "is not supported by this build yet")
		}
	}
	if err := f.allow("registries", "runtime", "prices", "agents", "lessons", "ui"); err != nil {
		return nil, err
	}
	for _, err := range []error{
		optional(f, "registries", &env.Registries, registries),
		readRuntime(f, env),
		optional(f, "prices", &env.Prices, prices),
		readAgents(f, env),
		readLessons(f, env),
		readUI(f, env),
	} {
		if err != nil {
			return nil, err
		}
	}
	return env, nil
}

// registries reads registries: a mapping from namespace to OCI repository prefix.
func registries(n *yaml.Node, path string) (map[string]string, error) {
	f, err := mapping(n, path)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(f.keys))
	for _, k := range f.keys {
		at := f.sub(k.Value)
		if !namespace.MatchString(k.Value) {
			return nil, errAt(k, at, "a namespace is lowercase letters, digits and dashes")
		}
		prefix, err := nonEmptyStr(f.get(k.Value), at)
		if err != nil {
			return nil, err
		}
		prefix = strings.TrimSuffix(prefix, "/")
		if strings.Contains(prefix, "://") || strings.ContainsAny(prefix, "@ \t") {
			return nil, errAt(f.get(k.Value), at, "want a repository prefix such as ghcr.io/org/actions, without "+
				"a scheme or digest")
		}
		out[k.Value] = prefix
	}
	return out, nil
}

func readRuntime(f *fields, env *Environment) error {
	n := f.get("runtime")
	if isNull(n) {
		return nil
	}
	rf, err := mapping(n, f.sub("runtime"))
	if err != nil {
		return err
	}
	if err := rf.allow("container"); err != nil {
		return err
	}
	return optional(rf, "container", &env.ContainerRuntime, oneOf(RuntimeDocker, RuntimePodman))
}

func prices(n *yaml.Node, path string) (map[string]Price, error) {
	f, err := mapping(n, path)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Price, len(f.keys))
	for _, k := range f.keys {
		pf, err := mapping(f.get(k.Value), f.sub(k.Value))
		if err != nil {
			return nil, err
		}
		if err := pf.allow("inPerMtok", "outPerMtok"); err != nil {
			return nil, err
		}
		var p Price
		if err := required(pf, "inPerMtok", &p.InPerMtok, nonNegativeNumber); err != nil {
			return nil, err
		}
		if err := required(pf, "outPerMtok", &p.OutPerMtok, nonNegativeNumber); err != nil {
			return nil, err
		}
		out[k.Value] = p
	}
	return out, nil
}

func readAgents(f *fields, env *Environment) error {
	n := f.get("agents")
	if isNull(n) {
		return nil
	}
	af, err := mapping(n, f.sub("agents"))
	if err != nil {
		return err
	}
	if err := af.allow("allowUnsandboxed", "image", "linuxBinary"); err != nil {
		return err
	}
	for _, err := range []error{
		optional(af, "allowUnsandboxed", &env.AllowUnsandboxed, boolean),
		optional(af, "image", &env.AgentImage, nonEmptyStr),
		optional(af, "linuxBinary", &env.AgentBinary, nonEmptyStr),
	} {
		if err != nil {
			return err
		}
	}
	return nil
}

func readLessons(f *fields, env *Environment) error {
	n := f.get("lessons")
	if isNull(n) {
		return nil
	}
	lf, err := mapping(n, f.sub("lessons"))
	if err != nil {
		return err
	}
	if err := lf.allow("maxTokens"); err != nil {
		return err
	}
	return optional(lf, "maxTokens", &env.LessonsMaxTokens, positiveInt)
}

func readUI(f *fields, env *Environment) error {
	n := f.get("ui")
	if isNull(n) {
		return nil
	}
	uf, err := mapping(n, f.sub("ui"))
	if err != nil {
		return err
	}
	if err := uf.allow("baseUrl", "listen"); err != nil {
		return err
	}
	if err := optional(uf, "listen", &env.UIListen, nonEmptyStr); err != nil {
		return err
	}
	if err := optional(uf, "baseUrl", &env.UIBaseURL, nonEmptyStr); err != nil {
		return err
	}
	if env.UIBaseURL != "" && !httpURL(env.UIBaseURL) {
		return errAt(uf.get("baseUrl"), uf.sub("baseUrl"), "want an http or https URL such as https://millstream.example")
	}
	return nil
}
