package pipeline

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
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
	// Store is the store key: the log store the commands and the engine use (spec section 14).
	Store StoreConfig
}

// StoreConfig is the store key: SQLite at a path, or Postgres at a DSN read from a source, never written in the
// file. The zero value, no store key, is SQLite at the --db path.
type StoreConfig struct {
	// SQLitePath is store.sqlite.path, relative to the environment file.
	SQLitePath string
	// PostgresDSN is store.postgres.dsn: where the DSN is read from.
	PostgresDSN *SecretSource
}

// Set reports whether the store key is there.
func (c StoreConfig) Set() bool { return c.SQLitePath != "" || c.PostgresDSN != nil }

// Path is where the SQLite database lives: the store key's path, else defaultPath (the --db flag). With Postgres,
// it is defaultPath, whose directory holds the local artifact store until the artifacts key arrives.
func (c StoreConfig) Path(defaultPath string) string {
	if c.SQLitePath != "" {
		return c.SQLitePath
	}
	return defaultPath
}

// SecretSource is where a secret's value is read from: an environment variable ({ env: NAME }) or a file
// ({ file: path }, relative to the environment file), such as a container secret's mount.
type SecretSource struct {
	Env  string
	File string
}

// Value reads the secret: the variable, which must be set and not empty, through lookup, or the file, without its
// trailing newline. Errors name the source, never the value.
func (s SecretSource) Value(lookup func(string) (string, bool)) (string, error) {
	if s.Env != "" {
		v, ok := lookup(s.Env)
		if !ok || v == "" {
			return "", fmt.Errorf("environment variable %s is not set", s.Env)
		}
		return v, nil
	}
	b, err := os.ReadFile(s.File) // #nosec G304 -- the operator names the secret's file
	if err != nil {
		return "", fmt.Errorf("read secret file: %w", err)
	}
	v := strings.TrimRight(string(b), "\r\n")
	if v == "" {
		return "", fmt.Errorf("secret file %s is empty", s.File)
	}
	return v, nil
}

// Price is a model's price per million tokens, in US dollars.
type Price struct {
	InPerMtok  float64
	OutPerMtok float64
}

// laterEnvironmentKeys are keys of the environment file in spec section 14 that this build does not read yet.
var laterEnvironmentKeys = []string{"environment", "artifacts", "listen", "auth", "node", "secrets", "retention",
	"improve"}

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
	abs := func(p *string) {
		if *p != "" && !filepath.IsAbs(*p) {
			*p = filepath.Join(filepath.Dir(path), *p)
		}
	}
	abs(&env.AgentBinary)
	abs(&env.Store.SQLitePath)
	if env.Store.PostgresDSN != nil {
		abs(&env.Store.PostgresDSN.File)
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
	if err := f.allow("registries", "runtime", "prices", "agents", "lessons", "ui", "store"); err != nil {
		return nil, err
	}
	for _, err := range []error{
		optional(f, "registries", &env.Registries, registries),
		readRuntime(f, env),
		optional(f, "prices", &env.Prices, prices),
		readAgents(f, env),
		readLessons(f, env),
		readUI(f, env),
		readStore(f, env),
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

// readStore reads store: exactly one of sqlite: { path } and postgres: { dsn: <secret source> }.
func readStore(f *fields, env *Environment) error {
	n := f.get("store")
	if isNull(n) {
		return nil
	}
	sf, err := mapping(n, f.sub("store"))
	if err != nil {
		return err
	}
	if err := sf.allow("sqlite", "postgres"); err != nil {
		return err
	}
	if len(sf.keys) != 1 {
		return errAt(sf.node, sf.path, "want exactly one of sqlite and postgres")
	}
	if v := sf.get("sqlite"); v != nil {
		lf, err := mapping(v, sf.sub("sqlite"))
		if err != nil {
			return err
		}
		if err := lf.allow("path"); err != nil {
			return err
		}
		env.Store.SQLitePath, err = lf.requiredStr("path")
		return err
	}
	pf, err := mapping(sf.get("postgres"), sf.sub("postgres"))
	if err != nil {
		return err
	}
	if err := pf.allow("dsn"); err != nil {
		return err
	}
	dsn, err := pf.require("dsn")
	if err != nil {
		return err
	}
	src, err := secretSource(dsn, pf.sub("dsn"))
	if err != nil {
		return err
	}
	env.Store.PostgresDSN = &src
	return nil
}

// secretSource reads a secret's source: { env: NAME } or { file: path }. A value written in the file itself is
// refused: secrets stay out of it.
func secretSource(n *yaml.Node, path string) (SecretSource, error) {
	f, err := mapping(n, path)
	if err != nil {
		return SecretSource{}, errAt(deref(n), path, "want { env: NAME } or { file: path }: a secret is never written in "+
			"the environment file")
	}
	if err := f.allow("env", "file"); err != nil {
		return SecretSource{}, err
	}
	if len(f.keys) != 1 {
		return SecretSource{}, errAt(f.node, path, "want exactly one of env and file")
	}
	var s SecretSource
	if err := optional(f, "env", &s.Env, nonEmptyStr); err != nil {
		return s, err
	}
	if s.Env != "" && !envName.MatchString(s.Env) {
		return s, errAt(f.get("env"), f.sub("env"), "want an environment variable name such as MILLSTREAM_DSN")
	}
	return s, optional(f, "file", &s.File, nonEmptyStr)
}

// envName is an environment variable's name.
var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
