package pipeline

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	pathpkg "path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

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
	// 127.0.0.1:7070. It must be a loopback address: only serve listens elsewhere, with auth.token.
	UIListen string
	// Store is the store key: the log store the commands and the engine use (spec section 14).
	Store StoreConfig
	// Mode is the environment key: production or development (spec section 11.2); empty means the command's
	// default (EffectiveMode).
	Mode string
	// Artifacts is the artifacts key: where artifacts are kept (spec section 9.2).
	Artifacts ArtifactsConfig
	// Listen is the listen key: the address serve (and run, after ui.listen) serves the API and the UI on.
	Listen string
	// AuthToken is auth.token: where the API's bearer token is read from; nil means no token, and the API serves
	// loopback clients only. A non-loopback listener requires it (spec section 15.2).
	AuthToken *SecretSource
	// AuthUser is auth.user: who decides the reviews of a token session (single user in the open-source edition);
	// empty means DefaultAuthUser.
	AuthUser string
	// TLS is the tls key: the certificate the engine serves its listener with, instead of plain HTTP behind a
	// TLS-terminating proxy; nil means plain HTTP.
	TLS *TLSConfig
	// MetricsPublic is metrics.public: /metrics answers without the token.
	MetricsPublic bool
	// NodeEmbedded is node.embedded: whether serve runs a node inside the engine (default true).
	NodeEmbedded bool
	// NodeToken is node.token: where the node token remote nodes join with is read from (M4 Task 4).
	NodeToken *SecretSource
	// Secrets maps each pipeline to its secrets' sources, by secret name: the grant of section 4.5. Nil when the
	// file has no secrets key: secrets then resolve from MILLSTREAM_SECRET_<NAME>, as without an environment file.
	Secrets map[string]map[string]SecretSource
	// Retention is the retention key (spec section 9.3).
	Retention Retention
	// Improve is the improve key (spec sections 11.2 and 11.3), kept for the improve pipeline (M5).
	Improve Improve
	// Engine is the engine key: how an engine on Postgres watches its write connection (spec section 5.9).
	Engine EngineConfig
}

// EngineConfig is the engine key; a zero duration means the store's default (5 s each).
type EngineConfig struct {
	// LockCheck is engine.lockCheck: how often an idle engine checks its write connection, and the lock with it.
	LockCheck time.Duration
	// Keepalive is engine.keepalive: the period of the write connection's TCP keepalives, which sets how soon a
	// dead engine's lock is released and a cut-off engine steps down.
	Keepalive time.Duration
}

// DefaultAuthUser is who decides the reviews of a token session without auth.user.
const DefaultAuthUser = "operator"

// TLSConfig is the tls key: a PEM certificate chain and its private key, paths relative to the environment file.
type TLSConfig struct {
	Cert string
	Key  string
}

// Environment modes (spec section 11.2).
const (
	ModeProduction  = "production"
	ModeDevelopment = "development"
)

// EffectiveMode is the environment key, else def: serve defaults to production, run to development.
func (e *Environment) EffectiveMode(def string) string {
	if e.Mode != "" {
		return e.Mode
	}
	return def
}

// ErrProduction marks what a production deployment refuses (spec section 11.2, D31).
var ErrProduction = errors.New("refused in production")

// CheckMode refuses what the mode forbids (spec section 11.2): in production the improve pipeline does not run, so
// improve.enabled and improve.publish: apply are refused. serve checks it with production as the default, run with
// development.
func (e *Environment) CheckMode(def string) error {
	if e.EffectiveMode(def) != ModeProduction {
		return nil
	}
	switch {
	case e.Improve.Enabled:
		return fmt.Errorf("improve.enabled: the improve pipeline runs in development deployments only: %w",
			ErrProduction)
	case e.Improve.Publish == PublishApply:
		return fmt.Errorf("improve.publish: apply is %w: set environment: development, or another publisher",
			ErrProduction)
	}
	return nil
}

// RequireDevelopment refuses what only a development deployment does, such as running the improve pipeline (what),
// in a production one; def is the command's default mode.
func (e *Environment) RequireDevelopment(def, what string) error {
	if e.EffectiveMode(def) == ModeProduction {
		return fmt.Errorf("%s runs in development deployments only (environment: production): %w", what,
			ErrProduction)
	}
	return nil
}

// SecretLookup returns how a pipeline's secrets resolve (spec section 4.5): through the secrets key, which is also
// the grant, when the file has one, an unmapped name resolving to nothing; else from MILLSTREAM_SECRET_<NAME>
// through env. The error of a mapped source that cannot be read names the source, never a value.
func (e *Environment) SecretLookup(pipelineName string, env func(string) (string, bool)) SecretLookup {
	if e == nil || e.Secrets == nil {
		return func(name string) (string, error) {
			if v, ok := env(SecretEnv(name)); ok {
				return v, nil
			}
			return "", fmt.Errorf("secret %s is not set: set %s", name, SecretEnv(name))
		}
	}
	sources := e.Secrets[pipelineName]
	return func(name string) (string, error) {
		src, ok := sources[name]
		if !ok {
			return "", fmt.Errorf("secret %s is not mapped for pipeline %s: map it under secrets.%s in the environment "+
				"file", name, pipelineName, pipelineName)
		}
		v, err := src.Value(env)
		if err != nil {
			return "", fmt.Errorf("secret %s: %w", name, err)
		}
		return v, nil
	}
}

// SecretLookupFor is SecretLookup for every pipeline: what CheckOptions.Secrets and the engine take.
func (e *Environment) SecretLookupFor(env func(string) (string, bool)) func(pipelineName string) SecretLookup {
	return func(pipelineName string) SecretLookup { return e.SecretLookup(pipelineName, env) }
}

// SecretEnvLookup is env with the variables MILLSTREAM_SECRET_<NAME> of a pipeline's secrets resolved through the
// secrets key, when the file has one: what a tool that hands secrets to an action through its environment (eval,
// the agent adapter) looks up. An unmapped or unreadable secret is unset.
func (e *Environment) SecretEnvLookup(pipelineName string, env func(string) (string, bool)) func(string) (string, bool) {
	if e == nil || e.Secrets == nil {
		return env
	}
	lookup := e.SecretLookup(pipelineName, env)
	return func(k string) (string, bool) {
		if !strings.HasPrefix(k, "MILLSTREAM_SECRET_") {
			return env(k)
		}
		for name := range e.Secrets[pipelineName] {
			if SecretEnv(name) == k {
				v, err := lookup(name)
				return v, err == nil
			}
		}
		return "", false
	}
}

// SecretLookup resolves a secret of one pipeline by name.
type SecretLookup func(name string) (string, error)

// EnvSecrets resolves secrets from MILLSTREAM_SECRET_<NAME> through env: the lookup without an environment file.
func EnvSecrets(env func(string) (string, bool)) func(pipelineName string) SecretLookup {
	return (*Environment)(nil).SecretLookupFor(env)
}

// ArtifactsConfig is the artifacts key: a local directory, or an S3-compatible bucket. The zero value is the local
// directory next to the state database.
type ArtifactsConfig struct {
	// LocalPath is artifacts.local.path, relative to the environment file.
	LocalPath string
	// S3 is artifacts.s3; nil without it. The S3 store itself arrives with remote nodes (M4 Task 4).
	S3 *S3Config
}

// S3Config is artifacts.s3: a bucket of an S3-compatible store, its credentials read from sources.
type S3Config struct {
	Bucket string
	// Endpoint is the store's URL; empty means AWS's endpoint for Region.
	Endpoint string
	Region   string
	// Prefix is the key prefix under which artifacts are kept.
	Prefix string
	// AccessKeyID and SecretAccessKey are where the credentials are read from; nil means the AWS_ACCESS_KEY_ID and
	// AWS_SECRET_ACCESS_KEY variables.
	AccessKeyID     *SecretSource
	SecretAccessKey *SecretSource
}

// Retention is the retention key: how long events, agent transcripts and metric rollups are kept, zero for ever,
// and how often the engine's retention job runs, zero for its default (spec section 9.3).
type Retention struct {
	Events      time.Duration
	Transcripts time.Duration
	Rollups     time.Duration
	Every       time.Duration
}

// Publishers of accepted proposals (spec section 11.3).
const (
	PublishApply     = "apply"
	PublishPatchFile = "patch-file"
	PublishGitPRGH   = "git-pr-gh"
	PublishGitPRGLab = "git-pr-glab"
)

// Improve is the improve key (spec sections 11.2 to 11.6): the improve pipeline's settings, parsed now and used from
// M5 on. Zero values mean the spec's defaults.
type Improve struct {
	Enabled bool
	// Harness is the agent harness drafting proposals: claude-code or api.
	Harness     string
	MinSeverity string
	// Publish is the publisher of accepted proposals: apply, patch-file, git-pr-gh or git-pr-glab.
	Publish string
	// PatchFile is improve.patchFile, the patch-file action's settings; Git is improve.git, the git actions'.
	PatchFile    PatchFileConfig
	Git          GitConfig
	BudgetDayUSD float64
	// EvidenceSample and EvidenceTTL are improve.evidence: the items an evidence bundle samples, and its expiry.
	EvidenceSample int
	EvidenceTTL    time.Duration
	// BacktestSample is improve.backtest.sample.
	BacktestSample int
	// Distill is improve.distill (section 11.5).
	DistillMinDeliveries int
	DistillLanguage      string
	DistillMinAgreement  float64
	DistillMinCoverage   float64
	// FilterMaxFalseDrop is improve.filter.maxFalseDrop.
	FilterMaxFalseDrop float64
}

// PublishActions are the publish actions improve.publish may name.
var PublishActions = []string{PublishApply, PublishPatchFile, PublishGitPRGH, PublishGitPRGLab}

// PatchFileConfig is improve.patchFile.
type PatchFileConfig struct {
	// Dir is where the patch-file action writes each accepted proposal's patch, relative to the environment file.
	Dir string
}

// GitConfig is improve.git: the repository the git-pr-gh and git-pr-glab actions open pull requests on. Millstream
// holds no forge credential (D32): Env names the variables of the engine's environment passed to the action's
// container, such as GH_TOKEN or GITLAB_TOKEN, and nothing else reaches it.
type GitConfig struct {
	// Repo is the repository's clone URL; Base the branch pull requests target (default main).
	Repo string
	Base string
	// Path is the package's directory in the repository (default the root); {pipeline} stands for the pipeline's
	// name.
	Path string
	// Image is the action's container image, with git and the forge's CLI, pinned by digest.
	Image string
	Env   []string
	// AuthorName and AuthorEmail sign the commits (default Millstream, millstream@localhost).
	AuthorName  string
	AuthorEmail string
}

// DefaultGitBase is improve.git.base's default.
const DefaultGitBase = "main"

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

// MinTokenLength is the fewest characters auth.token and node.token may hold (spec section 15.2): a deployment's
// tokens are long random strings, such as openssl rand -hex 32 prints.
const MinTokenLength = 32

// CheckToken refuses a token shorter than MinTokenLength; key names it in the error, never its value.
func CheckToken(key, token string) error {
	if n := utf8.RuneCountInString(token); n < MinTokenLength {
		return fmt.Errorf("%s: must be at least %d characters long, not %d", key, MinTokenLength, n)
	}
	return nil
}

// Token reads a token through Value and checks its length (CheckToken); errors are prefixed with key.
func (s SecretSource) Token(key string, lookup func(string) (string, bool)) (string, error) {
	v, err := s.Value(lookup)
	if err != nil {
		return "", fmt.Errorf("%s: %w", key, err)
	}
	if err := CheckToken(key, v); err != nil {
		return "", err
	}
	return v, nil
}

// Price is a model's price per million tokens, in US dollars.
type Price struct {
	InPerMtok  float64
	OutPerMtok float64
}

// LoadEnvironment reads the environment file at path. A missing file is an empty environment when required is
// false, an error otherwise.
func LoadEnvironment(path string, required bool) (*Environment, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- the operator names the environment file
	if errors.Is(err, fs.ErrNotExist) && !required {
		return newEnvironment(), nil
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
	abs(&env.Artifacts.LocalPath)
	abs(&env.Improve.PatchFile.Dir)
	if env.TLS != nil {
		abs(&env.TLS.Cert)
		abs(&env.TLS.Key)
	}
	for _, src := range env.sources() {
		abs(&src.File)
	}
	for _, sources := range env.Secrets {
		for name, src := range sources {
			abs(&src.File)
			sources[name] = src
		}
	}
	return env, nil
}

// newEnvironment is the environment without a file: every key at its default.
func newEnvironment() *Environment {
	return &Environment{ContainerRuntime: RuntimeDocker, NodeEmbedded: true}
}

// sources returns the secret sources of the file but the pipelines' secrets, for LoadEnvironment to resolve their
// files.
func (e *Environment) sources() []*SecretSource {
	out := []*SecretSource{e.Store.PostgresDSN, e.AuthToken, e.NodeToken}
	if s3 := e.Artifacts.S3; s3 != nil {
		out = append(out, s3.AccessKeyID, s3.SecretAccessKey)
	}
	return slices.DeleteFunc(out, func(s *SecretSource) bool { return s == nil })
}

// ParseEnvironment parses an environment file strictly: an unknown key is an error located by its key path.
func ParseEnvironment(data []byte) (*Environment, error) {
	env := newEnvironment()
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
	if err := f.allow("environment", "store", "artifacts", "listen", "auth", "tls", "metrics", "node", "registries",
		"secrets", "retention", "lessons", "ui", "improve", "prices", "runtime", "agents", "engine"); err != nil {
		return nil, err
	}
	for _, err := range []error{
		optional(f, "environment", &env.Mode, oneOf(ModeProduction, ModeDevelopment)),
		readStore(f, env),
		readArtifacts(f, env),
		optional(f, "listen", &env.Listen, hostPort),
		readAuth(f, env),
		readTLS(f, env),
		section(f, "metrics", []string{"public"}, func(mf *fields) []error {
			return []error{optional(mf, "public", &env.MetricsPublic, boolean)}
		}),
		readNode(f, env),
		optional(f, "registries", &env.Registries, registries),
		optional(f, "secrets", &env.Secrets, secretsByPipeline),
		readRetention(f, env),
		readLessons(f, env),
		readUI(f, env),
		readImprove(f, env),
		optional(f, "prices", &env.Prices, prices),
		readRuntime(f, env),
		readAgents(f, env),
		readEngine(f, env),
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

// hostPort reads a listen address: host:port.
func hostPort(n *yaml.Node, path string) (string, error) {
	s, err := nonEmptyStr(n, path)
	if err != nil {
		return "", err
	}
	if _, _, err := net.SplitHostPort(s); err != nil {
		return "", errAt(n, path, "want host:port, such as 127.0.0.1:7070")
	}
	return s, nil
}

// readArtifacts reads artifacts: exactly one of local: { path } and s3: { bucket, endpoint, region, prefix,
// accessKeyId, secretAccessKey }.
func readArtifacts(f *fields, env *Environment) error {
	n := f.get("artifacts")
	if isNull(n) {
		return nil
	}
	af, err := mapping(n, f.sub("artifacts"))
	if err != nil {
		return err
	}
	if err := af.allow("local", "s3"); err != nil {
		return err
	}
	if len(af.keys) != 1 {
		return errAt(af.node, af.path, "want exactly one of local and s3")
	}
	if v := af.get("local"); v != nil {
		lf, err := mapping(v, af.sub("local"))
		if err != nil {
			return err
		}
		if err := lf.allow("path"); err != nil {
			return err
		}
		env.Artifacts.LocalPath, err = lf.requiredStr("path")
		return err
	}
	sf, err := mapping(af.get("s3"), af.sub("s3"))
	if err != nil {
		return err
	}
	if err := sf.allow("bucket", "endpoint", "region", "prefix", "accessKeyId", "secretAccessKey"); err != nil {
		return err
	}
	s3 := &S3Config{}
	for _, err := range []error{
		required(sf, "bucket", &s3.Bucket, nonEmptyStr),
		optional(sf, "endpoint", &s3.Endpoint, nonEmptyStr),
		optional(sf, "region", &s3.Region, nonEmptyStr),
		optional(sf, "prefix", &s3.Prefix, nonEmptyStr),
		optionalSource(sf, "accessKeyId", &s3.AccessKeyID),
		optionalSource(sf, "secretAccessKey", &s3.SecretAccessKey),
	} {
		if err != nil {
			return err
		}
	}
	if s3.Endpoint != "" && !httpURL(s3.Endpoint) {
		return errAt(sf.get("endpoint"), sf.sub("endpoint"), "want an http or https URL such as https://s3.fr-par.scw.cloud")
	}
	if (s3.AccessKeyID == nil) != (s3.SecretAccessKey == nil) {
		return errAt(sf.node, sf.path, "set both accessKeyId and secretAccessKey, or neither")
	}
	env.Artifacts.S3 = s3
	return nil
}

// optionalSource reads key as a secret source when it is set.
func optionalSource(f *fields, key string, dst **SecretSource) error {
	v := f.get(key)
	if isNull(v) {
		return nil
	}
	src, err := secretSource(v, f.sub(key))
	if err != nil {
		return err
	}
	*dst = &src
	return nil
}

func readAuth(f *fields, env *Environment) error {
	return section(f, "auth", []string{"token", "user"}, func(af *fields) []error {
		if _, err := af.require("token"); err != nil {
			return []error{err}
		}
		return []error{optionalSource(af, "token", &env.AuthToken), optional(af, "user", &env.AuthUser, nonEmptyStr)}
	})
}

// readTLS reads tls: { cert, key }, both required.
func readTLS(f *fields, env *Environment) error {
	return section(f, "tls", []string{"cert", "key"}, func(tf *fields) []error {
		var t TLSConfig
		var err error
		if t.Cert, err = tf.requiredStr("cert"); err != nil {
			return []error{err}
		}
		if t.Key, err = tf.requiredStr("key"); err != nil {
			return []error{err}
		}
		env.TLS = &t
		return nil
	})
}

func readNode(f *fields, env *Environment) error {
	return section(f, "node", []string{"embedded", "token"}, func(nf *fields) []error {
		return []error{optional(nf, "embedded", &env.NodeEmbedded, boolean), optionalSource(nf, "token", &env.NodeToken)}
	})
}

// secretsByPipeline reads secrets: per pipeline, each secret's source.
func secretsByPipeline(n *yaml.Node, path string) (map[string]map[string]SecretSource, error) {
	f, err := mapping(n, path)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]SecretSource, len(f.keys))
	for _, k := range f.keys {
		pf, err := mapping(f.get(k.Value), f.sub(k.Value))
		if err != nil {
			return nil, err
		}
		sources := make(map[string]SecretSource, len(pf.keys))
		for _, sk := range pf.keys {
			if sources[sk.Value], err = secretSource(pf.get(sk.Value), pf.sub(sk.Value)); err != nil {
				return nil, err
			}
		}
		out[k.Value] = sources
	}
	return out, nil
}

func readRetention(f *fields, env *Environment) error {
	return section(f, "retention", []string{"events", "transcripts", "rollups", "every"}, func(rf *fields) []error {
		return []error{
			optional(rf, "events", &env.Retention.Events, duration),
			optional(rf, "transcripts", &env.Retention.Transcripts, duration),
			optional(rf, "rollups", &env.Retention.Rollups, duration),
			optional(rf, "every", &env.Retention.Every, duration),
		}
	})
}

func readEngine(f *fields, env *Environment) error {
	return section(f, "engine", []string{"lockCheck", "keepalive"}, func(ef *fields) []error {
		return []error{
			optional(ef, "lockCheck", &env.Engine.LockCheck, duration),
			optional(ef, "keepalive", &env.Engine.Keepalive, duration),
		}
	})
}

// section reads the mapping under key, when it is set, allowing only the keys given, with read; the first of read's
// errors is returned.
func section(f *fields, key string, allowed []string, read func(*fields) []error) error {
	n := f.get(key)
	if isNull(n) {
		return nil
	}
	sf, err := mapping(n, f.sub(key))
	if err != nil {
		return err
	}
	if err := sf.allow(allowed...); err != nil {
		return err
	}
	for _, err := range read(sf) {
		if err != nil {
			return err
		}
	}
	return nil
}

// fraction reads a number from 0 to 1.
func fraction(n *yaml.Node, path string) (float64, error) {
	v, err := nonNegativeNumber(n, path)
	if err != nil || v > 1 {
		return 0, errAt(deref(n), path, "must be a number from 0 to 1")
	}
	return v, nil
}

// DefaultDistillLanguage is the one improve.distill.language the distill proposal writes code in for now, its default.
const DefaultDistillLanguage = "go"

// distillLanguage reads improve.distill.language: Go alone for now, the only language the sandbox builds and runs
// (spec section 11.5).
func distillLanguage(n *yaml.Node, path string) (string, error) {
	s, err := nonEmptyStr(n, path)
	if err == nil && s != DefaultDistillLanguage {
		err = errAt(n, path, "%q is not supported yet: distilled actions are written in go, the only language the "+
			"candidate-code sandbox builds and runs", s)
	}
	return s, err
}

// readImprove reads improve (spec sections 11.2 to 11.6).
func readImprove(f *fields, env *Environment) error {
	n := f.get("improve")
	if isNull(n) {
		return nil
	}
	imf, err := mapping(n, f.sub("improve"))
	if err != nil {
		return err
	}
	if err := imf.allow("enabled", "harness", "minSeverity", "publish", "patchFile", "git", "budget", "evidence",
		"backtest", "distill", "filter"); err != nil {
		return err
	}
	im := &env.Improve
	sub := func(key string, allowed []string, read func(*fields) []error) error {
		return section(imf, key, allowed, read)
	}
	for _, err := range []error{
		optional(imf, "enabled", &im.Enabled, boolean),
		optional(imf, "harness", &im.Harness, oneOf("claude-code", "api")),
		optional(imf, "minSeverity", &im.MinSeverity, oneOf(Severities...)),
		optional(imf, "publish", &im.Publish, oneOf(PublishActions...)),
		sub("patchFile", []string{"dir"}, func(sf *fields) []error {
			return []error{required(sf, "dir", &im.PatchFile.Dir, nonEmptyStr)}
		}),
		sub("git", []string{"repo", "base", "path", "image", "env", "author"}, func(sf *fields) []error {
			g := &im.Git
			return []error{required(sf, "repo", &g.Repo, nonEmptyStr),
				optional(sf, "base", &g.Base, nonEmptyStr),
				optional(sf, "path", &g.Path, packageDir),
				required(sf, "image", &g.Image, pinnedImage),
				optional(sf, "env", &g.Env, envNames),
				section(sf, "author", []string{"name", "email"}, func(af *fields) []error {
					return []error{optional(af, "name", &g.AuthorName, nonEmptyStr),
						optional(af, "email", &g.AuthorEmail, nonEmptyStr)}
				})}
		}),
		sub("budget", []string{"dayUsd"}, func(sf *fields) []error {
			return []error{optional(sf, "dayUsd", &im.BudgetDayUSD, nonNegativeNumber)}
		}),
		sub("evidence", []string{"sample", "ttl"}, func(sf *fields) []error {
			return []error{optional(sf, "sample", &im.EvidenceSample, positiveInt),
				optional(sf, "ttl", &im.EvidenceTTL, duration)}
		}),
		sub("backtest", []string{"sample"}, func(sf *fields) []error {
			return []error{optional(sf, "sample", &im.BacktestSample, positiveInt)}
		}),
		sub("distill", []string{"minDeliveries", "language", "minAgreement", "minCoverage"}, func(sf *fields) []error {
			return []error{optional(sf, "minDeliveries", &im.DistillMinDeliveries, positiveInt),
				optional(sf, "language", &im.DistillLanguage, distillLanguage),
				optional(sf, "minAgreement", &im.DistillMinAgreement, fraction),
				optional(sf, "minCoverage", &im.DistillMinCoverage, fraction)}
		}),
		sub("filter", []string{"maxFalseDrop"}, func(sf *fields) []error {
			return []error{optional(sf, "maxFalseDrop", &im.FilterMaxFalseDrop, fraction)}
		}),
	} {
		if err != nil {
			return err
		}
	}
	if at := imf.get("publish"); at != nil {
		switch {
		case im.Publish == PublishPatchFile && im.PatchFile.Dir == "":
			return errAt(at, imf.sub("publish"), "patch-file needs improve.patchFile.dir")
		case (im.Publish == PublishGitPRGH || im.Publish == PublishGitPRGLab) && im.Git.Repo == "":
			return errAt(at, imf.sub("publish"), "%s needs improve.git (repo and image)", im.Publish)
		}
	}
	if env.Mode != "" {
		if err := env.CheckMode(""); err != nil {
			return errAt(imf.node, imf.path, "%s", strings.TrimPrefix(err.Error(), "improve."))
		}
	}
	return nil
}

// pinnedImage reads a container image reference pinned by digest (repository@sha256:<64 hex>).
func pinnedImage(n *yaml.Node, path string) (string, error) {
	s, err := nonEmptyStr(n, path)
	if err == nil && !pinnedRef.MatchString(s) {
		err = errAt(deref(n), path, "must be pinned by digest: <repository>@sha256:<64 hex characters>")
	}
	return s, err
}

var pinnedRef = regexp.MustCompile(`^[^\s@]+@sha256:[0-9a-f]{64}$`)

// packageDir reads a clean relative directory, "{pipeline}" standing for a pipeline's name.
func packageDir(n *yaml.Node, path string) (string, error) {
	s, err := nonEmptyStr(n, path)
	if err == nil && (filepath.IsAbs(s) || strings.Contains(s, "\\") || pathpkg.Clean(s) != s ||
		s == ".." || strings.HasPrefix(s, "../")) {
		err = errAt(deref(n), path, "must be a clean relative path inside the repository")
	}
	return s, err
}

// envNames reads a list of environment variable names.
func envNames(n *yaml.Node, path string) ([]string, error) {
	names, err := strSeq(n, path)
	for i, name := range names {
		if err == nil && !envName.MatchString(name) {
			err = errAt(deref(n).Content[i], indexPath(path, i), "must be an environment variable's name")
		}
	}
	return names, err
}
