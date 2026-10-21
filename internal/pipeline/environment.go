package pipeline

import (
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
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

// Environment is the environment file (spec section 14): one section struct per top-level key. Each leaf's tags
// are its file key (yaml), its variable's name segment (env: MILLSTREAM_<SECTION>_<KEY>), its default in the
// file's syntax (default; none for a key unset by default or computed from another) and its one-line doc.
type Environment struct {
	Mode       string                             `yaml:"environment" env:"ENVIRONMENT" doc:"production or development (spec section 11.2); empty: serve defaults to production, run to development"`
	Store      StoreConfig                        `yaml:"store" env:"STORE"`
	Artifacts  ArtifactsConfig                    `yaml:"artifacts" env:"ARTIFACTS"`
	Listen     string                             `yaml:"listen" env:"LISTEN" doc:"host:port serve serves the API and the UI on (run: after ui.listen); empty: 127.0.0.1:7070"`
	Auth       AuthConfig                         `yaml:"auth" env:"AUTH"`
	TLS        TLSConfig                          `yaml:"tls" env:"TLS"`
	Metrics    MetricsConfig                      `yaml:"metrics" env:"METRICS"`
	Node       NodeConfig                         `yaml:"node" env:"NODE"`
	Registries map[string]string                  `yaml:"registries" env:"REGISTRIES" doc:"registry namespace to OCI repository prefix, such as { lab: ghcr.io/monolithiclab/millstream-actions }"`
	Secrets    map[string]map[string]SecretSource `yaml:"secrets" env:"SECRETS" doc:"per pipeline, each secret's source ({ env } or { file }); unset: secrets read MILLSTREAM_SECRET_<NAME>"`
	Retention  Retention                          `yaml:"retention" env:"RETENTION"`
	Lessons    LessonsConfig                      `yaml:"lessons" env:"LESSONS"`
	UI         UIConfig                           `yaml:"ui" env:"UI"`
	Improve    Improve                            `yaml:"improve" env:"IMPROVE"`
	Prices     map[string]Price                   `yaml:"prices" env:"PRICES" doc:"token prices of agent models, by model: { <model>: { inPerMtok, outPerMtok } } in USD per million tokens"`
	Runtime    RuntimeConfig                      `yaml:"runtime" env:"RUNTIME"`
	Agents     AgentsConfig                       `yaml:"agents" env:"AGENTS"`
	Engine     EngineConfig                       `yaml:"engine" env:"ENGINE"`
}

// StoreConfig is the store key: the log store, named by its DSN's scheme.
type StoreConfig struct {
	DSN string `yaml:"dsn" env:"DSN" default:"sqlite://.millstream/millstream.db" doc:"sqlite://<path>, or postgres://[host][:port]/[database][?options] with libpq's PG* variables (PGPASSWORD) for the rest"`
}

// SQLitePath is the SQLite database's path, false on Postgres.
func (c StoreConfig) SQLitePath() (string, bool) { return strings.CutPrefix(c.DSN, sqliteScheme) }

// ArtifactsConfig is the artifacts key: where artifacts are kept (spec section 9.2), named by its URL's scheme.
type ArtifactsConfig struct {
	URL string `yaml:"url" env:"URL" doc:"file://<dir>, or s3://<bucket>[/<prefix>] with the AWS_* variables for credentials, region and endpoint; empty: artifacts beside the SQLite database (else beside .millstream/)"`
}

// Local is the local artifact directory, false unless the URL is file://.
func (c ArtifactsConfig) Local() (string, bool) { return strings.CutPrefix(c.URL, fileScheme) }

// S3 is the bucket and key prefix, false unless the URL is s3://.
func (c ArtifactsConfig) S3() (bucket, prefix string, ok bool) {
	rest, ok := strings.CutPrefix(c.URL, s3Scheme)
	bucket, prefix, _ = strings.Cut(rest, "/")
	return bucket, prefix, ok
}

const (
	sqliteScheme = "sqlite://"
	fileScheme   = "file://"
	s3Scheme     = "s3://"
)

// AuthConfig is the auth key.
type AuthConfig struct {
	Token *SecretSource `yaml:"token" env:"TOKEN" doc:"source of the API's bearer token (at least 32 characters); unset: the API serves loopback clients only"`
	User  string        `yaml:"user" env:"USER" default:"operator" doc:"who decides the reviews of a token session"`
}

// DefaultAuthUser is auth.user's default.
const DefaultAuthUser = "operator"

// TLSConfig is the tls key: a PEM certificate chain and its private key, both or neither.
type TLSConfig struct {
	Cert string `yaml:"cert" env:"CERT" doc:"PEM certificate chain serve and run serve HTTPS with; unset: plain HTTP behind a TLS proxy"`
	Key  string `yaml:"key" env:"KEY" doc:"PEM private key of tls.cert"`
}

// MetricsConfig is the metrics key.
type MetricsConfig struct {
	Public bool `yaml:"public" env:"PUBLIC" default:"false" doc:"/metrics answers without the token"`
}

// NodeConfig is the node key.
type NodeConfig struct {
	Embedded bool          `yaml:"embedded" env:"EMBEDDED" default:"true" doc:"serve runs a node inside the engine"`
	Token    *SecretSource `yaml:"token" env:"TOKEN" doc:"source of the token remote nodes join with (at least 32 characters); unset: no remote nodes"`
}

// Retention is the retention key (spec section 9.3).
type Retention struct {
	Events      Duration `yaml:"events" env:"EVENTS" doc:"how long events are kept, such as 90d; unset: for ever"`
	Transcripts Duration `yaml:"transcripts" env:"TRANSCRIPTS" doc:"how long agent transcripts are kept, such as 30d; unset: for ever"`
	Rollups     Duration `yaml:"rollups" env:"ROLLUPS" doc:"how long metric rollups are kept, such as 400d; unset: for ever"`
	Every       Duration `yaml:"every" env:"EVERY" default:"1h" doc:"how often the engine's retention job runs"`
}

// LessonsConfig is the lessons key (spec section 11.4).
type LessonsConfig struct {
	MaxTokens int `yaml:"maxTokens" env:"MAX_TOKENS" default:"2000" doc:"size limit of a lessons file, in approximate tokens"`
}

// UIConfig is the ui key.
type UIConfig struct {
	BaseURL string `yaml:"baseUrl" env:"BASE_URL" default:"http://127.0.0.1:7070" doc:"where the web UI is reached, for inbox links and the origins a token server accepts"`
	Listen  string `yaml:"listen" env:"LISTEN" doc:"loopback host:port run serves the API and the UI on; unset: listen, else 127.0.0.1:7070"`
}

// Improve is the improve key (spec sections 11.2 to 11.7).
type Improve struct {
	Enabled     bool             `yaml:"enabled" env:"ENABLED" default:"false" doc:"run the improve pipeline (development only)"`
	Harness     string           `yaml:"harness" env:"HARNESS" doc:"agent harness drafting proposals: claude-code or api"`
	MinSeverity string           `yaml:"minSeverity" env:"MIN_SEVERITY" default:"warning" doc:"the least severity of the insights considered: info, warning or error"`
	Publish     string           `yaml:"publish" env:"PUBLISH" doc:"publisher of accepted proposals: apply, patch-file, git-pr-gh or git-pr-glab"`
	PatchFile   PatchFileConfig  `yaml:"patchFile" env:"PATCH_FILE"`
	Git         GitConfig        `yaml:"git" env:"GIT"`
	Budget      BudgetConfig     `yaml:"budget" env:"BUDGET"`
	Evidence    EvidenceConfig   `yaml:"evidence" env:"EVIDENCE"`
	Backtest    BacktestConfig   `yaml:"backtest" env:"BACKTEST"`
	Validation  ValidationConfig `yaml:"validation" env:"VALIDATION"`
	Distill     DistillConfig    `yaml:"distill" env:"DISTILL"`
	Filter      FilterConfig     `yaml:"filter" env:"FILTER"`
}

// PatchFileConfig is improve.patchFile.
type PatchFileConfig struct {
	Dir string `yaml:"dir" env:"DIR" doc:"where the patch-file publisher writes each accepted proposal's patch"`
}

// GitConfig is improve.git: the repository the git-pr-gh and git-pr-glab actions open pull requests on. Millstream
// holds no forge credential (D32): Env names the variables passed to the action's container.
type GitConfig struct {
	Repo   string    `yaml:"repo" env:"REPO" doc:"clone URL of the repository pull requests are opened on"`
	Base   string    `yaml:"base" env:"BASE" default:"main" doc:"branch pull requests target"`
	Path   string    `yaml:"path" env:"PATH" doc:"the package's directory in the repository, {pipeline} for its name; unset: the root"`
	Image  string    `yaml:"image" env:"IMAGE" doc:"the action's image, with git and the forge's CLI, pinned by digest"`
	Env    []string  `yaml:"env" env:"ENV" doc:"variables of the engine passed to the action's container, such as [GH_TOKEN]"`
	Author GitAuthor `yaml:"author" env:"AUTHOR"`
}

// GitAuthor is improve.git.author: who signs the commits.
type GitAuthor struct {
	Name  string `yaml:"name" env:"NAME" default:"Millstream" doc:"commit author's name"`
	Email string `yaml:"email" env:"EMAIL" default:"millstream@localhost" doc:"commit author's email"`
}

// DefaultGitBase is improve.git.base's default.
const DefaultGitBase = "main"

// BudgetConfig is improve.budget.
type BudgetConfig struct {
	DayUSD float64 `yaml:"dayUsd" env:"DAY_USD" doc:"what backtests may spend a day, in USD; unset: no limit"`
}

// EvidenceConfig is improve.evidence (spec section 11.2).
type EvidenceConfig struct {
	Sample int      `yaml:"sample" env:"SAMPLE" default:"500" doc:"items an evidence bundle samples"`
	TTL    Duration `yaml:"ttl" env:"TTL" default:"30d" doc:"how long an evidence bundle lasts"`
}

// BacktestConfig is improve.backtest.
type BacktestConfig struct {
	Sample int `yaml:"sample" env:"SAMPLE" default:"200" doc:"past items a backtest replays"`
}

// ValidationConfig is improve.validation.
type ValidationConfig struct {
	Timeout Duration `yaml:"timeout" env:"TIMEOUT" default:"30m" doc:"how long a proposal's validation may run"`
}

// DistillConfig is improve.distill (spec section 11.5).
type DistillConfig struct {
	MinDeliveries int     `yaml:"minDeliveries" env:"MIN_DELIVERIES" default:"500" doc:"deliveries of an agent step before a distill is proposed"`
	Language      string  `yaml:"language" env:"LANGUAGE" default:"go" doc:"language of distilled actions: go, the only one for now"`
	MinAgreement  float64 `yaml:"minAgreement" env:"MIN_AGREEMENT" default:"0.98" doc:"agreement with the agent a distilled action needs, from 0 to 1"`
	MinCoverage   float64 `yaml:"minCoverage" env:"MIN_COVERAGE" default:"0.6" doc:"share of the holdout a distilled action must answer, from 0 to 1"`
}

// FilterConfig is improve.filter.
type FilterConfig struct {
	MaxFalseDrop float64 `yaml:"maxFalseDrop" env:"MAX_FALSE_DROP" default:"0.01" doc:"share of past approvals a proposed filter may drop, from 0 to 1"`
}

// RuntimeConfig is the runtime key (spec section 6.4).
type RuntimeConfig struct {
	Container string `yaml:"container" env:"CONTAINER" default:"docker" doc:"container runtime: docker or podman"`
}

// AgentsConfig is the agents key: the agent sandbox (spec section 7.3).
type AgentsConfig struct {
	AllowUnsandboxed bool   `yaml:"allowUnsandboxed" env:"ALLOW_UNSANDBOXED" default:"false" doc:"agents may run without a container sandbox"`
	Image            string `yaml:"image" env:"IMAGE" doc:"image the agent sandbox runs (CA certificates, the claude CLI for claude-code); unset: no sandbox"`
	LinuxBinary      string `yaml:"linuxBinary" env:"LINUX_BINARY" doc:"Linux build of millstream mounted into the sandbox as the adapter; unset: the running binary, on Linux"`
}

// EngineConfig is the engine key: how an engine on Postgres watches its write connection (spec section 5.9).
type EngineConfig struct {
	LockCheck Duration `yaml:"lockCheck" env:"LOCK_CHECK" default:"5s" doc:"how often an idle engine checks its write connection, and the lock with it"`
	Keepalive Duration `yaml:"keepalive" env:"KEEPALIVE" default:"5s" doc:"period of the write connection's TCP keepalives"`
}

// Price is a model's price per million tokens, in US dollars.
type Price struct {
	InPerMtok  float64 `yaml:"inPerMtok"`
	OutPerMtok float64 `yaml:"outPerMtok"`
}

// LoadEnvironment loads the environment in three layers over Defaults: the environment file at path (a missing
// one is no layer when required is false, an error otherwise), then the variables lookup holds (applyVariables;
// names are the process's, for its strictness). Flags are the caller's, applied after. Relative paths the file
// sets resolve against its directory; those of a default or a variable against the working directory.
func LoadEnvironment(path string, required bool, lookup func(string) (string, bool), names []string) (
	*Environment, error,
) {
	e := Defaults()
	data, err := os.ReadFile(path) // #nosec G304 -- the operator names the environment file
	switch {
	case errors.Is(err, fs.ErrNotExist) && !required:
	case err != nil:
		return nil, fmt.Errorf("environment file: %w", err)
	default:
		if err := e.decodeFile(data, filepath.Dir(path)); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
	}
	if err := applyVariables(e, lookup, names); err != nil {
		return nil, err
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return e, nil
}

// ParseEnvironment is an environment file's text over Defaults, validated: what tests and the improve package
// build an environment from.
func ParseEnvironment(data []byte) (*Environment, error) {
	e := Defaults()
	if err := e.decodeFile(data, ""); err != nil {
		return nil, err
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return e, nil
}

// decodeFile decodes an environment file into e strictly, then resolves the relative paths it set against dir
// (none when dir is empty).
func (e *Environment) decodeFile(data []byte, dir string) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse YAML: %w", err)
	}
	if len(doc.Content) == 0 {
		return nil
	}
	dsn := e.Store.DSN
	e.Store.DSN = "" // set again only by the file, whose path then resolves against its directory
	if err := decode(doc.Content[0], reflect.ValueOf(e).Elem(), ""); err != nil {
		return err
	}
	abs := func(p *string, scheme string) {
		if rest, ok := strings.CutPrefix(*p, scheme); ok && dir != "" && rest != "" && !filepath.IsAbs(rest) {
			*p = scheme + filepath.Join(dir, rest)
		}
	}
	abs(&e.Store.DSN, sqliteScheme)
	e.Store.DSN = cmp.Or(e.Store.DSN, dsn)
	abs(&e.Artifacts.URL, fileScheme)
	for _, p := range []*string{&e.Agents.LinuxBinary, &e.Improve.PatchFile.Dir, &e.TLS.Cert, &e.TLS.Key} {
		abs(p, "")
	}
	for _, src := range []*SecretSource{e.Auth.Token, e.Node.Token} {
		if src != nil {
			abs(&src.File, "")
		}
	}
	for _, sources := range e.Secrets {
		for name, src := range sources {
			abs(&src.File, "")
			sources[name] = src
		}
	}
	return nil
}

// Validate checks what decoding cannot: the values of e's keys, each error naming its key path.
func (e *Environment) Validate() error {
	var errs []error
	check := func(ok bool, key, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
		}
	}
	oneOf := func(key, v string, allowed ...string) {
		check(slices.Contains(allowed, v), key, "must be one of %s", strings.Join(allowed[1:], ", "))
	}
	hostPort := func(key, v string) {
		_, _, err := net.SplitHostPort(v)
		check(v == "" || err == nil, key, "want host:port, such as 127.0.0.1:7070")
	}
	source := func(key string, s *SecretSource) {
		if s != nil {
			check((s.Env == "") != (s.File == ""), key, "want exactly one of env and file")
			check(s.Env == "" || envName.MatchString(s.Env), key+".env", "want an environment variable name such as "+
				"MILLSTREAM_TOKEN")
		}
	}
	fraction := func(key string, v float64) { check(v >= 0 && v <= 1, key, "must be a number from 0 to 1") }
	positive := func(key string, v int) { check(v > 0, key, "must be a positive integer") }

	oneOf("environment", e.Mode, "", ModeProduction, ModeDevelopment)
	e.Store.validate(check)
	bucket, _, isS3 := e.Artifacts.S3()
	local, isLocal := e.Artifacts.Local()
	check(e.Artifacts.URL == "" || isLocal && local != "" || isS3 && bucket != "", "artifacts.url",
		"want file://<dir> or s3://<bucket>[/<prefix>]")
	hostPort("listen", e.Listen)
	hostPort("ui.listen", e.UI.Listen)
	source("auth.token", e.Auth.Token)
	check(e.Auth.User != "", "auth.user", "must not be empty")
	check((e.TLS.Cert == "") == (e.TLS.Key == ""), "tls", "set both cert and key, or neither")
	source("node.token", e.Node.Token)
	for ns, prefix := range e.Registries {
		check(namespace.MatchString(ns), "registries."+ns, "a namespace is lowercase letters, digits and dashes")
		check(prefix != "" && !strings.Contains(prefix, "://") && !strings.ContainsAny(prefix, "@ \t"),
			"registries."+ns, "want a repository prefix such as ghcr.io/org/actions, without a scheme or digest")
	}
	for p, sources := range e.Secrets {
		for name, s := range sources {
			source("secrets."+p+"."+name, &s)
		}
	}
	positive("lessons.maxTokens", e.Lessons.MaxTokens)
	check(httpURL(e.UI.BaseURL), "ui.baseUrl", "want an http or https URL such as https://millstream.example")
	im := &e.Improve
	oneOf("improve.harness", im.Harness, "", HarnessClaudeCode, HarnessAPI)
	oneOf("improve.minSeverity", im.MinSeverity, append([]string{""}, Severities...)...)
	oneOf("improve.publish", im.Publish, append([]string{""}, PublishActions...)...)
	check(im.Publish != PublishPatchFile || im.PatchFile.Dir != "", "improve.publish", "patch-file needs "+
		"improve.patchFile.dir")
	check(im.Publish != PublishGitPRGH && im.Publish != PublishGitPRGLab || im.Git.Repo != "" && im.Git.Image != "",
		"improve.publish", "%s needs improve.git.repo and improve.git.image", im.Publish)
	check(im.Git.Image == "" || pinnedRef.MatchString(im.Git.Image), "improve.git.image", "must be pinned by digest: "+
		"<repository>@sha256:<64 hex characters>")
	p := im.Git.Path
	check(p == "" || !filepath.IsAbs(p) && !strings.Contains(p, "\\") && pathpkg.Clean(p) == p && p != ".." &&
		!strings.HasPrefix(p, "../"), "improve.git.path", "must be a clean relative path inside the repository")
	for i, name := range im.Git.Env {
		check(envName.MatchString(name), fmt.Sprintf("improve.git.env[%d]", i), "must be an environment variable's name")
	}
	check(im.Budget.DayUSD >= 0, "improve.budget.dayUsd", "must be a non-negative number")
	positive("improve.evidence.sample", im.Evidence.Sample)
	positive("improve.backtest.sample", im.Backtest.Sample)
	positive("improve.distill.minDeliveries", im.Distill.MinDeliveries)
	check(im.Distill.Language == "go", "improve.distill.language", "%q is not supported yet: distilled actions are "+
		"written in go, the only language the candidate-code sandbox builds and runs", im.Distill.Language)
	fraction("improve.distill.minAgreement", im.Distill.MinAgreement)
	fraction("improve.distill.minCoverage", im.Distill.MinCoverage)
	fraction("improve.filter.maxFalseDrop", im.Filter.MaxFalseDrop)
	for model, price := range e.Prices {
		check(price.InPerMtok >= 0 && price.OutPerMtok >= 0, "prices."+model, "must be non-negative numbers")
	}
	oneOf("runtime.container", e.Runtime.Container, "", RuntimeDocker, RuntimePodman)
	if e.Mode != "" {
		errs = append(errs, e.CheckMode(""))
	}
	return errors.Join(errs...)
}

// validate checks store.dsn: sqlite://<path>, or a postgres:// URI without a password, which libpq's PGPASSWORD
// (or ~/.pgpass) supplies, so that the DSN is no secret. The DSN is never part of the error.
func (c StoreConfig) validate(check func(ok bool, key, format string, args ...any)) {
	if path, ok := c.SQLitePath(); ok {
		check(path != "", "store.dsn", "want sqlite://<path>")
		return
	}
	u, err := url.Parse(c.DSN)
	if err != nil || u.Scheme != "postgres" && u.Scheme != "postgresql" {
		check(false, "store.dsn", "want sqlite://<path> or postgres://[host][:port]/[database][?option=value]")
		return
	}
	_, hasPassword := u.User.Password()
	check(!hasPassword, "store.dsn", "a password in the DSN is refused: set PGPASSWORD (or use ~/.pgpass) instead")
}

// envName is an environment variable's name.
var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var pinnedRef = regexp.MustCompile(`^[^\s@]+@sha256:[0-9a-f]{64}$`)

// Publishers of accepted proposals (spec section 11.3).
const (
	PublishApply     = "apply"
	PublishPatchFile = "patch-file"
	PublishGitPRGH   = "git-pr-gh"
	PublishGitPRGLab = "git-pr-glab"
)

// PublishActions are the publish actions improve.publish may name.
var PublishActions = []string{PublishApply, PublishPatchFile, PublishGitPRGH, PublishGitPRGLab}

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

// ErrSecretUnset marks a secret lookup that failed for want of a value only: the name is mapped for the pipeline
// (without an environment file, every name is, to MILLSTREAM_SECRET_<NAME>) but its source holds nothing. check,
// apply and firings accept it: only a dispatch needs the value (spec section 4.5).
var ErrSecretUnset = errors.New("secret unset")

// unsetError is a lookup error that is ErrSecretUnset.
type unsetError struct{ err error }

func (u unsetError) Error() string      { return u.err.Error() }
func (u unsetError) Unwrap() error      { return u.err }
func (unsetError) Is(target error) bool { return target == ErrSecretUnset }

// SecretLookup returns how a pipeline's secrets resolve (spec section 4.5): through the secrets key, which is also
// the grant, when the file has one, an unmapped name resolving to nothing; else from MILLSTREAM_SECRET_<NAME>
// through env. The error of a mapped source that cannot be read names the source, never a value; it is
// ErrSecretUnset when the source holds nothing (an unset variable, a missing or empty file).
func (e *Environment) SecretLookup(pipelineName string, env func(string) (string, bool)) SecretLookup {
	if e == nil || e.Secrets == nil {
		return func(name string) (string, error) {
			if v, ok := env(SecretEnv(name)); ok {
				return v, nil
			}
			return "", unsetError{fmt.Errorf("secret %s is not set: set %s", name, SecretEnv(name))}
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

// SecretSource is where a secret's value is read from: an environment variable ({ env: NAME }) or a file
// ({ file: path }, relative to the environment file), such as a container secret's mount.
type SecretSource struct {
	Env  string `yaml:"env"`
	File string `yaml:"file"`
}

// Value reads the secret: the variable, which must be set and not empty, through lookup, or the file, without its
// trailing newline. Errors name the source, never the value.
func (s SecretSource) Value(lookup func(string) (string, bool)) (string, error) {
	if s.Env != "" {
		v, ok := lookup(s.Env)
		if !ok || v == "" {
			return "", unsetError{fmt.Errorf("environment variable %s is not set", s.Env)}
		}
		return v, nil
	}
	b, err := os.ReadFile(s.File) // #nosec G304 -- the operator names the secret's file
	if errors.Is(err, fs.ErrNotExist) {
		return "", unsetError{fmt.Errorf("read secret file: %w", err)}
	}
	if err != nil { // unreadable: a misconfiguration, not a value still to come
		return "", fmt.Errorf("read secret file: %w", err)
	}
	v := strings.TrimRight(string(b), "\r\n")
	if v == "" {
		return "", unsetError{fmt.Errorf("secret file %s is empty", s.File)}
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
