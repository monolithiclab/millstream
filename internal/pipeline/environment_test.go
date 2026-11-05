package pipeline

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/testdir"
)

// vars is a variable lookup and its names, from a map.
func vars(m map[string]string) (func(string) (string, bool), []string) {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }, names
}

// load loads the environment file src (none when empty) from a new directory, with the variables m.
func load(t *testing.T, src string, m map[string]string) (*Environment, string, error) {
	t.Helper()
	dir := testdir.New(t)
	path := filepath.Join(dir, EnvironmentFile)
	if src != "" {
		if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	lookup, names := vars(m)
	env, err := LoadEnvironment(path, false, lookup, names, "")
	return env, dir, err
}

// TestParseEnvironment reads every key, each to its field.
func TestParseEnvironment(t *testing.T) {
	t.Parallel()
	env, err := ParseEnvironment([]byte(`
environment: development
store: { dsn: "postgres://db:5432/ms?sslmode=disable" }
artifacts: { url: s3://arts/m }
listen: 0.0.0.0:7070
auth: { token: { env: MILLSTREAM_TOKEN }, user: ana }
tls: { cert: tls/cert.pem, key: tls/key.pem }
metrics: { public: true }
node: { embedded: false, token: { file: node.token } }
registries: { lab: ghcr.io/monolithiclab/millstream-actions/, local: "localhost:5000/actions" }
secrets:
  property-sourcing: { listingsToken: { env: LISTINGS_TOKEN }, apiKey: { file: key } }
  other: {}
retention: { events: 90d, transcripts: 30d, rollups: 400d, every: 30m }
lessons: { maxTokens: 500 }
ui: { baseUrl: "https://millstream.example", listen: "127.0.0.1:8080" }
improve:
  enabled: true
  harness: api
  minSeverity: error
  publish: apply
  patchFile: { dir: proposals }
  git: { repo: "https://git.example/p.git", base: trunk, path: "pipelines/{pipeline}", env: [GH_TOKEN],
    image: "ghcr.io/x/publish@sha256:0000000000000000000000000000000000000000000000000000000000000000",
    author: { name: Bot, email: bot@example.invalid } }
  budget: { dayUsd: 5 }
  evidence: { sample: 50, ttl: 3d }
  backtest: { sample: 20 }
  validation: { timeout: 45m }
  distill: { minDeliveries: 50, language: go, minAgreement: 0.9, minCoverage: 0.5, auditRate: 0.1 }
  filter: { maxFalseDrop: 0.02 }
prices: { claude-sonnet-5-5: { inPerMtok: 3, outPerMtok: 15.5 } }
runtime: { container: podman }
agents: { allowUnsandboxed: true, image: "agents:1", linuxBinary: /opt/millstream-linux }
engine: { lockCheck: 2s, keepalive: 3s }
`))
	if err != nil {
		t.Fatal(err)
	}
	day := Duration(24 * time.Hour)
	want := &Environment{
		Mode: ModeDevelopment, Store: StoreConfig{DSN: "postgres://db:5432/ms?sslmode=disable"},
		Artifacts: ArtifactsConfig{URL: "s3://arts/m"}, Listen: "0.0.0.0:7070",
		Auth: AuthConfig{Token: &SecretSource{Env: "MILLSTREAM_TOKEN"}, User: "ana"},
		TLS:  TLSConfig{Cert: "tls/cert.pem", Key: "tls/key.pem"}, Metrics: MetricsConfig{Public: true},
		Node: NodeConfig{Token: &SecretSource{File: "node.token"}},
		Registries: map[string]string{"lab": "ghcr.io/monolithiclab/millstream-actions/",
			"local": "localhost:5000/actions"},
		Secrets: map[string]map[string]SecretSource{
			"property-sourcing": {"listingsToken": {Env: "LISTINGS_TOKEN"}, "apiKey": {File: "key"}}, "other": {},
		},
		Retention: Retention{Events: 90 * day, Transcripts: 30 * day, Rollups: 400 * day,
			Every: Duration(30 * time.Minute)},
		Lessons: LessonsConfig{MaxTokens: 500},
		UI:      UIConfig{BaseURL: "https://millstream.example", Listen: "127.0.0.1:8080"},
		Improve: Improve{Enabled: true, Harness: "api", MinSeverity: "error", Publish: PublishApply,
			PatchFile: PatchFileConfig{Dir: "proposals"}, Git: GitConfig{Repo: "https://git.example/p.git",
				Base: "trunk", Path: "pipelines/{pipeline}", Env: []string{"GH_TOKEN"},
				Author: GitAuthor{Name: "Bot", Email: "bot@example.invalid"},
				Image:  "ghcr.io/x/publish@sha256:0000000000000000000000000000000000000000000000000000000000000000"},
			Budget: BudgetConfig{DayUSD: 5}, Evidence: EvidenceConfig{Sample: 50, TTL: 3 * day},
			Backtest: BacktestConfig{Sample: 20}, Validation: ValidationConfig{Timeout: Duration(45 * time.Minute)},
			Distill: DistillConfig{MinDeliveries: 50, Language: "go", MinAgreement: 0.9, MinCoverage: 0.5, AuditRate: 0.1},
			Filter:  FilterConfig{MaxFalseDrop: 0.02}},
		Prices:  map[string]Price{"claude-sonnet-5-5": {InPerMtok: 3, OutPerMtok: 15.5}},
		Runtime: RuntimeConfig{Container: RuntimePodman},
		Agents:  AgentsConfig{AllowUnsandboxed: true, Image: "agents:1", LinuxBinary: "/opt/millstream-linux"},
		Engine:  EngineConfig{LockCheck: Duration(2 * time.Second), Keepalive: Duration(3 * time.Second)},
	}
	if !reflect.DeepEqual(env, want) {
		t.Fatalf("environment\n%+v\nwant\n%+v", env, want)
	}
	for _, src := range []string{"", "# nothing\n", "ui: ~\n"} {
		if empty, err := ParseEnvironment([]byte(src)); err != nil || !reflect.DeepEqual(empty, Defaults()) {
			t.Fatalf("%q: %+v, %v", src, empty, err)
		}
	}
}

func TestCheckMode(t *testing.T) {
	t.Parallel()
	env := &Environment{Mode: ModeDevelopment, Improve: Improve{Enabled: true}}
	if err := env.CheckMode(ModeProduction); err != nil {
		t.Fatalf("development, explicit: %v", err)
	}
	env.Mode = ""
	if err := env.CheckMode(ModeProduction); !errors.Is(err, ErrProduction) {
		t.Fatalf("improve.enabled under a production default: %v", err)
	}
	if err := env.CheckMode(ModeDevelopment); err != nil {
		t.Fatalf("improve.enabled under a development default: %v", err)
	}
	env.Improve.Enabled, env.Improve.Publish = false, PublishApply
	if err := env.CheckMode(ModeProduction); !errors.Is(err, ErrProduction) ||
		!strings.Contains(err.Error(), "improve.publish: apply") {
		t.Fatalf("publish apply in production: %v", err)
	}
	if err := env.RequireDevelopment(ModeProduction, "millstream improve"); !errors.Is(err, ErrProduction) {
		t.Fatalf("improve in production: %v", err)
	}
	if err := env.RequireDevelopment(ModeDevelopment, "millstream improve"); err != nil {
		t.Fatalf("improve in development: %v", err)
	}
}

func TestParseEnvironmentErrors(t *testing.T) {
	t.Parallel()
	const pinned = "x@sha256:0000000000000000000000000000000000000000000000000000000000000000"
	tests := map[string]struct{ src, want string }{
		"unknown key":       {"registry: {}\n", "line 1: registry: unknown key"},
		"duplicate key":     {"listen: a:1\nlisten: b:2\n", "line 2: listen: duplicate key"},
		"environment":       {"environment: staging\n", "environment: must be one of production, development"},
		"listen":            {"listen: 7070\n", "listen: want host:port"},
		"ui listen":         {"ui: { listen: here }\n", "ui.listen: want host:port"},
		"artifacts key":     {"artifacts: { local: { path: a } }\n", "artifacts.local: unknown key"},
		"artifacts scheme":  {"artifacts: { url: \"ftp://x\" }\n", "artifacts.url: want file://<dir> or s3://"},
		"artifacts bucket":  {"artifacts: { url: \"s3://\" }\n", "artifacts.url: want"},
		"auth inline":       {"auth: { token: s3cret }\n", "auth.token: want { env: NAME }"},
		"auth key":          {"auth: { oidc: {} }\n", "auth.oidc: unknown key"},
		"auth user":         {"auth: { token: { env: T }, user: \"\" }\n", "auth.user: must not be empty"},
		"tls no key":        {"tls: { cert: c.pem }\n", "tls: set both cert and key, or neither"},
		"tls key":           {"tls: { cert: c, key: k, ca: x }\n", "tls.ca: unknown key"},
		"metrics public":    {"metrics: { public: maybe }\n", "metrics.public: must be true or false"},
		"metrics key":       {"metrics: { path: /m }\n", "metrics.path: unknown key"},
		"node embedded":     {"node: { embedded: maybe }\n", "node.embedded: must be true or false"},
		"node token":        {"node: { token: { env: \"a b\" } }\n", "node.token.env: want an environment"},
		"node key":          {"node: { tls: {} }\n", "node.tls: unknown key"},
		"secrets scalar":    {"secrets: x\n", "secrets: must be a mapping"},
		"secrets pipeline":  {"secrets: { p: [a] }\n", "secrets.p: must be a mapping"},
		"secret inline":     {"secrets: { p: { token: abc } }\n", "secrets.p.token: want { env: NAME }"},
		"secret source":     {"secrets: { p: { token: { vault: x } } }\n", "secrets.p.token.vault: unknown key"},
		"secret both":       {"secrets: { p: { token: { env: A, file: f } } }\n", "secrets.p.token: want exactly one"},
		"secret none":       {"secrets: { p: { token: {} } }\n", "secrets.p.token: want exactly one"},
		"retention":         {"retention: { events: forever }\n", "retention.events: invalid duration"},
		"retention key":     {"retention: { items: 3d }\n", "retention.items: unknown key"},
		"engine key":        {"engine: { id: a }\n", "engine.id: unknown key"},
		"engine zero":       {"engine: { lockCheck: 0s }\n", "engine.lockCheck: duration \"0s\" must be positive"},
		"engine keepalive":  {"engine: { keepalive: [often] }\n", "engine.keepalive: must be a duration"},
		"improve key":       {"improve: { autoApply: true }\n", "improve.autoApply: unknown key"},
		"improve harness":   {"improve: { harness: gpt }\n", "improve.harness: must be one of claude-code, api"},
		"improve publish":   {"improve: { publish: email }\n", "improve.publish: must be one of apply"},
		"improve severity":  {"improve: { minSeverity: high }\n", "improve.minSeverity: must be one of"},
		"improve sample":    {"improve: { evidence: { sample: 0 } }\n", "improve.evidence.sample: must be a positive"},
		"improve ttl":       {"improve: { evidence: { ttl: soon } }\n", "improve.evidence.ttl:"},
		"improve evidence":  {"improve: { evidence: { size: 3 } }\n", "improve.evidence.size: unknown key"},
		"improve budget":    {"improve: { budget: { dayUsd: -1 } }\n", "improve.budget.dayUsd: must be a non-negative"},
		"improve backtest":  {"improve: { backtest: { sample: -1 } }\n", "improve.backtest.sample: must be a positive"},
		"improve agreement": {"improve: { distill: { minAgreement: 2 } }\n", "improve.distill.minAgreement: must be a number from 0 to 1"},
		"improve language":  {"improve: { distill: { language: python } }\n", `improve.distill.language: "python" is not supported yet: distilled actions are written in go`},
		"improve filter":    {"improve: { filter: 3 }\n", "improve.filter: must be a mapping"},
		"improve prod":      {"environment: production\nimprove: { enabled: true }\n", "improve.enabled: the improve pipeline runs in development"},
		"patch-file dir":    {"improve: { publish: patch-file }\n", "improve.publish: patch-file needs improve.patchFile.dir"},
		"git repo":          {"improve: { publish: git-pr-gh }\n", "improve.publish: git-pr-gh needs improve.git.repo"},
		"git image":         {"improve: { git: { repo: r, image: alpine/git } }\n", "improve.git.image: must be pinned by digest"},
		"git path":          {"improve: { git: { repo: r, path: ../x, image: " + pinned + " } }\n", "improve.git.path: must be a clean"},
		"git env":           {"improve: { git: { repo: r, env: [GH-TOKEN], image: " + pinned + " } }\n", "improve.git.env[0]: must be an environment"},
		"git env list":      {"improve: { git: { env: GH_TOKEN } }\n", "improve.git.env: must be a list"},
		"improve prod pub":  {"environment: production\nimprove: { publish: apply }\n", "improve.publish: apply is refused in production"},
		"store scalar":      {"store: sqlite\n", "store: must be a mapping"},
		"store key":         {"store: { sqlite: { path: a } }\n", "store.sqlite: unknown key"},
		"not a mapping":     {"- a\n", "must be a mapping"},
		"bad namespace":     {"registries: { Lab: ghcr.io/x }\n", "registries.Lab: a namespace is lowercase"},
		"scheme":            {"registries: { lab: https://ghcr.io/x }\n", "registries.lab: want a repository prefix"},
		"digest":            {"registries: { lab: ghcr.io/x@sha256:1 }\n", "registries.lab: want a repository prefix"},
		"empty prefix":      {"registries: { lab: \"\" }\n", "registries.lab: want a repository prefix"},
		"registries scalar": {"registries: x\n", "registries: must be a mapping"},
		"bad runtime":       {"runtime: { container: lxc }\n", "runtime.container: must be one of docker, podman"},
		"runtime key":       {"runtime: { exec: true }\n", "runtime.exec: unknown key"},
		"runtime scalar":    {"runtime: docker\n", "runtime: must be a mapping"},
		"price negative":    {"prices: { m: { inPerMtok: -1, outPerMtok: 1 } }\n", "prices.m: must be non-negative"},
		"price number":      {"prices: { m: { inPerMtok: cheap } }\n", "prices.m.inPerMtok: must be a number"},
		"price key":         {"prices: { m: { in: 1 } }\n", "prices.m.in: unknown key"},
		"price missing":     {"prices: { m: { inPerMtok: 1 } }\n", "prices.m.outPerMtok: is required"},
		"prices scalar":     {"prices: 3\n", "prices: must be a mapping"},
		"price scalar":      {"prices: { m: 3 }\n", "prices.m: must be a mapping"},
		"agents not bool":   {"agents: { allowUnsandboxed: yes please }\n", "agents.allowUnsandboxed: must be true or false"},
		"agents key":        {"agents: { sandbox: none }\n", "agents.sandbox: unknown key"},
		"agents scalar":     {"agents: true\n", "agents: must be a mapping"},
		"agents binary":     {"agents: { linuxBinary: [x] }\n", "agents.linuxBinary: must be a string"},
		"lessons zero":      {"lessons: { maxTokens: 0 }\n", "lessons.maxTokens: must be a positive integer"},
		"lessons int":       {"lessons: { maxTokens: 1.5 }\n", "lessons.maxTokens: must be an integer"},
		"lessons key":       {"lessons: { max: 3 }\n", "lessons.max: unknown key"},
		"lessons scalar":    {"lessons: 3\n", "lessons: must be a mapping"},
		"ui scalar":         {"ui: x\n", "ui: must be a mapping"},
		"ui key":            {"ui: { url: x }\n", "ui.url: unknown key"},
		"ui base url":       {"ui: { baseUrl: \"ftp://x\" }\n", "ui.baseUrl: want an http or https URL"},
		"yaml":              {"registries: [\n", "parse YAML"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ParseEnvironment([]byte(tt.src))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

// TestLayers: a variable overrides the file, which overrides the default tag.
func TestLayers(t *testing.T) {
	t.Parallel()
	const src = "runtime: { container: podman }\n"
	if env, _, err := load(t, src, map[string]string{"MILLSTREAM_RUNTIME_CONTAINER": "docker"}); err != nil ||
		env.Runtime.Container != RuntimeDocker {
		t.Fatalf("file and variable: %+v, %v", env, err)
	}
	if env, _, err := load(t, src, nil); err != nil || env.Runtime.Container != RuntimePodman {
		t.Fatalf("file alone: %+v, %v", env, err)
	}
	if env, _, err := load(t, "", nil); err != nil || !reflect.DeepEqual(env, Defaults()) {
		t.Fatalf("no file: %+v, %v", env, err)
	}
	lookup, names := vars(nil)
	if _, err := LoadEnvironment(filepath.Join(testdir.New(t), "absent.yaml"), true, lookup, names, ""); err == nil {
		t.Fatal("a required missing file: no error")
	}
	if _, _, err := load(t, "listen: [\n", nil); err == nil || !strings.Contains(err.Error(), EnvironmentFile) {
		t.Fatalf("a bad file: %v", err)
	}
}

func TestVariableShapes(t *testing.T) {
	t.Parallel()
	env, _, err := load(t, "listen: 127.0.0.1:9000\nauth: { token: { file: tok } }\n", map[string]string{
		"MILLSTREAM_REGISTRIES":         "{ lab: ghcr.io/x/actions }",
		"MILLSTREAM_AUTH_TOKEN":         strings.Repeat("t", MinTokenLength),
		"MILLSTREAM_RETENTION_EVENTS":   "90d",
		"MILLSTREAM_LISTEN":             "",
		"MILLSTREAM_IMPROVE_GIT_ENV":    "[GH_TOKEN, GITLAB_TOKEN]",
		"MILLSTREAM_PRICES":             "{ m: { inPerMtok: 1, outPerMtok: 2 } }",
		"MILLSTREAM_LESSONS_MAX_TOKENS": "300",
		"MILLSTREAM_UI_BASE_URL":        "https://ms.example",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(env.Registries, map[string]string{"lab": "ghcr.io/x/actions"}) ||
		*env.Auth.Token != (SecretSource{Env: "MILLSTREAM_AUTH_TOKEN"}) ||
		env.Retention.Events != Duration(90*24*time.Hour) || env.Listen != "127.0.0.1:9000" ||
		!reflect.DeepEqual(env.Improve.Git.Env, []string{"GH_TOKEN", "GITLAB_TOKEN"}) ||
		env.Prices["m"] != (Price{InPerMtok: 1, OutPerMtok: 2}) || env.Lessons.MaxTokens != 300 ||
		env.UI.BaseURL != "https://ms.example" {
		t.Fatalf("environment %+v", env)
	}
}

func TestUnknownVariable(t *testing.T) {
	t.Parallel()
	_, _, err := load(t, "", map[string]string{"MILLSTREAM_UI_BASEURL": "x"})
	if err == nil || !strings.Contains(err.Error(), "MILLSTREAM_UI_BASEURL: no such setting; did you mean "+
		"MILLSTREAM_UI_BASE_URL?") {
		t.Fatalf("a misspelt variable: %v", err)
	}
	// A section or a map named alone, or a variable under one that names no setting: a map is set whole.
	for _, name := range []string{"MILLSTREAM_IMPROVE_GIT_AUTHOR_MAIL", "MILLSTREAM_UI", "MILLSTREAM_IMPROVE_GIT",
		"MILLSTREAM_REGISTRIES_LAB", "MILLSTREAM_PRICES_M", "MILLSTREAM_SECRETS_P"} {
		if _, _, err := load(t, "", map[string]string{name: "x"}); err == nil ||
			!strings.Contains(err.Error(), name+": no such setting") {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, _, err := load(t, "", map[string]string{"MILLSTREAM_PRICES": "{ m: { inPerMtok: 1 } }"}); err == nil ||
		!strings.Contains(err.Error(), "MILLSTREAM_PRICES: prices.m.outPerMtok: is required") {
		t.Errorf("a price without its output price: %v", err)
	}
	// The CLI's own variables, the pipelines' secrets and variables, and the node's join token are no settings,
	// or (MILLSTREAM_NODE_TOKEN) one on purpose.
	for _, name := range []string{"MILLSTREAM_SERVER", "MILLSTREAM_TOKEN", "MILLSTREAM_COLOR", "MILLSTREAM_SECRET_X",
		"MILLSTREAM_LIVE", "MILLSTREAM_LIVE_MODEL", "MILLSTREAM_VAR_CITY", "MILLSTREAM_ENV_FILE", "MILLSTREAM_DSN",
		"MILLSTREAM_WORKDIR", "MILLSTREAM_NODE_TOKEN"} {
		if _, _, err := load(t, "", map[string]string{name: strings.Repeat("x", MinTokenLength)}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestVariableTypeError(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]string{
		"MILLSTREAM_NODE_EMBEDDED":    "MILLSTREAM_NODE_EMBEDDED: node.embedded: must be true or false",
		"MILLSTREAM_REGISTRIES":       "MILLSTREAM_REGISTRIES: registries: want YAML flow syntax",
		"MILLSTREAM_ENGINE_KEEPALIVE": "MILLSTREAM_ENGINE_KEEPALIVE: engine.keepalive: invalid duration",
	} {
		if _, _, err := load(t, "", map[string]string{name: "{maybe"}); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", name, err, want)
		}
	}
}

func TestStoreDSN(t *testing.T) {
	t.Parallel()
	if p, ok := Defaults().Store.SQLitePath(); !ok || p != ".millstream/millstream.db" {
		t.Fatalf("default store: %q, %v", p, ok)
	}
	env, dir, err := load(t, "store: { dsn: sqlite://state/m.db }\n", nil)
	if p, ok := env.Store.SQLitePath(); err != nil || !ok || p != filepath.Join(dir, "state", "m.db") {
		t.Fatalf("a relative path in the file: %q, %v", p, err)
	}
	env, _, err = load(t, "", map[string]string{"MILLSTREAM_STORE_DSN": "sqlite://state/m.db"})
	if p, ok := env.Store.SQLitePath(); err != nil || !ok || p != "state/m.db" {
		t.Fatalf("a relative path in a variable: %q, %v", p, err)
	}
	bad := filepath.Join(testdir.New(t), EnvironmentFile)
	if err := os.WriteFile(bad, []byte("store: { dsn: mysql://x }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	lookup, names := vars(map[string]string{"MILLSTREAM_STORE_DSN": "mysql://y"})
	env, err = LoadEnvironment(bad, true, lookup, names, "sqlite://flag.db")
	if p, ok := env.Store.SQLitePath(); err != nil || !ok || p != "flag.db" {
		t.Fatalf("the flag, the last layer: %q, %v", p, err)
	}
	if _, err := ParseEnvironment([]byte("store: { dsn: \"postgres://db/ms?sslmode=disable\" }\n")); err != nil {
		t.Fatal(err)
	}
	if _, ok := (StoreConfig{DSN: "postgres://db/ms"}).SQLitePath(); ok {
		t.Fatal("Postgres has a SQLite path")
	}
	for dsn, want := range map[string]string{
		"postgres://u:pw@db/ms":        "store.dsn: a password in the DSN is refused: set PGPASSWORD",
		"postgres://db/ms?password=pw": "store.dsn: a password in the DSN is refused: set PGPASSWORD",
		"mysql://x":                    "store.dsn: want sqlite://<path> or postgres://",
		"state/m.db":                   "store.dsn: want sqlite://<path> or postgres://",
		"sqlite://":                    "store.dsn: want sqlite://<path>",
	} {
		_, err := ParseEnvironment([]byte("store: { dsn: \"" + dsn + "\" }\n"))
		if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "pw") {
			t.Errorf("%s: %v, want %q", dsn, err, want)
		}
	}
}

func TestArtifactsURL(t *testing.T) {
	t.Parallel()
	env, dir, err := load(t, "artifacts: { url: file://arts }\n", nil)
	if p, ok := env.Artifacts.Local(); err != nil || !ok || p != filepath.Join(dir, "arts") {
		t.Fatalf("a relative directory: %q, %v", p, err)
	}
	if _, ok := Defaults().Artifacts.Local(); ok {
		t.Fatal("artifacts.url has a default")
	}
	b, p, ok := ArtifactsConfig{URL: "s3://bucket/pre/fix"}.S3()
	if !ok || b != "bucket" || p != "pre/fix" {
		t.Fatalf("S3() = %q, %q, %v", b, p, ok)
	}
	if _, _, ok := (ArtifactsConfig{URL: "file://a"}).S3(); ok {
		t.Fatal("a directory is a bucket")
	}
	if _, err := ParseEnvironment([]byte("artifacts: { url: \"ftp://x\" }\n")); err == nil {
		t.Fatal("ftp://: no error")
	}
}

// TestLoadEnvironmentPaths: the relative paths the file sets resolve against its directory.
func TestLoadEnvironmentPaths(t *testing.T) {
	t.Parallel()
	for src, got := range map[string]func(*Environment) string{
		"agents: { linuxBinary: bin/millstream }\n": func(e *Environment) string { return e.Agents.LinuxBinary },
		"auth: { token: { file: tok } }\n":          func(e *Environment) string { return e.Auth.Token.File },
		"node: { token: { file: tok } }\n":          func(e *Environment) string { return e.Node.Token.File },
		"tls: { cert: c.pem, key: k.pem }\n":        func(e *Environment) string { return e.TLS.Key },
		"improve: { patchFile: { dir: p } }\n":      func(e *Environment) string { return e.Improve.PatchFile.Dir },
		"secrets: { p: { k: { file: k.txt } } }\n":  func(e *Environment) string { return e.Secrets["p"]["k"].File },
	} {
		env, dir, err := load(t, src, nil)
		if err != nil || !filepath.IsAbs(got(env)) || !strings.HasPrefix(got(env), dir) {
			t.Fatalf("%s: %+v, %v", src, env, err)
		}
	}
}

func TestSecretLookup(t *testing.T) {
	vars := map[string]string{"MILLSTREAM_SECRET_API_KEY": "from-default", "LISTINGS_TOKEN": "mapped"}
	lookup := func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
	bare := EnvSecrets(lookup)("p")
	if v, err := bare("apiKey"); v != "from-default" || err != nil {
		t.Fatalf("without a secrets key: %q, %v", v, err)
	}
	if _, err := bare("other"); !errors.Is(err, ErrSecretUnset) || !strings.Contains(err.Error(), "MILLSTREAM_SECRET_OTHER") {
		t.Fatalf("unset: %v", err)
	}
	env := &Environment{Secrets: map[string]map[string]SecretSource{"p": {"token": {Env: "LISTINGS_TOKEN"}, "gone": {Env: "NOPE"}}}}
	mapped := env.SecretLookup("p", lookup)
	if v, err := mapped("token"); v != "mapped" || err != nil {
		t.Fatalf("mapped: %q, %v", v, err)
	}
	if _, err := mapped("apiKey"); err == nil || errors.Is(err, ErrSecretUnset) ||
		!strings.Contains(err.Error(), "not mapped for pipeline p") {
		t.Fatalf("unmapped, the default variable set: %v", err)
	}
	if _, err := mapped("gone"); !errors.Is(err, ErrSecretUnset) || !strings.Contains(err.Error(), "NOPE is not set") {
		t.Fatalf("mapped to an unset variable: %v", err)
	}
	// A missing file holds no value yet; an unreadable one (here a directory) is a misconfiguration.
	dir := testdir.New(t)
	files := &Environment{Secrets: map[string]map[string]SecretSource{"p": {
		"missing": {File: dir + "/nope"}, "broken": {File: dir},
	}}}
	if _, err := files.SecretLookup("p", lookup)("missing"); !errors.Is(err, ErrSecretUnset) {
		t.Fatalf("missing file: %v", err)
	}
	if _, err := files.SecretLookup("p", lookup)("broken"); err == nil || errors.Is(err, ErrSecretUnset) {
		t.Fatalf("unreadable file: %v", err)
	}
	if _, err := env.SecretLookup("q", lookup)("token"); err == nil {
		t.Fatal("another pipeline's grant: no error")
	}
	envLookup := env.SecretEnvLookup("p", lookup)
	if v, ok := envLookup("MILLSTREAM_SECRET_TOKEN"); v != "mapped" || !ok {
		t.Fatalf("the mapped secret's variable: %q, %v", v, ok)
	}
	if _, ok := envLookup("MILLSTREAM_SECRET_API_KEY"); ok {
		t.Fatal("an unmapped secret's variable resolved")
	}
	if v, ok := envLookup("LISTINGS_TOKEN"); v != "mapped" || !ok {
		t.Fatalf("another variable: %q, %v", v, ok)
	}
}

func TestSecretSourceValue(t *testing.T) {
	t.Parallel()
	lookup := func(name string) (string, bool) {
		v, ok := map[string]string{"SET": "postgres://x", "EMPTY": ""}[name]
		return v, ok
	}
	file := filepath.Join(testdir.New(t), "dsn")
	if err := os.WriteFile(file, []byte("postgres://from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(testdir.New(t), "empty")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		src  SecretSource
		want string // "" for an error
	}{
		{SecretSource{Env: "SET"}, "postgres://x"},
		{SecretSource{Env: "EMPTY"}, ""},
		{SecretSource{Env: "UNSET"}, ""},
		{SecretSource{File: file}, "postgres://from-file"},
		{SecretSource{File: empty}, ""},
		{SecretSource{File: filepath.Join(testdir.New(t), "absent")}, ""},
	} {
		got, err := tt.src.Value(lookup)
		if got != tt.want || (err == nil) != (tt.want != "") {
			t.Errorf("%+v.Value() = %q, %v; want %q", tt.src, got, err, tt.want)
		}
	}
}

func TestEnvironmentFileIsNotPartOfThePackage(t *testing.T) {
	t.Parallel()
	dir := testdir.New(t)
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(minimalPipeline), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, EnvironmentFile), []byte("runtime: { container: podman }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	after, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := after.Files[EnvironmentFile]; ok || after.Hash != before.Hash {
		t.Fatalf("the environment file entered the package: %v", after.Files)
	}
}

func TestLockRoundTrip(t *testing.T) {
	t.Parallel()
	l := &Lock{Actions: map[string]LockEntry{
		"lab/score@1":      {Version: "1.10.0", Image: "ghcr.io/lab/actions/score:1.10.0", Digest: digest2},
		"./actions/report": {Image: "report:dev", Digest: idReport},
	}}
	data := l.Marshal()
	want := `# Written by millstream lock: the image of each container and registry action, pinned by digest.
actions:
  "./actions/report":
    image: "report:dev"
    digest: "` + idReport + `"
  "lab/score@1":
    version: "1.10.0"
    image: "ghcr.io/lab/actions/score:1.10.0"
    digest: "` + digest2 + `"
`
	if string(data) != want {
		t.Fatalf("lock file:\n%s\nwant:\n%s", data, want)
	}
	back, err := ParseLock(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.Actions) != 2 || back.Actions["lab/score@1"] != l.Actions["lab/score@1"] ||
		back.Actions["./actions/report"] != l.Actions["./actions/report"] {
		t.Fatalf("parsed back %+v", back)
	}
	if empty := (&Lock{}).Marshal(); !strings.HasSuffix(string(empty), "actions: {}\n") {
		t.Fatalf("empty lock %q", empty)
	}
	for _, src := range []string{"", "actions:\n", "actions: {}\n"} {
		if l, err := ParseLock([]byte(src)); err != nil || len(l.Actions) != 0 {
			t.Fatalf("ParseLock(%q) = %+v, %v", src, l, err)
		}
	}
}

func TestParseLockErrors(t *testing.T) {
	t.Parallel()
	tests := map[string]struct{ src, want string }{
		"unknown top key":   {"pins: {}\n", "pins: unknown key"},
		"unknown entry key": {"actions: { a: { image: x, digest: y, tag: z } }\n", "actions.a.tag: unknown key"},
		"no image":          {"actions: { a: { digest: y } }\n", "actions.a.image: is required"},
		"no digest":         {"actions: { a: { image: x } }\n", "actions.a.digest: is required"},
		"empty version":     {"actions: { a: { version: \"\", image: x, digest: y } }\n", "actions.a.version: must not be empty"},
		"actions list":      {"actions: [a]\n", "actions: must be a mapping"},
		"entry scalar":      {"actions: { a: x }\n", "actions.a: must be a mapping"},
		"top list":          {"- a\n", "must be a mapping"},
		"yaml":              {"actions: {\n", "parse YAML"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ParseLock([]byte(tt.src))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}
