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

func TestParseEnvironment(t *testing.T) {
	t.Parallel()
	env, err := ParseEnvironment([]byte(`
registries: { lab: ghcr.io/monolithiclab/millstream-actions/, local: "localhost:5000/actions" }
runtime: { container: podman }
prices: { claude-sonnet-5-5: { inPerMtok: 3, outPerMtok: 15.5 } }
agents: { allowUnsandboxed: true, image: "agents:1", linuxBinary: /opt/millstream-linux }
lessons: { maxTokens: 500 }
ui: { baseUrl: "https://millstream.example", listen: "127.0.0.1:8080" }
`))
	if err != nil {
		t.Fatal(err)
	}
	if env.Registries["lab"] != "ghcr.io/monolithiclab/millstream-actions" ||
		env.Registries["local"] != "localhost:5000/actions" || env.ContainerRuntime != RuntimePodman ||
		env.Prices["claude-sonnet-5-5"] != (Price{InPerMtok: 3, OutPerMtok: 15.5}) || !env.AllowUnsandboxed ||
		env.AgentImage != "agents:1" || env.AgentBinary != "/opt/millstream-linux" || env.LessonsMaxTokens != 500 || env.UIBaseURL != "https://millstream.example" ||
		env.UIListen != "127.0.0.1:8080" {
		t.Fatalf("environment %+v", env)
	}
	empty, err := ParseEnvironment(nil)
	if err != nil || empty.ContainerRuntime != RuntimeDocker || empty.Registries != nil ||
		empty.LessonsMaxTokens != 0 || !empty.NodeEmbedded || empty.Secrets != nil || empty.Mode != "" {
		t.Fatalf("empty environment %+v, %v", empty, err)
	}
}

// TestParseEnvironmentServerKeys reads every key the server uses, each to its field.
func TestParseEnvironmentServerKeys(t *testing.T) {
	env, err := ParseEnvironment([]byte(`
environment: development
artifacts: { s3: { bucket: arts, endpoint: "https://s3.example", region: fr-par, prefix: m/,
  accessKeyId: { env: S3_KEY }, secretAccessKey: { file: s3.secret } } }
listen: 0.0.0.0:7070
auth: { token: { env: MILLSTREAM_TOKEN }, user: ana }
tls: { cert: tls/cert.pem, key: tls/key.pem }
metrics: { public: true }
node: { embedded: false, token: { file: node.token } }
secrets:
  property-sourcing: { homesiteToken: { env: HOMESITE_TOKEN }, apiKey: { file: key } }
  other: {}
retention: { events: 90d, transcripts: 30d, rollups: 400d, every: 30m }
engine: { lockCheck: 2s, keepalive: 3s }
improve:
  enabled: true
  harness: api
  minSeverity: warning
  publish: apply
  budget: { dayUsd: 5 }
  evidence: { sample: 500, ttl: 30d }
  backtest: { sample: 200 }
  distill: { minDeliveries: 500, language: go, minAgreement: 0.98, minCoverage: 0.6 }
  filter: { maxFalseDrop: 0.01 }
`))
	if err != nil {
		t.Fatal(err)
	}
	day := 24 * time.Hour
	want := &Environment{
		ContainerRuntime: RuntimeDocker, Mode: ModeDevelopment, Listen: "0.0.0.0:7070",
		Artifacts: ArtifactsConfig{S3: &S3Config{Bucket: "arts", Endpoint: "https://s3.example", Region: "fr-par",
			Prefix: "m/", AccessKeyID: &SecretSource{Env: "S3_KEY"}, SecretAccessKey: &SecretSource{File: "s3.secret"}}},
		AuthToken: &SecretSource{Env: "MILLSTREAM_TOKEN"}, AuthUser: "ana",
		TLS: &TLSConfig{Cert: "tls/cert.pem", Key: "tls/key.pem"}, MetricsPublic: true,
		NodeToken: &SecretSource{File: "node.token"},
		Secrets: map[string]map[string]SecretSource{
			"property-sourcing": {"homesiteToken": {Env: "HOMESITE_TOKEN"}, "apiKey": {File: "key"}}, "other": {},
		},
		Retention: Retention{Events: 90 * day, Transcripts: 30 * day, Rollups: 400 * day, Every: 30 * time.Minute},
		Engine:    EngineConfig{LockCheck: 2 * time.Second, Keepalive: 3 * time.Second},
		Improve: Improve{Enabled: true, Harness: "api", MinSeverity: "warning", Publish: PublishApply, BudgetDayUSD: 5,
			EvidenceSample: 500, EvidenceTTL: 30 * day, BacktestSample: 200, DistillMinDeliveries: 500,
			DistillLanguage: "go", DistillMinAgreement: 0.98, DistillMinCoverage: 0.6, FilterMaxFalseDrop: 0.01},
	}
	if !reflect.DeepEqual(env, want) {
		t.Fatalf("environment\n%+v\nwant\n%+v", env, want)
	}
	local, err := ParseEnvironment([]byte("artifacts: { local: { path: arts } }\n"))
	if err != nil || local.Artifacts.LocalPath != "arts" || local.Artifacts.S3 != nil {
		t.Fatalf("local artifacts %+v, %v", local, err)
	}
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

// TestDeployEnvironmentParses: the Compose deployment's environment file parses strictly.
func TestDeployEnvironmentParses(t *testing.T) {
	env, err := LoadEnvironment(filepath.Join("..", "..", "deploy", "compose", "millstream.yaml"), true)
	if err != nil {
		t.Fatal(err)
	}
	if env.Store.PostgresDSN == nil || env.AuthToken == nil || env.NodeEmbedded || env.Listen != "0.0.0.0:7070" {
		t.Fatalf("deploy environment %+v", env)
	}
}

func TestSecretLookup(t *testing.T) {
	vars := map[string]string{"MILLSTREAM_SECRET_API_KEY": "from-default", "HOMESITE": "mapped"}
	lookup := func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
	bare := EnvSecrets(lookup)("p")
	if v, err := bare("apiKey"); v != "from-default" || err != nil {
		t.Fatalf("without a secrets key: %q, %v", v, err)
	}
	if _, err := bare("other"); err == nil || !strings.Contains(err.Error(), "MILLSTREAM_SECRET_OTHER") {
		t.Fatalf("unset: %v", err)
	}
	env := &Environment{Secrets: map[string]map[string]SecretSource{"p": {"token": {Env: "HOMESITE"}, "gone": {Env: "NOPE"}}}}
	mapped := env.SecretLookup("p", lookup)
	if v, err := mapped("token"); v != "mapped" || err != nil {
		t.Fatalf("mapped: %q, %v", v, err)
	}
	if _, err := mapped("apiKey"); err == nil || !strings.Contains(err.Error(), "not mapped for pipeline p") {
		t.Fatalf("unmapped, the default variable set: %v", err)
	}
	if _, err := mapped("gone"); err == nil || !strings.Contains(err.Error(), "NOPE is not set") {
		t.Fatalf("mapped to an unset variable: %v", err)
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
	if v, ok := envLookup("HOMESITE"); v != "mapped" || !ok {
		t.Fatalf("another variable: %q, %v", v, ok)
	}
}

func TestParseEnvironmentErrors(t *testing.T) {
	t.Parallel()
	tests := map[string]struct{ src, want string }{
		"unknown key":       {"registry: {}\n", "line 1: registry: unknown key"},
		"environment":       {"environment: staging\n", "environment: must be one of production, development"},
		"listen":            {"listen: 7070\n", "listen: want host:port"},
		"listen empty":      {"listen: \"\"\n", "listen: must not be empty"},
		"artifacts none":    {"artifacts: {}\n", "artifacts: want exactly one of local and s3"},
		"artifacts both":    {"artifacts: { local: { path: a }, s3: { bucket: b } }\n", "artifacts: want exactly one"},
		"artifacts key":     {"artifacts: { gcs: {} }\n", "artifacts.gcs: unknown key"},
		"local no path":     {"artifacts: { local: {} }\n", "artifacts.local.path: is required"},
		"s3 no bucket":      {"artifacts: { s3: { region: fr-par } }\n", "artifacts.s3.bucket: is required"},
		"s3 endpoint":       {"artifacts: { s3: { bucket: b, endpoint: s3.example } }\n", "artifacts.s3.endpoint: want an http"},
		"s3 key":            {"artifacts: { s3: { bucket: b, acl: private } }\n", "artifacts.s3.acl: unknown key"},
		"s3 inline key":     {"artifacts: { s3: { bucket: b, accessKeyId: AKIA } }\n", "artifacts.s3.accessKeyId: want { env: NAME }"},
		"s3 half creds":     {"artifacts: { s3: { bucket: b, accessKeyId: { env: K } } }\n", "artifacts.s3: set both"},
		"auth empty":        {"auth: {}\n", "auth.token: is required"},
		"auth inline":       {"auth: { token: s3cret }\n", "auth.token: want { env: NAME }"},
		"auth key":          {"auth: { oidc: {} }\n", "auth.oidc: unknown key"},
		"auth user":         {"auth: { token: { env: T }, user: \"\" }\n", "auth.user: must not be empty"},
		"tls no key":        {"tls: { cert: c.pem }\n", "tls.key: is required"},
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
		"retention":         {"retention: { events: forever }\n", "retention.events:"},
		"retention key":     {"retention: { items: 3d }\n", "retention.items: unknown key"},
		"engine key":        {"engine: { id: a }\n", "engine.id: unknown key"},
		"engine zero":       {"engine: { lockCheck: 0s }\n", "engine.lockCheck: duration \"0s\" must be positive"},
		"engine keepalive":  {"engine: { keepalive: often }\n", "engine.keepalive:"},
		"improve key":       {"improve: { autoApply: true }\n", "improve.autoApply: unknown key"},
		"improve harness":   {"improve: { harness: gpt }\n", "improve.harness: must be one of claude-code, api"},
		"improve publish":   {"improve: { publish: email }\n", "improve.publish: must be one of apply"},
		"improve severity":  {"improve: { minSeverity: high }\n", "improve.minSeverity: must be one of"},
		"improve sample":    {"improve: { evidence: { sample: 0 } }\n", "improve.evidence.sample: must be a positive"},
		"improve ttl":       {"improve: { evidence: { ttl: soon } }\n", "improve.evidence.ttl:"},
		"improve evidence":  {"improve: { evidence: { size: 3 } }\n", "improve.evidence.size: unknown key"},
		"improve budget":    {"improve: { budget: { dayUsd: -1 } }\n", "improve.budget.dayUsd: must be a non-negative"},
		"improve agreement": {"improve: { distill: { minAgreement: 2 } }\n", "improve.distill.minAgreement: must be a number from 0 to 1"},
		"improve filter":    {"improve: { filter: 3 }\n", "improve.filter: must be a mapping"},
		"improve prod":      {"environment: production\nimprove: { enabled: true }\n", "improve: enabled: the improve pipeline runs in development"},
		"improve prod pub":  {"environment: production\nimprove: { publish: apply }\n", "improve: publish: apply is refused in production"},
		"store both":        {"store: { sqlite: { path: a }, postgres: { dsn: { env: D } } }\n", "store: want exactly one"},
		"store none":        {"store: {}\n", "store: want exactly one of sqlite and postgres"},
		"store scalar":      {"store: sqlite\n", "store: must be a mapping"},
		"store key":         {"store: { mysql: {} }\n", "store.mysql: unknown key"},
		"sqlite no path":    {"store: { sqlite: {} }\n", "store.sqlite.path: is required"},
		"sqlite key":        {"store: { sqlite: { file: a } }\n", "store.sqlite.file: unknown key"},
		"dsn missing":       {"store: { postgres: {} }\n", "store.postgres.dsn: is required"},
		"dsn inline":        {"store: { postgres: { dsn: \"postgres://u:pw@h/db\" } }\n", "store.postgres.dsn: want { env: NAME }"},
		"dsn both":          {"store: { postgres: { dsn: { env: D, file: f } } }\n", "store.postgres.dsn: want exactly one"},
		"dsn bad env":       {"store: { postgres: { dsn: { env: \"my dsn\" } } }\n", "store.postgres.dsn.env: want an environment"},
		"dsn source key":    {"store: { postgres: { dsn: { vault: x } } }\n", "store.postgres.dsn.vault: unknown key"},
		"postgres key":      {"store: { postgres: { dsn: { env: D }, pool: 3 } }\n", "store.postgres.pool: unknown key"},
		"not a mapping":     {"- a\n", "must be a mapping"},
		"bad namespace":     {"registries: { Lab: ghcr.io/x }\n", "registries.Lab: a namespace is lowercase"},
		"scheme":            {"registries: { lab: https://ghcr.io/x }\n", "registries.lab: want a repository prefix"},
		"digest":            {"registries: { lab: ghcr.io/x@sha256:1 }\n", "registries.lab"},
		"empty prefix":      {"registries: { lab: \"\" }\n", "registries.lab: must not be empty"},
		"bad runtime":       {"runtime: { container: lxc }\n", "runtime.container: must be one of docker, podman"},
		"runtime key":       {"runtime: { exec: true }\n", "runtime.exec: unknown key"},
		"runtime scalar":    {"runtime: docker\n", "runtime: must be a mapping"},
		"price missing":     {"prices: { m: { inPerMtok: 1 } }\n", "prices.m.outPerMtok: is required"},
		"price negative":    {"prices: { m: { inPerMtok: -1, outPerMtok: 1 } }\n", "prices.m.inPerMtok: must be a non-negative"},
		"price key":         {"prices: { m: { in: 1 } }\n", "prices.m.in: unknown key"},
		"prices scalar":     {"prices: 3\n", "prices: must be a mapping"},
		"price scalar":      {"prices: { m: 3 }\n", "prices.m: must be a mapping"},
		"agents not bool":   {"agents: { allowUnsandboxed: yes please }\n", "agents.allowUnsandboxed: must be true or false"},
		"agents key":        {"agents: { sandbox: none }\n", "agents.sandbox: unknown key"},
		"agents scalar":     {"agents: true\n", "agents: must be a mapping"},
		"agents image":      {"agents: { image: \"\" }\n", "agents.image: must not be empty"},
		"agents binary":     {"agents: { linuxBinary: [x] }\n", "agents.linuxBinary: must be a string"},
		"registries scalar": {"registries: x\n", "registries: must be a mapping"},
		"lessons zero":      {"lessons: { maxTokens: 0 }\n", "lessons.maxTokens: must be a positive integer"},
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

func TestParseEnvironmentStore(t *testing.T) {
	t.Parallel()
	for src, want := range map[string]StoreConfig{
		"store: { sqlite: { path: state/m.db } }\n":                  {SQLitePath: "state/m.db"},
		"store: { postgres: { dsn: { env: MILLSTREAM_DSN } } }\n":    {PostgresDSN: &SecretSource{Env: "MILLSTREAM_DSN"}},
		"store: { postgres: { dsn: { file: /run/secrets/dsn } } }\n": {PostgresDSN: &SecretSource{File: "/run/secrets/dsn"}},
		"registries: {}\n": {},
	} {
		env, err := ParseEnvironment([]byte(src))
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		got := env.Store
		if got.SQLitePath != want.SQLitePath || (got.PostgresDSN == nil) != (want.PostgresDSN == nil) ||
			got.PostgresDSN != nil && *got.PostgresDSN != *want.PostgresDSN {
			t.Errorf("%s: store %+v, want %+v", src, got, want)
		}
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

func TestLoadEnvironment(t *testing.T) {
	t.Parallel()
	dir := testdir.New(t)
	missing := filepath.Join(dir, "absent.yaml")
	if env, err := LoadEnvironment(missing, false); err != nil || env.ContainerRuntime != RuntimeDocker {
		t.Fatalf("an optional missing file: %+v, %v", env, err)
	}
	if _, err := LoadEnvironment(missing, true); err == nil {
		t.Fatal("a required missing file: no error")
	}
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("store: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadEnvironment(bad, false); err == nil || !strings.Contains(err.Error(), bad) {
		t.Fatalf("a bad file: %v", err)
	}
	// agents.linuxBinary is relative to the environment file.
	rel := filepath.Join(dir, "rel.yaml")
	if err := os.WriteFile(rel, []byte("agents: { linuxBinary: bin/millstream }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if env, err := LoadEnvironment(rel, true); err != nil || env.AgentBinary != filepath.Join(dir, "bin", "millstream") {
		t.Fatalf("relative linuxBinary: %+v, %v", env, err)
	}
	// So are store.sqlite.path and a DSN's file.
	for src, want := range map[string]func(*Environment) string{
		"store: { sqlite: { path: state/m.db } }\n":             func(e *Environment) string { return e.Store.SQLitePath },
		"store: { postgres: { dsn: { file: secrets/dsn } } }\n": func(e *Environment) string { return e.Store.PostgresDSN.File },
		"artifacts: { local: { path: arts } }\n":                func(e *Environment) string { return e.Artifacts.LocalPath },
		"auth: { token: { file: tok } }\n":                      func(e *Environment) string { return e.AuthToken.File },
		"tls: { cert: c.pem, key: k.pem }\n":                    func(e *Environment) string { return e.TLS.Key },
		"secrets: { p: { k: { file: k.txt } } }\n":              func(e *Environment) string { return e.Secrets["p"]["k"].File },
	} {
		if err := os.WriteFile(rel, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
		env, err := LoadEnvironment(rel, true)
		if err != nil || !filepath.IsAbs(want(env)) || !strings.HasPrefix(want(env), dir) {
			t.Fatalf("%s: %+v, %v", src, env, err)
		}
	}
}

// TestEnvironmentFileIsNotPartOfThePackage: millstream.yaml at a package's root changes neither its files nor its
// hash.
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

func TestStoreConfig(t *testing.T) {
	if (StoreConfig{}).Set() || (StoreConfig{}).Path("d.db") != "d.db" {
		t.Fatal("the zero store config")
	}
	c := StoreConfig{SQLitePath: "s.db"}
	if !c.Set() || c.Path("d.db") != "s.db" {
		t.Fatal("a SQLite path")
	}
}
