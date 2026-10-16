package pipeline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
		empty.LessonsMaxTokens != 0 {
		t.Fatalf("empty environment %+v, %v", empty, err)
	}
}

func TestParseEnvironmentErrors(t *testing.T) {
	t.Parallel()
	tests := map[string]struct{ src, want string }{
		"unknown key":       {"registry: {}\n", "line 1: registry: unknown key"},
		"later key":         {"auth: { token: { env: T } }\n", "line 1: auth: is not supported by this build yet"},
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
	file := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(file, []byte("postgres://from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(t.TempDir(), "empty")
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
		{SecretSource{File: filepath.Join(t.TempDir(), "absent")}, ""},
	} {
		got, err := tt.src.Value(lookup)
		if got != tt.want || (err == nil) != (tt.want != "") {
			t.Errorf("%+v.Value() = %q, %v; want %q", tt.src, got, err, tt.want)
		}
	}
}

func TestLoadEnvironment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
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
	dir := t.TempDir()
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
