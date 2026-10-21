//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"os"
	osexec "os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/postgres"
)

// The soak (make soak, spec section 19's M4 row compressed): the Compose deployment of deploy/compose, in a project
// of its own, runs three pipelines unattended for SOAK_DURATION: the M1 fixture with its builtins (a filter and an
// auto-approving review) fired by a cron trigger every minute and by a webhook, a pipeline importing its crawled
// listings, and the M2 fixture whose score agent replays its recordings on the nodes, fired every minute. A chaos
// driver kills or restarts the active engine and kills or partitions the nodes at random intervals (SOAK_CHAOS on
// average) while the webhook is called. At the end the triggers are disabled, every run must settle, and the store
// must hold the crash suite's invariants; /metrics and the UI must answer. build/soak/report.md is the summary.
//
// It runs only when SOAK_DURATION is set (make soak sets it): make test-e2e skips it.

const (
	soakProject = "millstream-soak"
	soakNetwork = soakProject + "_default"
	soakImage   = "millstream:dev"
)

// soakEngines and soakNodes are the Compose services the chaos driver acts on, soakServices both.
var (
	soakEngines  = []string{"engine", "engine-standby"}
	soakNodes    = []string{"node-1", "node-2"}
	soakServices = slices.Concat(soakEngines, soakNodes)
)

// soakOverride is the Compose file layered over deploy/compose/compose.yaml: host ports chosen by Docker (the
// project never collides with a real deployment), Postgres published on the loopback for the final checks, the
// soak's certificate in place of the deployment's (volumes merge by target), the scorer's secret and the fake
// model's price on the engines, and on the nodes agents allowed unsandboxed, the same price and the M2 recordings
// the harness replays.
const soakOverride = `services:
  postgres:
    ports: ["127.0.0.1::5432"]
  engine: &engine
    ports: !override ["127.0.0.1::7070"]
    environment:
      MILLSTREAM_SECRET_SCORER_KEY: sk-fake-soak
      MILLSTREAM_PRICES: ${SOAK_PRICES}
    volumes:
      - ${SOAK_DIR}/tls:/etc/millstream/tls:ro
  engine-standby: *engine
  node-1: &node
    environment:
      MILLSTREAM_AGENTS_ALLOW_UNSANDBOXED: "true"
      MILLSTREAM_PRICES: ${SOAK_PRICES}
      MILLSTREAM_HARNESS_MODE: replay
      MILLSTREAM_HARNESS_DIR: /etc/millstream/recordings
    volumes:
      - ${SOAK_DIR}/tls/ca.pem:/etc/millstream/tls/ca.pem:ro
      - ${SOAK_RECORDINGS}:/etc/millstream/recordings:ro
  node-2: *node
`

// soakConfig is the soak's knobs, from the environment.
type soakConfig struct {
	duration time.Duration // SOAK_DURATION: how long the chaos runs
	chaos    time.Duration // SOAK_CHAOS: the mean interval between chaos events
	webhook  time.Duration // SOAK_WEBHOOK: the interval between webhook calls
	stuck    time.Duration // SOAK_STUCK: a run older than this at the end must be settled
	seed     uint64        // SOAK_SEED: the chaos schedule's seed
}

func soakConfigFromEnv(t *testing.T) (soakConfig, bool) {
	t.Helper()
	d, ok := os.LookupEnv("SOAK_DURATION")
	if !ok {
		return soakConfig{}, false
	}
	dur := func(name, value string) time.Duration {
		if v, ok := os.LookupEnv(name); ok {
			value = v
		}
		x, err := time.ParseDuration(value)
		if err != nil || x <= 0 {
			t.Fatalf("%s=%q: want a positive duration", name, value)
		}
		return x
	}
	c := soakConfig{duration: dur("SOAK_DURATION", d), chaos: dur("SOAK_CHAOS", "2m"),
		webhook: dur("SOAK_WEBHOOK", "45s"), stuck: dur("SOAK_STUCK", "5m"), seed: uint64(time.Now().UnixNano())} // #nosec G115 -- a seed
	if s, ok := os.LookupEnv("SOAK_SEED"); ok {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("SOAK_SEED=%q: %v", s, err)
		}
		c.seed = n
	}
	return c, true
}

// soak is one soak's deployment and its tallies.
type soak struct {
	t        *testing.T
	cfg      soakConfig
	root     string            // the repository
	dir      string            // build/soak/_work: packages, certificates, environment files
	out      string            // build/soak: the report and the logs
	env      []string          // compose's variables
	ids      map[string]string // each service's container
	token    string
	client   *http.Client
	dsn      string
	password string
	hookPath string
	hookTok  string
	rng      *mrand.Rand

	mu         sync.Mutex
	events     []string // the chaos log
	kills      map[string]int
	partitions int
	takeovers  []takeover
	hooks      map[string]int // webhook outcomes
	checks     []string       // the end checks, each "pass: …" or "FAIL: …"
}

func TestSoak(t *testing.T) {
	cfg, ok := soakConfigFromEnv(t)
	if !ok {
		t.Skip("set SOAK_DURATION to run the soak (make soak)")
	}
	s := newSoak(t, cfg)
	s.up()
	s.applyPackages()
	start := time.Now()
	t.Logf("soak: %s of chaos every %s on average, seed %d", cfg.duration, cfg.chaos, cfg.seed)

	ctx, stop := context.WithTimeout(t.Context(), cfg.duration)
	defer stop()
	var wg sync.WaitGroup
	wg.Go(func() { s.webhooks(ctx) })
	s.chaos(ctx)
	wg.Wait()
	end := time.Now()

	s.restore()
	s.checkStuck(end)
	settled := s.quiesce(end)
	s.checkEndpoints()
	if !settled { // the invariants need every run settled: the report says what was stuck
		s.writeReport(start, end, snapshot{}, nil)
		return
	}
	s.compose(append([]string{"stop"}, soakServices...)...)
	snap, runs := s.checkInvariants()
	s.writeReport(start, end, snap, runs)
}

func newSoak(t *testing.T, cfg soakConfig) *soak {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	s := &soak{t: t, cfg: cfg, root: root, out: filepath.Join(root, "build", "soak"),
		rng:   mrand.New(mrand.NewPCG(cfg.seed, 0)), // #nosec G404 -- a reproducible chaos schedule
		kills: map[string]int{}, hooks: map[string]int{}}
	// Under the repository, which Docker Desktop shares; "_" keeps the packages' Go sources out of ./... .
	s.dir = filepath.Join(s.out, "_work")
	if err := os.RemoveAll(s.dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(s.dir, "packages"), 0o750); err != nil {
		t.Fatal(err)
	}
	s.token = randomHex(t)
	s.password = randomHex(t)
	if err := os.MkdirAll(filepath.Join(s.dir, "tls"), 0o750); err != nil {
		t.Fatal(err)
	}
	caPath, _, _ := writeCA(t, filepath.Join(s.dir, "tls"))
	caPEM, err := os.ReadFile(caPath) // #nosec G304 -- the test's own authority
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	s.client = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	// MILLSTREAM_AUTH_TOKEN is the engines' token, MILLSTREAM_TOKEN the one the apply container presents.
	s.env = []string{"MILLSTREAM_IMAGE=" + soakImage, "PGPASSWORD=" + s.password,
		"MILLSTREAM_AUTH_TOKEN=" + s.token, "MILLSTREAM_TOKEN=" + s.token, "MILLSTREAM_NODE_TOKEN=" + randomHex(t),
		"SOAK_DIR=" + s.dir, fmt.Sprintf("SOAK_PRICES={ %s: { inPerMtok: %d, outPerMtok: %d } }", m2Model,
			m2InPerMtok, m2OutPerMtok),
		"SOAK_RECORDINGS=" + filepath.Join(root, "examples", "m2-fixture", "recordings")}
	writeFile(t, filepath.Join(s.dir, "compose.soak.yaml"), soakOverride)
	s.buildPackages()
	return s
}

// randomHex returns 32 random bytes in hex: a token or a password.
func randomHex(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// writeFile writes a file the containers read: world-readable, since they run as another uid.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil { // #nosec G306 -- read by the containers' uid
		t.Fatal(err)
	}
}

// buildPackages writes the soak's three packages under packages/, their exec actions compiled for the Docker
// server's Linux: m1 is the builtins fixture with a cron trigger every minute and a webhook trigger, importer
// imports its crawled listings, and m2 is the M2 fixture without its dedupe (every run scores its day again through
// the recordings) with a cron trigger every minute.
func (s *soak) buildPackages() {
	t := s.t
	arch := strings.TrimSpace(s.run("docker", "version", "--format", "{{.Server.Arch}}"))
	platform := "linux-" + arch
	m1, importer, m2 := s.pkg("m1"), s.pkg("importer"), s.pkg("m2")
	if err := os.CopyFS(m1, os.DirFS(filepath.Join(s.root, "examples", "m1-fixture"))); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(m2, os.DirFS(filepath.Join(s.root, "examples", "m2-fixture"))); err != nil {
		t.Fatal(err)
	}
	for _, p := range m2NotPackaged {
		if err := os.RemoveAll(filepath.Join(m2, p)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(importer, 0o750); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(importer, "pipeline.yaml"), importerPipeline)
	everyMinute := [2]string{`daily: { cron: "0 6 * * *" }`, `daily: { cron: "* * * * *" }`}
	if err := errors.Join(
		insertBuiltins(filepath.Join(m1, "pipeline.yaml")),
		// No dedupe: the crawl draws the same listings at every run, which a dedupe would drop after the first.
		rewrite(filepath.Join(m1, "pipeline.yaml"), everyMinute,
			[2]string{"  adhoc: { manual: {} }\n", "  adhoc: { manual: {} }\n  hook: { webhook: {} }\n"},
			[2]string{"in: [trigger.adhoc, trigger.daily]", "in: [trigger.adhoc, trigger.daily, trigger.hook]"},
			[2]string{"  unique:\n    action: dedupe\n    in: listings.crawled\n    out: listings.unique\n" +
				"    config: { key: item.id, window: 30d }\n\n", ""},
			[2]string{"    in: listings.unique\n", "    in: listings.crawled\n"}),
		// Its trigger is renamed: a cron trigger of the same name in m1 would give both pipelines' runs and items the
		// same ids, which the snapshot, keyed by id, would merge.
		rewrite(filepath.Join(m2, "pipeline.yaml"), [2]string{`daily: { cron: "0 6 * * *" }`, `minutely: { cron: "* * * * *" }`},
			[2]string{"in: [trigger.adhoc, trigger.daily]", "in: [trigger.adhoc, trigger.minutely]"},
			[2]string{dedupeStep, ""}, [2]string{"    in: listings.unique\n", "    in: listings.raw\n"}),
	); err != nil {
		t.Fatal(err)
	}
	builds := map[string]string{}
	for _, a := range []string{"crawl", "score", "rentability", "report"} {
		builds[filepath.Join(m1, "actions", a, "bin", platform, a)] = "./examples/m1-fixture/actions/" + a
	}
	for name, pkg := range m2Actions {
		builds[filepath.Join(m2, "actions", name, "bin", platform, name)] = pkg
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(builds))
	for out, pkg := range builds {
		wg.Go(func() {
			errs <- goBuildEnv(s.root, out, pkg, []string{"GOOS=linux", "GOARCH=" + arch, "CGO_ENABLED=0"})
		})
	}
	wg.Wait()
	close(errs)
	var all []error
	for err := range errs {
		all = append(all, err)
	}
	if err := errors.Join(all...); err != nil {
		t.Fatal(err)
	}
}

func (s *soak) pkg(name string) string { return filepath.Join(s.dir, "packages", name) }

// run runs a command to completion and returns its output, failing the test when it fails.
func (s *soak) run(name string, args ...string) string {
	s.t.Helper()
	out, err := s.try(name, args...)
	if err != nil {
		s.t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, lastLines(out, 40))
	}
	return out
}

// try runs a command to completion and returns its combined output.
func (s *soak) try(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := osexec.CommandContext(ctx, name, args...) // #nosec G204 -- docker and go, with the soak's own arguments
	cmd.Env = append(os.Environ(), s.env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// composeArgs are docker compose's arguments for the soak's own project.
func (s *soak) composeArgs(args ...string) []string {
	return append([]string{"compose", "-p", soakProject, "--project-directory",
		filepath.Join(s.root, "deploy", "compose"), "-f", filepath.Join(s.root, "deploy", "compose", "compose.yaml"),
		"-f", filepath.Join(s.dir, "compose.soak.yaml")}, args...)
}

func (s *soak) compose(args ...string) string {
	s.t.Helper()
	return s.run("docker", s.composeArgs(args...)...)
}

// container returns the id of a service's container, which a kill, a start or a restart keeps.
func (s *soak) container(service string) string { return s.ids[service] }

// url returns the https address of an engine's API on the host: its port is chosen again at each start.
func (s *soak) url(service string) (string, error) {
	addr, err := s.try("docker", s.composeArgs("port", service, "7070")...)
	if err != nil {
		return "", fmt.Errorf("%s: %w: %s", service, err, strings.TrimSpace(addr))
	}
	return "https://" + strings.TrimSpace(addr), nil
}

// request sends a request with the API token to an engine, a POST with an empty JSON object, and returns the
// status and the body.
func (s *soak) request(service, method, path string) (int, []byte, error) {
	body := ""
	if method == http.MethodPost {
		body = "{}"
	}
	return s.send(service, method, path, body, map[string]string{"Authorization": "Bearer " + s.token})
}

// send sends a request to an engine, a JSON body when there is one, and returns the status and the body.
func (s *soak) send(service, method, path, body string, header map[string]string) (int, []byte, error) {
	base, err := s.url(service)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequest(method, base+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

// up starts the deployment and waits until one engine leads, the other stands by and both nodes are registered.
// The deployment is torn down when the test ends, its logs saved first.
func (s *soak) up() {
	t := s.t
	t.Cleanup(s.down)
	s.compose("up", "-d", "--quiet-pull")
	s.ids = map[string]string{}
	for _, svc := range soakServices {
		if s.ids[svc] = strings.TrimSpace(s.compose("ps", "-a", "-q", svc)); s.ids[svc] == "" {
			t.Fatalf("no container for %s", svc)
		}
	}
	pgAddr := strings.TrimSpace(s.compose("port", "postgres", "5432"))
	s.dsn = "postgres://millstream:" + s.password + "@" + pgAddr + "/millstream?sslmode=disable"
	deadline := time.Now().Add(3 * time.Minute)
	for {
		active, view, err := s.active()
		if n := view.liveNodes(); err == nil && n == len(soakNodes) {
			t.Logf("deployment up: %s active, %d nodes", active, n)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the deployment did not come up: %v\n%s", err, lastLines(s.compose("logs", "--tail", "30"), 120))
		}
		time.Sleep(time.Second)
	}
}

// down saves each service's log under build/soak/logs, after a failure with the goroutines of the engines and the
// nodes (SIGQUIT), and removes the project's containers and volumes.
func (s *soak) down() {
	if s.t.Failed() {
		for _, svc := range soakServices {
			_, _ = s.try("docker", "kill", "--signal", "QUIT", s.container(svc))
		}
		time.Sleep(2 * time.Second)
	}
	logs := filepath.Join(s.out, "logs")
	if err := os.MkdirAll(logs, 0o750); err == nil {
		for _, svc := range append([]string{"postgres"}, soakServices...) {
			out, _ := s.try("docker", s.composeArgs("logs", "--no-color", svc)...)
			_ = os.WriteFile(filepath.Join(logs, svc+".log"), []byte(out), 0o600)
		}
	}
	if out, err := s.try("docker", s.composeArgs("down", "-v", "--remove-orphans", "--timeout", "10")...); err != nil {
		s.t.Errorf("compose down: %v\n%s", err, out)
	}
}

// get calls an engine's API and decodes its JSON answer into v.
func (s *soak) get(service, path string, v any) error {
	code, body, err := s.request(service, http.MethodGet, path)
	switch {
	case err != nil:
		return err
	case code != http.StatusOK:
		return fmt.Errorf("GET %s on %s: %d", path, service, code)
	}
	return json.Unmarshal(body, v)
}

// soakNodesView is what the soak reads of GET /api/v1/nodes.
type soakNodesView struct {
	Engine struct{ State string }
	Nodes  []struct {
		Embedded bool
		State    string
	}
}

// active returns the engine service that leads, and its view of the nodes.
func (s *soak) active() (string, soakNodesView, error) {
	var errs []error
	for _, svc := range soakEngines {
		var v soakNodesView
		if err := s.get(svc, "/api/v1/nodes", &v); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", svc, err))
			continue
		}
		if v.Engine.State == "active" {
			return svc, v, nil
		}
	}
	return "", soakNodesView{}, fmt.Errorf("no engine leads: %w", errors.Join(errs...))
}

// waitActive returns the engine service that leads, waiting up to a minute for one to.
func (s *soak) waitActive() string {
	s.t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		active, _, err := s.active()
		if err == nil {
			return active
		}
		if time.Now().After(deadline) {
			s.t.Fatal(err)
		}
		time.Sleep(time.Second)
	}
}

// liveNodes returns how many remote nodes the view lists as active.
func (v soakNodesView) liveNodes() int {
	n := 0
	for _, node := range v.Nodes {
		if !node.Embedded && node.State == "active" {
			n++
		}
	}
	return n
}

// applyPackages applies the three packages through the active engine, with the image's own CLI in a container
// sharing that engine's network namespace (the API answers loopback hosts; the CLI trusts the soak's authority
// through SSL_CERT_FILE), and keeps the webhook trigger's token apply prints.
func (s *soak) applyPackages() {
	t := s.t
	active := s.waitActive()
	out := s.run("docker", "run", "--rm", "--network", "container:"+s.container(active), "--read-only",
		"--tmpfs", "/tmp", "-v", filepath.Join(s.dir, "packages")+":/packages:ro",
		"-v", filepath.Join(s.dir, "tls", "ca.pem")+":/ca.pem:ro", "-e", "SSL_CERT_FILE=/ca.pem",
		"-e", "MILLSTREAM_TOKEN", "-e", "MILLSTREAM_SECRET_SCORER_KEY=sk-fake-soak", soakImage,
		"--server", "https://127.0.0.1:7070", "apply", "--message", "soak",
		"/packages/m1", "/packages/importer", "/packages/m2")
	m := regexp.MustCompile(`webhook hook: POST (\S+) with Authorization: Bearer (\S+) `).FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("apply printed no webhook token:\n%s", out)
	}
	s.hookPath, s.hookTok = m[1], m[2]
	t.Logf("applied through %s:\n%s", active, strings.ReplaceAll(out, s.hookTok, "mswh_…"))
}

// event records a chaos event in the log and the report.
func (s *soak) event(format string, args ...any) {
	line := time.Now().Format("15:04:05") + " " + fmt.Sprintf(format, args...)
	s.mu.Lock()
	s.events = append(s.events, line)
	s.mu.Unlock()
	s.t.Log(line)
}

// chaos acts on the deployment at random intervals, of mean cfg.chaos, until ctx ends: it kills (sometimes
// restarts) the active engine, kills a node, or cuts a node off its network for a while; every container it stops
// is started again before the next event.
func (s *soak) chaos(ctx context.Context) {
	for {
		wait := time.Duration(s.rng.ExpFloat64() * float64(s.cfg.chaos))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		switch r := s.rng.Float64(); {
		case r < 0.3:
			s.failover(false)
		case r < 0.4:
			s.failover(true)
		case r < 0.75:
			s.killNode()
		default:
			s.partitionNode()
		}
	}
}

// downtime returns how long a stopped container stays down: between 2 and 30 seconds.
func (s *soak) downtime() time.Duration { return 2*time.Second + s.rng.N(28*time.Second) }

// failover kills the active engine (SIGKILL, or with graceful a docker restart: SIGTERM, then started again),
// measures the standby's takeover from its log, and starts the killed one again after a while, as the new standby.
func (s *soak) failover(graceful bool) {
	active, _, err := s.active()
	if err != nil {
		s.event("failover skipped: %v", err)
		return
	}
	standby := soakEngines[0]
	if active == standby {
		standby = soakEngines[1]
	}
	id, end, how := s.container(active), time.Now(), "killed"
	if graceful {
		how = "restarted"
		s.run("docker", "restart", "--timeout", "10", id)
	} else {
		s.run("docker", "kill", id)
	}
	tk, ok := s.measureTakeover(standby, end)
	s.mu.Lock()
	s.kills[active]++
	if ok {
		s.takeovers = append(s.takeovers, tk)
	}
	s.mu.Unlock()
	if ok {
		s.event("%s %s: %s led after %s, first dispatch after %s", how, active, standby,
			tk.lead.Round(time.Millisecond), tk.dispatch.Round(time.Millisecond))
	} else {
		s.event("%s %s: %s did not take over within a minute", how, active, standby)
	}
	if !graceful {
		time.Sleep(s.downtime())
		s.run("docker", "start", id)
	}
}

// measureTakeover is failover_test.go's over a container's log: it reads the standby's log after end, the active
// engine's end, for its "leading the deployment" and its next "first dispatch" (none when there was nothing to
// dispatch): it waits up to a minute for the first, 30 seconds more for the second.
func (s *soak) measureTakeover(standby string, end time.Time) (takeover, bool) {
	id := s.container(standby)
	since := end.Add(-time.Second).UTC().Format(time.RFC3339Nano)
	find := func(msg string, from time.Time) (time.Time, bool) {
		out, err := s.try("docker", "logs", "--since", since, id)
		if err != nil {
			return time.Time{}, false
		}
		for line := range strings.Lines(out) {
			if !strings.Contains(line, `"msg":"`+msg+`"`) {
				continue
			}
			if ts := logTime(s.t, strings.TrimSpace(line)); !ts.Before(from) {
				return ts, true
			}
		}
		return time.Time{}, false
	}
	poll := func(msg string, from time.Time, wait time.Duration) (time.Time, bool) {
		deadline := time.Now().Add(wait)
		for {
			if ts, ok := find(msg, from); ok {
				return ts, true
			}
			if time.Now().After(deadline) {
				return time.Time{}, false
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	led, ok := poll("leading the deployment", end, time.Minute)
	if !ok {
		return takeover{}, false
	}
	tk := takeover{lead: led.Sub(end)}
	if first, ok := poll("first dispatch", led, 30*time.Second); ok {
		tk.dispatch = first.Sub(end)
	}
	return tk, true
}

// killNode kills a node and starts it again after a while.
func (s *soak) killNode() {
	node := soakNodes[s.rng.IntN(len(soakNodes))]
	id := s.container(node)
	s.run("docker", "kill", id)
	down := s.downtime()
	s.mu.Lock()
	s.kills[node]++
	s.mu.Unlock()
	s.event("killed %s for %s", node, down.Round(time.Second))
	time.Sleep(down)
	s.run("docker", "start", id)
}

// partitionNode cuts a node off the project's network for between 5 and 90 seconds, past its lease at times
// (60 s), then connects it again.
func (s *soak) partitionNode() {
	node := soakNodes[s.rng.IntN(len(soakNodes))]
	id := s.container(node)
	cut := 5*time.Second + s.rng.N(85*time.Second)
	if out, err := s.try("docker", "network", "disconnect", soakNetwork, id); err != nil {
		s.event("partition of %s skipped: %v: %s", node, err, strings.TrimSpace(out)) // not running: restarting
		return
	}
	s.mu.Lock()
	s.partitions++
	s.mu.Unlock()
	s.event("partitioned %s for %s", node, cut.Round(time.Second))
	time.Sleep(cut)
	s.run("docker", "network", "connect", soakNetwork, id)
}

// webhooks calls the m1 webhook trigger every cfg.webhook until ctx ends, on the active engine (another engine
// answering 503 is tried next), and tallies the outcomes.
func (s *soak) webhooks(ctx context.Context) {
	tick := time.NewTicker(s.cfg.webhook)
	defer tick.Stop()
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		outcome := "unanswered"
		for _, svc := range soakEngines {
			code, err := s.callWebhook(svc, n)
			if err != nil {
				continue
			}
			outcome = strconv.Itoa(code)
			if code != http.StatusServiceUnavailable {
				break
			}
		}
		s.mu.Lock()
		s.hooks[outcome]++
		s.mu.Unlock()
	}
}

func (s *soak) callWebhook(service string, n int) (int, error) {
	code, _, err := s.send(service, http.MethodPost, s.hookPath, fmt.Sprintf(`{"call":%d}`, n),
		map[string]string{"Authorization": "Bearer " + s.hookTok, "Idempotency-Key": "soak-" + strconv.Itoa(n)})
	return code, err
}

// restore starts every container and connects every node again, whatever the last chaos event left.
func (s *soak) restore() {
	for _, svc := range soakServices {
		id := s.container(svc)
		_, _ = s.try("docker", "network", "connect", soakNetwork, id) // fails when connected: fine
		s.run("docker", "start", id)
	}
}

// check records an end check's outcome, failing the test when it failed.
func (s *soak) check(ok bool, format string, args ...any) {
	s.t.Helper()
	msg := fmt.Sprintf(format, args...)
	if ok {
		s.checks = append(s.checks, "pass: "+msg)
		return
	}
	s.checks = append(s.checks, "FAIL: "+msg)
	s.t.Error(msg)
}

// openRead opens the store read-only beside the engines.
func (s *soak) openRead() *postgres.Store {
	s.t.Helper()
	st, err := postgres.OpenReadOnly(s.t.Context(), s.dsn, postgres.Options{})
	if err != nil {
		s.t.Fatal(err)
	}
	return st
}

// checkStuck checks that every run started more than cfg.stuck before the chaos ended had settled by then.
func (s *soak) checkStuck(end time.Time) {
	st := s.openRead()
	defer func() { _ = st.Close() }()
	open, err := st.Runs(s.t.Context(), store.RunFilter{Unsettled: true})
	if err != nil {
		s.t.Fatal(err)
	}
	var stuck []string
	for _, r := range open {
		if r.StartedAt.Before(end.Add(-s.cfg.stuck)) {
			stuck = append(stuck, fmt.Sprintf("%s/%s (%s since %s)", r.Pipeline, r.ID, r.State,
				r.StartedAt.Format(time.TimeOnly)))
		}
	}
	s.check(len(stuck) == 0, "no run started more than %s before the end unsettled at the end (%d open, %d stuck%s)",
		s.cfg.stuck, len(open), len(stuck), listed(stuck))
}

func listed(xs []string) string {
	if len(xs) == 0 {
		return ""
	}
	return ": " + strings.Join(xs[:min(len(xs), 10)], ", ")
}

// quiesce waits, for up to cfg.stuck, until every run settled, then disables the triggered pipelines through the
// active engine. A disabled pipeline's deliveries wait (spec section 17), so it disables them only between two
// minutes' cron firings once their runs settled, and resumes them to try again when a firing came first.
func (s *soak) quiesce(end time.Time) bool {
	t := s.t
	active := s.waitActive()
	lifecycle := func(action string) {
		for _, p := range []string{pipelineName, m2Pipeline} {
			if code, body, err := s.request(active, http.MethodPost, "/api/v1/pipelines/"+p+"/"+action); err != nil ||
				code/100 != 2 {
				t.Fatalf("%s %s: %d %v %s", action, p, code, err, body)
			}
		}
	}
	st := s.openRead()
	defer func() { _ = st.Close() }()
	open := func() []store.Run {
		runs, err := st.Runs(t.Context(), store.RunFilter{Unsettled: true})
		if err != nil {
			t.Fatal(err)
		}
		return runs
	}
	deadline := time.Now().Add(s.cfg.stuck)
	for {
		if sec := time.Now().Second(); sec >= 15 && sec <= 50 && len(open()) == 0 {
			lifecycle("disable")
			if len(open()) == 0 {
				s.check(true, "every run settled, the triggers disabled")
				return true
			}
			lifecycle("resume")
		}
		if time.Now().After(deadline) {
			var stuck []string
			for _, r := range open() {
				if r.StartedAt.Before(end) {
					stuck = append(stuck, r.Pipeline+"/"+r.ID+" ("+string(r.State)+")")
				}
			}
			s.check(false, "every run started during the chaos settled within %s of its end: %d not%s", s.cfg.stuck,
				len(stuck), listed(stuck))
			s.diagnose(active)
			return false
		}
		time.Sleep(2 * time.Second)
	}
}

// diagnose writes build/soak/stuck.txt: the active engine's view of its nodes and the deliveries not terminal.
func (s *soak) diagnose(active string) {
	var b strings.Builder
	if _, body, err := s.request(active, http.MethodGet, "/api/v1/nodes"); err == nil {
		fmt.Fprintf(&b, "GET /api/v1/nodes on %s:\n%s\n\n", active, body)
	}
	conn, err := pgx.Connect(s.t.Context(), s.dsn)
	if err == nil {
		defer func() { _ = conn.Close(context.Background()) }()
		rows, err := conn.Query(s.t.Context(), `SELECT pipeline, id, run_id, step, state, attempt, lease_owner,
			fencing_token, COALESCE(lease_expires_at::text, ''), next_attempt_at::text FROM deliveries
			WHERE state NOT IN ('done', 'filtered', 'dead', 'cancelled') ORDER BY pipeline, step`)
		if err == nil {
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					break
				}
				fmt.Fprintln(&b, vals...)
			}
			rows.Close()
		}
	}
	_ = os.WriteFile(filepath.Join(s.out, "stuck.txt"), []byte(b.String()), 0o600)
}

// checkEndpoints checks that /metrics and the UI answer on the active engine, and the UI on the standby.
func (s *soak) checkEndpoints() {
	active, _, err := s.active()
	if err != nil {
		s.check(false, "an engine leads at the end: %v", err)
		return
	}
	code, metrics, err := s.request(active, http.MethodGet, "/metrics")
	s.check(err == nil && code == http.StatusOK && bytes.Contains(metrics, []byte("millstream_")),
		"/metrics answers on the active engine (%d, %d bytes)", code, len(metrics))
	for _, svc := range soakEngines {
		code, page, err := s.request(svc, http.MethodGet, "/")
		s.check(err == nil && code == http.StatusOK && bytes.Contains(page, []byte("<html")),
			"the UI answers on %s (%d %v)", svc, code, err)
	}
}

// checkInvariants reads the store with the crash suite's invariants (every run settled, every delivery terminal,
// no item published twice, no reservation left, spend consistent, dedupe keys sound), and checks that every import
// run settled after its producer run; it returns the snapshot and the number of runs of each pipeline.
func (s *soak) checkInvariants() (snapshot, map[string]int) {
	var snap snapshot
	ok := s.t.Run("invariants", func(t *testing.T) { snap = readSnapshot(t, s.dsn) })
	s.check(ok, "the store holds the crash suite's invariants (runs settled, deliveries terminal, no duplicate "+
		"items, no reservation left, spend sums agree)")
	st := s.openRead()
	defer func() { _ = st.Close() }()
	runs, err := st.Runs(s.t.Context(), store.RunFilter{})
	if err != nil {
		s.t.Fatal(err)
	}
	byID := map[string]store.Run{}
	perPipeline := map[string]int{}
	for _, r := range runs {
		byID[r.Pipeline+"/"+r.ID] = r
		perPipeline[r.Pipeline]++
	}
	var bad []string
	imports := 0
	for _, r := range runs {
		if r.Producer == "" {
			continue
		}
		imports++
		p, found := byID[r.Producer+"/"+r.TriggerKey]
		if !found || r.SettledAt.Before(p.SettledAt) {
			bad = append(bad, r.ID)
		}
	}
	s.check(len(bad) == 0 && imports > 0, "every import run (%d) settled after its producer run%s", imports,
		listed(bad))
	return snap, perPipeline
}

// writeReport writes build/soak/report.md.
func (s *soak) writeReport(start, end time.Time, snap snapshot, runs map[string]int) {
	var b bytes.Buffer
	p := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	p("# Soak report")
	p("")
	p("Generated by `make soak` (internal/e2e/soak_test.go). Not committed: build/ is git-ignored.")
	p("")
	p("- Started %s, chaos for %s (until %s), mean chaos interval %s, webhook every %s, seed %d.",
		start.Format(time.DateTime), end.Sub(start).Round(time.Second), end.Format(time.TimeOnly), s.cfg.chaos,
		s.cfg.webhook, s.cfg.seed)
	p("- Deployment: deploy/compose/compose.yaml as project %s, image %s: Postgres, two engines, two nodes.",
		soakProject, soakImage)
	p("")
	p("## Work")
	p("")
	states := map[store.RunState]int{}
	for _, state := range snap.Runs {
		states[state]++
	}
	dstates := map[store.DeliveryState]int{}
	for _, d := range snap.Deliveries {
		dstates[d.State]++
	}
	p("| What | Count |")
	p("| --- | --- |")
	p("| Runs | %d %v, by pipeline %v |", len(snap.Runs), states, runs)
	p("| Items | %d |", len(snap.Items))
	p("| Deliveries | %d %v |", len(snap.Deliveries), dstates)
	p("| Dead letters | %d |", len(snap.DeadLetters))
	p("| Webhook calls | %v |", s.hooks)
	p("")
	p("## Chaos")
	p("")
	s.mu.Lock()
	p("- Kills: %v; partitions: %d; failovers measured: %d.", s.kills, s.partitions, len(s.takeovers))
	p("- Takeover, active engine's end to the standby's lock: %s.", durationStats(s.takeovers,
		func(tk takeover) time.Duration { return tk.lead }))
	p("- Takeover, active engine's end to the first dispatch: %s.", durationStats(s.takeovers,
		func(tk takeover) time.Duration { return tk.dispatch }))
	p("")
	p("<details><summary>Events</summary>")
	p("")
	for _, e := range s.events {
		p("    %s", e)
	}
	p("")
	p("</details>")
	s.mu.Unlock()
	p("")
	p("## Checks")
	p("")
	for _, c := range s.checks {
		p("- %s", c)
	}
	path := filepath.Join(s.out, "report.md")
	if err := os.WriteFile(path, b.Bytes(), 0o600); err != nil {
		s.t.Fatal(err)
	}
	s.t.Logf("report: %s\n%s", path, b.String())
}
