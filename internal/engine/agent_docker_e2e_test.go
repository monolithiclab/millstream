//go:build e2e

package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	osexec "os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

// dockerCLI returns the local Docker CLI and the daemon's architecture, or skips the test when Docker is not
// available.
func dockerCLI(t *testing.T) (container.Runtime, string) {
	t.Helper()
	rt, ok := container.Find(pipeline.RuntimeDocker, os.LookupEnv)
	if !ok {
		t.Skip("docker is not installed: the real agent sandbox test is skipped")
	}
	out, err := osexec.CommandContext(t.Context(), rt.Bin, "version", "--format", "{{.Server.Arch}}").CombinedOutput()
	if err != nil {
		t.Skipf("docker is not reachable: the real agent sandbox test is skipped: %s", strings.TrimSpace(string(out)))
	}
	return rt, strings.TrimSpace(string(out))
}

// linuxMillstream builds millstream for Linux on arch, statically, into dir.
func linuxMillstream(t *testing.T, dir, arch string) string {
	t.Helper()
	_, file, _, _ := goruntime.Caller(0)
	bin := filepath.Join(dir, "millstream")
	cmd := osexec.CommandContext(t.Context(), "go", "build", "-o", bin, "./cmd/millstream")
	cmd.Dir = filepath.Join(filepath.Dir(file), "..", "..")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+arch)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build millstream for linux/%s: %v\n%s", arch, err, out)
	}
	return bin
}

// storeArtifacts binds a local artifact store to one pipeline for the runtime.
type storeArtifacts struct {
	s        *artifacts.Store
	pipeline string
}

func (a storeArtifacts) Open(_ context.Context, ref store.ArtifactRef) (io.ReadCloser, error) {
	return a.s.Open(a.pipeline, ref.SHA256)
}

func (a storeArtifacts) Put(_ context.Context, name string, r io.Reader) (store.ArtifactRef, error) {
	return a.s.Put(a.pipeline, name, r)
}

// TestRealAgentSandbox runs an agent delivery in a real container: the engine's assignment for a sandboxed agent
// step, an image FROM scratch, the Linux millstream mounted as the adapter, the package mounted read-only, and the
// harness replayed from a recording, so nothing reaches a model.
func TestRealAgentSandbox(t *testing.T) {
	t.Parallel()
	rt, arch := dockerCLI(t)
	dir := t.TempDir()
	bin := linuxMillstream(t, dir, arch)

	imgDir := filepath.Join(dir, "image")
	if err := os.MkdirAll(imgDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imgDir, "empty"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imgDir, "Dockerfile"), []byte("FROM scratch\nCOPY empty /empty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	image := "localhost/millstream-agent-test-" + hex.EncodeToString(suffix) + ":1"
	if out, err := osexec.CommandContext(t.Context(), rt.Bin, "build", "--quiet", "--tag", image, imgDir).CombinedOutput(); err != nil {
		t.Fatalf("docker build: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = osexec.Command(rt.Bin, "image", "rm", "--force", image).Run() }) // #nosec G204 -- test

	// The recording the adapter will replay: keyed by the prompt the payload renders and the output schema.
	recordings := filepath.Join(dir, "recordings")
	environ := map[string]string{"MILLSTREAM_SECRET_KEY": "sk-0123456789",
		agent.EnvHarnessMode: agent.ModeReplay, agent.EnvHarnessDir: recordings}
	// The container mounts the package: it is written to disk here, where the default build's packages live in memory.
	v := newEnv(t, Options{Env: func(k string) (string, bool) { s, ok := environ[k]; return s, ok },
		Agents: Agents{Sandbox: &AgentSandbox{Image: image, Binary: bin}}})
	onDisk := agentPackage(t, nil, "")
	onDisk.Dir = filepath.Join(dir, "pkg")
	for name, body := range onDisk.Files {
		path := filepath.Join(onDisk.Dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	v.apply(onDisk)
	v.fire("p", "adhoc", "k")
	as, err := v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("Pull = %v, %v", as, err)
	}
	a := as[0]
	pkg, err := pipeline.Load(a.Spec.Mounts[1].Source)
	if err != nil {
		t.Fatal(err)
	}
	act, err := pipeline.Resolver{}.Resolve(pkg, "./actions/score")
	if err != nil {
		t.Fatal(err)
	}
	req := agent.Request{Model: act.Agent.Model, Prompt: "Score map[n:1].\n", Schema: act.Output}
	rec := agent.Recorder{Name: pipeline.HarnessAPI, Mode: agent.ModeRecord, Dir: recordings,
		Harness: harnessFunc(func(agent.Request) (agent.Response, error) {
			return agent.Response{Text: "```json\n{\"score\": 7}\n```", TokensIn: 10, TokensOut: 3}, nil
		})}
	if _, err := rec.Run(t.Context(), req); err != nil {
		t.Fatal(err)
	}

	opts := exec.Options{Launchers: map[string]exec.Launcher{pipeline.KindContainer: container.Launcher{Runtime: rt}},
		ReadyTimeout: time.Minute, ShutdownTimeout: 10 * time.Second}
	p, err := exec.Start(t.Context(), a.Spec, opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	arts := storeArtifacts{s: artifacts.New(filepath.Join(dir, "artifacts")), pipeline: "p"}
	item := a.ItemMsg
	item.Payload = json.RawMessage(`{"n": 1}`)
	res, err := p.Deliver(t.Context(), exec.Delivery{Item: item, Artifacts: arts, Timeout: time.Minute})
	if err != nil || res.Err != nil {
		t.Fatalf("Deliver: %+v, %v (logs %s)", res.Err, err, v.logs.String())
	}
	if len(res.Outputs) != 1 || string(res.Outputs[0].Payload) != `{"score":7}` || len(res.Artifacts) != 1 ||
		res.Artifacts[0].Name != agent.TranscriptFile || len(res.Metrics) != 5 {
		t.Fatalf("result %+v", res)
	}
	f, err := arts.Open(t.Context(), res.Artifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	r, err := artifacts.Decompressed(res.Artifacts[0], f)
	if err != nil {
		t.Fatal(err)
	}
	var tr agent.Transcript
	if err := json.NewDecoder(r).Decode(&tr); err != nil || tr.Prompt != req.Prompt || tr.TokensIn != 10 {
		t.Fatalf("transcript %+v, %v", tr, err)
	}
}

// harnessFunc is a function as an agent.Harness.
type harnessFunc func(agent.Request) (agent.Response, error)

func (f harnessFunc) Run(_ context.Context, req agent.Request) (agent.Response, error) { return f(req) }
