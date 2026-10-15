//go:build e2e

package container

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/runtime/exec/exectest"
	"github.com/monolithiclab/millstream/internal/store"
)

// imageManifest is the manifest the test image carries in its label.
const imageManifest = `{"name": "test/fake", "version": "1.0.0", "kind": "container", "input": {"type": "object"}, ` +
	`"runtime": {"container": {"command": ["/fakeaction"]}}}`

// dockerRuntime returns the local Docker CLI, or skips the test when Docker is not available.
func dockerRuntime(t *testing.T) Runtime {
	t.Helper()
	rt, ok := Find(pipeline.RuntimeDocker, os.LookupEnv)
	if !ok {
		t.Skip("docker is not installed: the real container tests are skipped")
	}
	if out, err := rt.command(t.Context(), "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		t.Skipf("docker info failed, the daemon is not reachable: the real container tests are skipped: %s",
			strings.TrimSpace(string(out)))
	}
	return rt
}

// buildImage builds an image FROM scratch holding the fake action, statically built for the daemon's platform and
// labelled with imageManifest, as <repo>:1.0.0, and removes it when the test ends.
func buildImage(t *testing.T, rt Runtime) (repo string) {
	t.Helper()
	arch, err := rt.output("version", "--format", "{{.Server.Arch}}")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bin, err := exectest.BuildFor(dir, "linux", strings.TrimSpace(string(arch)))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(bin, filepath.Join(dir, "fakeaction")); err != nil {
		t.Fatal(err)
	}
	dockerfile := fmt.Sprintf("FROM scratch\nCOPY fakeaction /fakeaction\nLABEL %s='%s'\n", pipeline.ManifestLabel,
		imageManifest)
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o600); err != nil {
		t.Fatal(err)
	}
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	repo = "localhost/millstream-test-" + hex.EncodeToString(suffix) + "/fake"
	if _, err := rt.output("build", "--quiet", "--tag", repo+":1.0.0", dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = rt.output("image", "rm", "--force", repo+":1.0.0") })
	return repo
}

// TestRealContainer resolves a registry action from a locally built image and runs deliveries through it under
// Docker: the protocol, the mounted working directory with its artifacts, and the removal of a killed container.
func TestRealContainer(t *testing.T) {
	t.Parallel()
	rt := dockerRuntime(t)
	repo := buildImage(t, rt)
	images := Images{Runtime: rt}

	tags, err := images.Tags(repo)
	if err != nil || len(tags) != 1 || tags[0] != "1.0.0" {
		t.Fatalf("Tags = %v, %v", tags, err)
	}
	r := pipeline.Resolver{Images: images}
	r.Registry = pipeline.Registry{Prefixes: map[string]string{"test": strings.TrimSuffix(repo, "/fake")}, Resolver: r}
	pkg := &pipeline.Package{Files: map[string][]byte{}}
	a, err := r.Resolve(pkg, "test/fake@1")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if a.Kind != pipeline.KindContainer || a.Version != "1.0.0" || a.Image != repo+":1.0.0" ||
		!strings.HasPrefix(a.Pin, "sha256:") || a.ImageDigest != a.Pin || !a.Unpinned || a.Input == nil {
		t.Fatalf("resolved %+v", a)
	}

	opts := exec.Options{
		Launchers:    map[string]exec.Launcher{pipeline.KindContainer: Launcher{Runtime: rt}},
		ReadyTimeout: 30 * time.Second, ShutdownTimeout: 10 * time.Second,
	}
	spec := exec.Spec{Action: a.Name, Version: a.Version, Kind: a.Kind, Digest: a.ImageDigest, Image: a.Pin,
		Command: a.Command, Config: map[string]any{"mode": "echo"}}
	p, err := exec.Start(t.Context(), spec, opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	res, err := p.Deliver(t.Context(), exec.Delivery{Item: item("dl_1", `{}`), Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	var echo struct {
		Workdir string `json:"workdir"`
	}
	if err := json.Unmarshal(res.Outputs[0].Payload, &echo); err != nil || echo.Workdir == "" ||
		!strings.Contains(echo.Workdir, "millstream-work-") {
		t.Fatalf("echo %s (%v)", res.Outputs[0].Payload, err)
	}

	// The container writes its artifact in the mounted working directory, as the node's user: the node reads it.
	arts := &memArtifacts{}
	res, err = p.Deliver(t.Context(), exec.Delivery{
		Item:      item("dl_2", `{"mode": "artifact", "files": {"out/r.txt": "from the container"}, "emit": [{"name": "r.txt", "path": "out/r.txt"}]}`),
		Artifacts: arts, Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Deliver artifact: %v", err)
	}
	if got := res.Outputs[0].Artifacts; len(got) != 1 || arts.data[got[0].SHA256] != "from the container" {
		t.Fatalf("artifacts %+v", got)
	}

	// A delivery that times out kills the client and removes its container.
	_, err = p.Deliver(t.Context(), exec.Delivery{Item: item("dl_3", `{"mode": "hang"}`), Timeout: time.Second})
	if exec.ClassOf(err) != exec.ClassTimeout || p.Alive() {
		t.Fatalf("hang: %v, alive %v", err, p.Alive())
	}
	name := Name(echo.Workdir)
	out, err := rt.output("ps", "--all", "--quiet", "--filter", "name=^"+name+"$")
	if err != nil || strings.TrimSpace(string(out)) != "" {
		t.Fatalf("container %s left after the kill: %q (%v)", name, out, err)
	}
}

func item(id, payload string) protocol.Item {
	return protocol.Item{Delivery: id, ID: "it_" + id, Payload: json.RawMessage(payload), Attempt: 1,
		Artifacts: []protocol.Artifact{}}
}

// memArtifacts keeps artifacts in memory.
type memArtifacts struct{ data map[string]string }

func (m *memArtifacts) Put(_ context.Context, name string, r io.Reader) (store.ArtifactRef, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return store.ArtifactRef{}, err
	}
	if m.data == nil {
		m.data = map[string]string{}
	}
	sha := fmt.Sprintf("%064x", len(m.data)+1)
	m.data[sha] = string(b)
	return store.ArtifactRef{Name: name, SHA256: sha, Size: int64(len(b)), Type: "text/plain"}, nil
}

func (m *memArtifacts) Open(context.Context, store.ArtifactRef) (io.ReadCloser, error) {
	return nil, errors.New("no input artifacts in this test")
}
