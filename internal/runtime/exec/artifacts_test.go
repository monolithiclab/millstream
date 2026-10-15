package exec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"strings"
	"sync"
	"testing"

	"github.com/monolithiclab/millstream/internal/protocol"
	"github.com/monolithiclab/millstream/internal/store"
)

// memArtifacts is an in-memory artifact store.
type memArtifacts struct {
	mu      sync.Mutex
	blobs   map[string][]byte
	putErr  error
	openErr error
}

func newMemArtifacts(contents ...string) (*memArtifacts, []store.ArtifactRef) {
	m := &memArtifacts{blobs: map[string][]byte{}}
	var refs []store.ArtifactRef
	for i, c := range contents {
		ref, _ := m.Put(context.Background(), fmt.Sprintf("in-%d.txt", i), strings.NewReader(c))
		refs = append(refs, ref)
	}
	return m, refs
}

func (m *memArtifacts) Put(_ context.Context, name string, r io.Reader) (store.ArtifactRef, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return store.ArtifactRef{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.putErr != nil {
		return store.ArtifactRef{}, m.putErr
	}
	sum := sha256.Sum256(data)
	ref := store.ArtifactRef{Name: name, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data)), Type: "text/plain"}
	m.blobs[ref.SHA256] = data
	return ref, nil
}

func (m *memArtifacts) Open(_ context.Context, ref store.ArtifactRef) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.openErr != nil {
		return nil, m.openErr
	}
	data, ok := m.blobs[ref.SHA256]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memArtifacts) content(sha string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return string(m.blobs[sha])
}

// artifactDelivery is a delivery of the fake action in artifact mode.
func artifactDelivery(t *testing.T, id string, payload map[string]any, a Artifacts, inputs ...store.ArtifactRef) Delivery {
	t.Helper()
	payload = maps.Clone(payload)
	payload["mode"] = "artifact"
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	d := delivery(id, string(body))
	d.Inputs, d.Artifacts = inputs, a
	return d
}

func TestDeliverMovesArtifacts(t *testing.T) {
	t.Parallel()
	p := startFake(t, "echo", nil)
	mem, inputs := newMemArtifacts("first input", "second input")
	inputs[1].Name = "page.html"
	d := artifactDelivery(t, "dl_1", map[string]any{
		"files": map[string]string{"out/report.txt": "the report", "chart.svg": "<svg/>"},
		"emit":  []protocol.Artifact{{Name: "report.txt", Path: "out/report.txt"}, {Name: "chart.svg", Path: "./chart.svg"}},
	}, mem, inputs...)
	res, err := p.Deliver(t.Context(), d)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal(res.Outputs[0].Payload, &got); err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"in-0.txt": "in/in-0.txt=first input", "page.html": "in/page.html=second input"}; !maps.Equal(got, want) {
		t.Fatalf("the action read %v, want %v", got, want)
	}
	arts := res.Outputs[0].Artifacts
	if len(arts) != 2 || arts[0].Name != "report.txt" || mem.content(arts[0].SHA256) != "the report" ||
		arts[1].Name != "chart.svg" || mem.content(arts[1].SHA256) != "<svg/>" || arts[0].Size != 10 {
		t.Fatalf("artifacts = %+v", arts)
	}
	// The next delivery starts from an empty working directory, without the previous one's files.
	res, err = p.Deliver(t.Context(), artifactDelivery(t, "dl_2", map[string]any{
		"emit": []protocol.Artifact{{Name: "report.txt", Path: "out/report.txt"}},
	}, mem))
	requireClass(t, err, ClassProtocol)
	if !Permanent(err) || !p.Alive() || res.Outputs != nil {
		t.Fatalf("a missing file: %v, alive %v", err, p.Alive())
	}
}

// TestDeliverAttemptArtifacts: the artifacts named by done or error are the attempt's, kept apart from the outputs;
// after an error, one that cannot be moved is dropped and the action's error reported.
func TestDeliverAttemptArtifacts(t *testing.T) {
	t.Parallel()
	p := startFake(t, "echo", nil)
	mem, _ := newMemArtifacts()
	files := map[string]string{"transcript.json": "{}", "out.txt": "out"}
	res, err := p.Deliver(t.Context(), artifactDelivery(t, "dl_1", map[string]any{
		"files": files, "emit": []protocol.Artifact{{Name: "out.txt", Path: "out.txt"}},
		"attempt": []protocol.Artifact{{Name: "transcript.json", Path: "transcript.json"}},
	}, mem))
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(res.Outputs[0].Artifacts) != 1 || res.Outputs[0].Artifacts[0].Name != "out.txt" ||
		len(res.Artifacts) != 1 || res.Artifacts[0].Name != "transcript.json" || mem.content(res.Artifacts[0].SHA256) != "{}" {
		t.Fatalf("outputs %+v, attempt %+v", res.Outputs, res.Artifacts)
	}

	res, err = p.Deliver(t.Context(), artifactDelivery(t, "dl_2", map[string]any{
		"files": files, "fail": true, "attempt": []protocol.Artifact{{Name: "transcript.json", Path: "transcript.json"}},
	}, mem))
	if err != nil || res.Err == nil || res.Outputs != nil || len(res.Artifacts) != 1 {
		t.Fatalf("error answer: %+v, %v", res, err)
	}

	// A refused attempt artifact on done is a permanent protocol error; on error it is dropped.
	_, err = p.Deliver(t.Context(), artifactDelivery(t, "dl_3", map[string]any{
		"attempt": []protocol.Artifact{{Name: "transcript.json", Path: "../x"}},
	}, mem))
	requireClass(t, err, ClassProtocol)
	res, err = p.Deliver(t.Context(), artifactDelivery(t, "dl_4", map[string]any{
		"fail": true, "attempt": []protocol.Artifact{{Name: "transcript.json", Path: "../x"}},
	}, mem))
	if err != nil || res.Err == nil || res.Err.Message != "failed" || res.Artifacts != nil {
		t.Fatalf("refused artifact after error: %+v, %v", res, err)
	}
}

// TestDeliverRefusesArtifactPaths: a path outside the working directory, through a symbolic link or to something
// other than a regular file is a permanent protocol error; the process stays healthy.
func TestDeliverRefusesArtifactPaths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		payload map[string]any
	}{
		{"traversal", map[string]any{"emit": []protocol.Artifact{{Name: "x", Path: "../../etc/passwd"}}}},
		{"absolute", map[string]any{"emit": []protocol.Artifact{{Name: "x", Path: "/etc/passwd"}}}},
		{"empty path", map[string]any{"emit": []protocol.Artifact{{Name: "x", Path: ""}}}},
		{"symlink out", map[string]any{
			"links": map[string]string{"evil": "/etc/passwd"},
			"emit":  []protocol.Artifact{{Name: "x", Path: "evil"}},
		}},
		{"symlink inside", map[string]any{
			"files": map[string]string{"real.txt": "x"}, "links": map[string]string{"alias": "real.txt"},
			"emit": []protocol.Artifact{{Name: "x", Path: "alias"}},
		}},
		{"symlinked directory", map[string]any{
			"files": map[string]string{"d/real.txt": "x"}, "links": map[string]string{"dl": "d"},
			"emit": []protocol.Artifact{{Name: "x", Path: "dl/real.txt"}},
		}},
		{"directory", map[string]any{
			"files": map[string]string{"d/real.txt": "x"},
			"emit":  []protocol.Artifact{{Name: "x", Path: "d"}},
		}},
		{"path as name", map[string]any{
			"files": map[string]string{"a.txt": "x"},
			"emit":  []protocol.Artifact{{Name: "out/a.txt", Path: "a.txt"}},
		}},
		{"duplicate name", map[string]any{
			"files": map[string]string{"a.txt": "x", "b.txt": "y"},
			"emit":  []protocol.Artifact{{Name: "a", Path: "a.txt"}, {Name: "a", Path: "b.txt"}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := startFake(t, "echo", nil)
			mem, _ := newMemArtifacts()
			_, err := p.Deliver(t.Context(), artifactDelivery(t, "dl_1", tt.payload, mem))
			requireClass(t, err, ClassProtocol)
			if !Permanent(err) || !errors.Is(err, protocol.ErrProtocol) || !p.Alive() {
				t.Fatalf("err %v (permanent %v), alive %v", err, Permanent(err), p.Alive())
			}
			if len(mem.blobs) != 0 {
				t.Fatalf("stored %d artifacts", len(mem.blobs))
			}
		})
	}
}

// TestDeliverArtifactStoreFailures: a store that fails, or none at all, fails the delivery as a retryable artifact
// error and keeps the process.
func TestDeliverArtifactStoreFailures(t *testing.T) {
	t.Parallel()
	emit := map[string]any{
		"files": map[string]string{"a.txt": "x"}, "emit": []protocol.Artifact{{Name: "a.txt", Path: "a.txt"}},
	}
	boom := errors.New("disk full")
	tests := []struct {
		name   string
		build  func() (Artifacts, []store.ArtifactRef)
		output bool // the delivery emits an artifact (else it has an input)
		want   error
	}{
		{"put fails", func() (Artifacts, []store.ArtifactRef) {
			m, _ := newMemArtifacts()
			m.putErr = boom
			return m, nil
		}, true, boom},
		{"open fails", func() (Artifacts, []store.ArtifactRef) {
			m, refs := newMemArtifacts("x")
			m.openErr = boom
			return m, refs
		}, false, boom},
		{"no store for outputs", func() (Artifacts, []store.ArtifactRef) { return nil, nil }, true, errNoArtifacts},
		{"no store for inputs", func() (Artifacts, []store.ArtifactRef) {
			_, refs := newMemArtifacts("x")
			return nil, refs
		}, false, errNoArtifacts},
		{"duplicate input names", func() (Artifacts, []store.ArtifactRef) {
			m, refs := newMemArtifacts("x", "y")
			refs[1].Name = refs[0].Name
			return m, refs
		}, false, fs.ErrExist},
		{"input name with a path", func() (Artifacts, []store.ArtifactRef) {
			m, refs := newMemArtifacts("x")
			refs[0].Name = "../x"
			return m, refs
		}, false, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := startFake(t, "echo", nil)
			a, inputs := tt.build()
			payload := map[string]any{}
			if tt.output {
				payload = emit
			}
			_, err := p.Deliver(t.Context(), artifactDelivery(t, "dl_1", payload, a, inputs...))
			requireClass(t, err, ClassArtifact)
			if Permanent(err) || !p.Alive() || (tt.want != nil && !errors.Is(err, tt.want)) {
				t.Fatalf("err %v (permanent %v), alive %v", err, Permanent(err), p.Alive())
			}
		})
	}
}

// TestPoolKeepsProcessAfterArtifactError: the pool hands the same process the next delivery.
func TestPoolKeepsProcessAfterArtifactError(t *testing.T) {
	t.Parallel()
	pool := NewPool(PoolOptions{Options: testOpts})
	t.Cleanup(func() { _ = pool.Close() })
	spec := fakeSpec("echo", nil)
	_, err := pool.Deliver(t.Context(), spec, artifactDelivery(t, "dl_1", map[string]any{
		"emit": []protocol.Artifact{{Name: "x", Path: "../x"}},
	}, nil))
	requireClass(t, err, ClassArtifact)
	first := decodeEcho(t, mustDeliver(t, pool, spec, delivery("dl_2", `{}`)))
	second := decodeEcho(t, mustDeliver(t, pool, spec, delivery("dl_3", `{}`)))
	if first.PID != second.PID {
		t.Fatalf("pids %d and %d: the process was replaced", first.PID, second.PID)
	}
}

func mustDeliver(t *testing.T, pool *Pool, spec Spec, d Delivery) Result {
	t.Helper()
	res, err := pool.Deliver(t.Context(), spec, d)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	return res
}
