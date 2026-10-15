package node

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/runtime/exec"
	"github.com/monolithiclab/millstream/internal/store"
)

// artifactAssignment runs the fake action in artifact mode on an item carrying one input artifact, emitting one.
func artifactAssignment(t *testing.T, tr *fakeTransport, id string) Assignment {
	t.Helper()
	input, err := tr.PutArtifact(t.Context(), "listings", "page.html", strings.NewReader("listing 42"))
	if err != nil {
		t.Fatal(err)
	}
	a := assignment(id, "artifact", nil)
	a.Lease.Item.Artifacts = []store.ArtifactRef{input}
	a.ItemMsg.Payload = json.RawMessage(`{"files": {"out/r.txt": "report"}, "emit": [{"name": "r.txt", "path": "out/r.txt"}]}`)
	return a
}

// TestArtifactsGoThroughTheTransport: the input artifact comes from the transport's store into the working
// directory, the output one goes to it and its reference travels in the Done report.
func TestArtifactsGoThroughTheTransport(t *testing.T) {
	t.Parallel()
	tr := newFakeTransport()
	tr.queue = []Assignment{artifactAssignment(t, tr, "dl_1")}
	stop := startNode(t, tr, Options{})
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.dones) != 1 {
		t.Fatalf("dones %+v fails %+v", tr.dones, tr.fails)
	}
	o := tr.dones[0].Outputs[0]
	if string(o.Payload) != `{"page.html":"in/page.html=listing 42"}` {
		t.Fatalf("the action read %s", o.Payload)
	}
	if len(o.Artifacts) != 1 || o.Artifacts[0].Name != "r.txt" || string(tr.blobs["listings/"+o.Artifacts[0].SHA256]) != "report" {
		t.Fatalf("output artifacts %+v", o.Artifacts)
	}
}

// TestArtifactStoreFailureIsRetryable: a store that refuses the upload fails the delivery as a retryable artifact
// error.
func TestArtifactStoreFailureIsRetryable(t *testing.T) {
	t.Parallel()
	tr := newFakeTransport()
	tr.queue = []Assignment{artifactAssignment(t, tr, "dl_1")}
	tr.putErr = errors.New("bucket unreachable")
	stop := startNode(t, tr, Options{})
	await(t, tr.reported, 1)
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.fails) != 1 || tr.fails[0].Class != exec.ClassArtifact || !tr.fails[0].Retryable ||
		!strings.Contains(tr.fails[0].Message, "bucket unreachable") {
		t.Fatalf("fails %+v", tr.fails)
	}
}
