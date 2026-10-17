package engine

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"

	"github.com/monolithiclab/millstream/internal/testdir"
)

const (
	importSrc = `pipeline: src
triggers:
  go: { manual: {} }
steps:
  crawl: { action: ./actions/crawl, in: trigger.go, out: listings }
`
	importDeal = `pipeline: deal
vars: { tag: null }
streams:
  approved: { pipeline: src, stream: listings }
steps:
  notify: { action: ./actions/notify, in: approved }
`
)

// TestImports covers the engine's part of shared streams (spec section 4.12): the variables of import runs resolved
// at apply, the back-fill replay over the producer's retained items, and the artifacts of a copy opened from the
// consumer.
func TestImports(t *testing.T) {
	env := map[string]string{}
	v := newEnv(t, Options{Artifacts: artifacts.New(testdir.New(t)), Env: func(k string) (string, bool) {
		val, ok := env[k]
		return val, ok
	}})
	v.apply(newPackage(t, importSrc, "crawl"))
	deal := newPackage(t, importDeal, "notify")
	if _, err := v.e.Apply(v.ctx, deal); !errors.Is(err, ErrFiringConfig) || !strings.Contains(err.Error(), "tag") {
		t.Fatalf("Apply with a required variable unset: %v", err)
	}
	env["MILLSTREAM_VAR_TAG"] = "hot"
	v.apply(deal)

	ref, err := v.e.PutArtifact(v.ctx, store.LeaseRef{Pipeline: "src"}, "page.html", strings.NewReader("<p>listing</p>"))
	if err != nil {
		t.Fatal(err)
	}
	v.fire("src", "go", "r1")
	ls, err := v.st.Lease(v.ctx, store.LeaseRequest{Owner: "n", Max: 1, TTL: time.Minute})
	if err != nil || len(ls) != 1 {
		t.Fatalf("Lease = %v, %v", ls, err)
	}
	if _, err := v.st.Done(v.ctx, store.DoneRequest{LeaseRef: ls[0].Ref(), Attempt: 1, Outputs: []store.Output{
		{Key: "a", Payload: json.RawMessage(`{"n":1}`), Artifacts: []store.ArtifactRef{ref}},
		{Key: "b", Payload: json.RawMessage(`{"n":2}`)},
	}}); err != nil {
		t.Fatal(err)
	}
	run := v.run("deal", ids.ImportRun("approved", "run_r1_go"))
	if run.Vars["tag"] != "hot" || run.VarSources["tag"] != "env" {
		t.Fatalf("import run vars = %v from %v", run.Vars, run.VarSources)
	}

	// The copy's artifact is the producer's file, copied into the consumer's directory on first open, from the
	// pipeline the copy came from only.
	r, err := OpenArtifact(v.ctx, v.e.opts.Artifacts, "deal", "src", ref)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	_ = r.Close()
	if string(b) != "<p>listing</p>" {
		t.Fatalf("artifact content %q", b)
	}
	if _, err := v.e.OpenArtifact(v.ctx, store.LeaseRef{Pipeline: "other"}, ref); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("an artifact of another pipeline: %v", err)
	}
	if _, err := OpenArtifact(v.ctx, v.e.opts.Artifacts, "other", "", ref); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("an artifact of another pipeline, no copy: %v", err)
	}

	// The back-fill selects the producer's items on the stream, leaving the expired out.
	items, err := v.st.Items(v.ctx, store.ItemFilter{Pipeline: "src", Stream: "listings"})
	if err != nil || len(items) != 2 {
		t.Fatalf("listings = %v, %v", items, err)
	}
	if err := v.st.Exec(v.ctx, `UPDATE items SET payload = 'null', payload_hash = '', artifacts = '[]',
		tombstone = 'expired' WHERE pipeline = 'src' AND id = ?`, items[1].ID); err != nil {
		t.Fatal(err)
	}
	id, created, err := v.e.Replay(v.ctx, ReplayRequest{Pipeline: "deal", Stream: "approved", RequestID: "bf",
		Where: "item.n == 1"})
	if err != nil || !created || id != ids.ReplayRun("bf") {
		t.Fatalf("Replay --stream = %s, %v, %v", id, created, err)
	}
	ds := v.deliveries(id)
	if len(ds) != 1 || ds[0].ItemID != ids.ImportItem("approved", items[0].ID) || ds[0].Step != "notify" {
		t.Fatalf("back-fill deliveries = %+v", ds)
	}
	for _, req := range []ReplayRequest{
		{Pipeline: "deal", Stream: "approved", Step: "notify"},
		{Pipeline: "deal", Stream: "approved", RunID: "r1"},
		{Pipeline: "deal", Stream: "notify"},
		{Pipeline: "deal"},
	} {
		if _, _, err := v.e.Replay(v.ctx, req); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("Replay %+v: %v, want invalid", req, err)
		}
	}
}
