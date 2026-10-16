package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

const retentionSrc = `pipeline: src
triggers:
  go: { manual: {} }
streams:
  listings: { retention: 1d }
steps:
  crawl: { action: ./actions/a, in: trigger.go, out: listings }
`

// memFiles is an artifact store in memory as the sweep sees it: each file's modification time by pipeline and name.
type memFiles struct {
	mu sync.Mutex
	m  map[artifacts.File]bool
}

func (f *memFiles) put(pipelineName, name string, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.m {
		if k.Pipeline == pipelineName && k.Name == name {
			delete(f.m, k)
		}
	}
	f.m[artifacts.File{Pipeline: pipelineName, Name: name, ModTime: at}] = true
}

func (f *memFiles) List(context.Context) ([]artifacts.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []artifacts.File
	for k := range f.m {
		out = append(out, k)
	}
	return out, nil
}

func (f *memFiles) ModTime(_ context.Context, pipelineName, sha string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.m {
		if k.Pipeline == pipelineName && k.Name == sha {
			return k.ModTime, nil
		}
	}
	return time.Time{}, fmt.Errorf("%s of %s: %w", sha, pipelineName, fs.ErrNotExist)
}

func (f *memFiles) Copy(ctx context.Context, from, to, sha string) error {
	t, err := f.ModTime(ctx, from, sha)
	if err == nil {
		f.put(to, sha, t)
	}
	return err
}

func (f *memFiles) Remove(_ context.Context, pipelineName, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.m {
		if k.Pipeline == pipelineName && k.Name == name {
			delete(f.m, k)
		}
	}
	return nil
}

func (f *memFiles) has(pipelineName, name string) bool {
	_, err := f.ModTime(context.Background(), pipelineName, name)
	return err == nil
}

// TestRetainAndPurge covers the engine's retention job and purges (spec section 9.3): a stream's retention applied
// from the active revision, the artifact sweep (an expired item's file deleted after the grace, but first copied
// into the prefix of an importer that references it without holding it; files no entry names deleted once old; a
// file stored again within the grace kept), the package directory and the caches pruned of revisions nothing uses,
// and a purge run to its end, its files swept at once, and resumed when interrupted.
func TestRetainAndPurge(t *testing.T) {
	sha := func(c byte) string { return strings.Repeat(string(c), 64) }
	photoA := store.ArtifactRef{Name: "a.jpg", SHA256: sha('a'), Size: 1, Type: "image/jpeg"}
	photoB := store.ArtifactRef{Name: "b.jpg", SHA256: sha('b'), Size: 1, Type: "image/jpeg"}
	files := &memFiles{m: map[artifacts.File]bool{}}
	pkgs := t.TempDir()
	v := newEnv(t, Options{ArtifactFiles: files, PackageDir: pkgs, Env: func(k string) (string, bool) {
		return "x", k == "MILLSTREAM_VAR_TAG"
	}, Stubs: map[store.StepRef]Stub{
		{Pipeline: "src", Step: "crawl"}: {Outputs: []store.Output{
			{Key: "a", Payload: json.RawMessage(`{"n":1}`), Artifacts: []store.ArtifactRef{photoA}},
			{Key: "b", Payload: json.RawMessage(`{"n":2}`), Artifacts: []store.ArtifactRef{photoB}},
		}},
		{Pipeline: "deal", Step: "notify"}: {},
	}})
	src := newPackage(t, retentionSrc, "a")
	v.apply(src)
	v.apply(newPackage(t, importDeal, "notify"))
	v.fire("src", "go", "r1")
	v.pull(10)
	v.pull(10)
	v.clock.Advance(25 * time.Hour)
	now := v.clock.Now()
	// photo b was stored again an hour ago; an old orphan and temporary file, a fresh orphan; package directories.
	files.put("src", photoA.SHA256, t0)
	files.put("src", photoB.SHA256, now.Add(-time.Hour))
	files.put("src", sha('o'), t0)
	files.put("src", ".put-1", t0)
	files.put("src", sha('f'), now.Add(-time.Hour))
	for _, d := range []string{src.Hash, sha('9'), ".tmp-1"} {
		if err := os.Mkdir(filepath.Join(pkgs, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(filepath.Join(pkgs, ".tmp-1"), now, now); err != nil {
		t.Fatal(err)
	}
	v.e.mu.Lock()
	v.e.revs[99] = &revision{id: 99, hash: "gone"}
	v.e.actions[actionKey{"gone", "./actions/a"}] = &resolvedAction{}
	v.e.mu.Unlock()

	if err := v.e.Retain(v.ctx); err != nil {
		t.Fatal(err)
	}
	items, err := v.st.Items(v.ctx, store.ItemFilter{Pipeline: "src", Stream: "listings"})
	if err != nil || len(items) != 2 || items[0].Tombstone != store.TombstoneExpired {
		t.Fatalf("listings after retention = %+v, %v", items, err)
	}
	for _, c := range []struct {
		pipeline, name string
		want           bool
	}{
		{"src", photoA.SHA256, false}, {"deal", photoA.SHA256, true}, {"src", photoB.SHA256, true},
		{"src", sha('o'), false}, {"src", ".put-1", false}, {"src", sha('f'), true},
	} {
		if files.has(c.pipeline, c.name) != c.want {
			t.Errorf("file %s of %s kept %v, want %v", c.name[:8], c.pipeline, !c.want, c.want)
		}
	}
	for name, want := range map[string]bool{src.Hash: true, sha('9'): false, ".tmp-1": true} {
		if _, err := os.Stat(filepath.Join(pkgs, name)); (err == nil) != want {
			t.Errorf("package directory %s kept %v, want %v", name, err == nil, want)
		}
	}
	v.e.mu.Lock()
	_, rev := v.e.revs[99]
	_, act := v.e.actions[actionKey{"gone", "./actions/a"}]
	v.e.mu.Unlock()
	if rev || act {
		t.Fatalf("caches kept a revision nothing uses: revision %v, action %v", rev, act)
	}

	// A purge of b runs to its end: both copies erased, its file deleted without waiting for the grace.
	p, created, err := v.e.Purge(v.ctx, PurgeRequest{Key: "b"})
	if err != nil || !created || p.State != store.PurgeDone || p.Items != 2 || !strings.HasPrefix(p.ID, "purge_") {
		t.Fatalf("Purge = %+v, %v, %v", p, created, err)
	}
	if files.has("src", photoB.SHA256) {
		t.Fatal("the purged item's file is still there")
	}
	again, created, err := v.e.Purge(v.ctx, PurgeRequest{RequestID: strings.TrimPrefix(p.ID, "purge_"), Key: "b"})
	if err != nil || created || again.State != store.PurgeDone {
		t.Fatalf("Purge again = %+v, %v, %v", again, created, err)
	}
	// One a crash left fenced is finished by the next pass.
	if _, _, err := v.st.StartPurge(v.ctx, store.PurgeRequest{ID: "p_crashed",
		Item: ids.ImportItem("approved", items[0].ID), Pipeline: "deal"}); err != nil {
		t.Fatal(err)
	}
	if err := v.e.Retain(v.ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := v.st.Purge(v.ctx, "p_crashed"); err != nil || got.State != store.PurgeDone {
		t.Fatalf("interrupted purge after a pass = %+v, %v", got, err)
	}
	if _, _, err := v.e.Purge(context.Background(), PurgeRequest{}); !errors.Is(err, store.ErrInvalid) {
		t.Fatalf("Purge naming nothing = %v", err)
	}
	// A store that fails fails every step, each reported.
	if err := v.st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.e.Retain(v.ctx); err == nil || !strings.Contains(err.Error(), "resume purges") ||
		!strings.Contains(err.Error(), "artifact sweep") || !strings.Contains(err.Error(), "prune") {
		t.Fatalf("Retain on a closed store = %v", err)
	}
}
