package publish

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// baseSrc is the base revision's pipeline file; diff changes its filter (line 4), a config proposal.
const (
	baseSrc = "pipeline: extra\ntriggers: { adhoc: { manual: {} } }\nsteps:\n" +
		"  pass: { action: filter, in: trigger.adhoc, config: { expr: \"true\" } }\n"
	diff = "--- a/pipeline.yaml\n+++ b/pipeline.yaml\n@@ -4 +4 @@\n" +
		"-  pass: { action: filter, in: trigger.adhoc, config: { expr: \"true\" } }\n" +
		"+  pass: { action: filter, in: trigger.adhoc, config: { expr: \"false\" } }\n"
)

// fixture returns the base package, a source of it, and an accepted config proposal of diff on it.
func fixture(t *testing.T) (*pipeline.Package, Packages, Accepted) {
	t.Helper()
	base, err := pipeline.FromFiles("", map[string][]byte{"pipeline.yaml": []byte(baseSrc)})
	if err != nil {
		t.Fatal(err)
	}
	packages := func(_ context.Context, hash string) (*pipeline.Package, error) {
		if hash != base.Hash {
			return nil, store.ErrNotFound
		}
		return base, nil
	}
	return base, packages, Accepted{ProposalID: "pr_1", Pipeline: "extra", Kind: store.ProposalConfig,
		BaseRevision: base.Hash, Patch: diff, PatchSHA256: patch.Hash([]byte(diff))}
}

// TestVerify: a publish action publishes the validated patch only, under its kind's path rules, for its pipeline.
func TestVerify(t *testing.T) {
	t.Parallel()
	_, packages, acc := fixture(t)
	if prep, err := Verify(t.Context(), packages, acc); err != nil || prep.Patched.Pipeline.Steps["pass"].Config["expr"] != "false" {
		t.Fatalf("Verify = %v", err)
	}
	tests := map[string]func(a *Accepted){
		"other content":     func(a *Accepted) { a.Patch = strings.Replace(a.Patch, "false", "1 > 0", 1) },
		"no hash":           func(a *Accepted) { a.PatchSHA256 = "" },
		"kind's paths":      func(a *Accepted) { a.Kind = store.ProposalLessons },
		"another pipeline":  func(a *Accepted) { a.Pipeline = "other" },
		"unknown base":      func(a *Accepted) { a.BaseRevision = strings.Repeat("0", 64) },
		"symlink on create": func(a *Accepted) { a.Patch, a.PatchSHA256 = symlinkPatch, patch.Hash([]byte(symlinkPatch)) },
	}
	for name, mutate := range tests {
		a := acc
		mutate(&a)
		_, err := Verify(t.Context(), packages, a)
		if want := ErrRefused; name == "unknown base" {
			if !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("%s: %v", name, err)
			}
		} else if !errors.Is(err, want) {
			t.Fatalf("%s: %v, want a refusal", name, err)
		}
	}
}

const symlinkPatch = "diff --git a/lessons/pass.yaml b/lessons/pass.yaml\nnew file mode 120000\n" +
	"--- /dev/null\n+++ b/lessons/pass.yaml\n@@ -0,0 +1 @@\n+/etc/passwd\n"

// TestNew: improve.publish picks the action, each refusing what it lacks.
func TestNew(t *testing.T) {
	t.Parallel()
	_, packages, _ := fixture(t)
	if p, err := New(pipeline.Improve{}, Deps{Packages: packages}); p != nil || err != nil {
		t.Fatalf("no publish action: %v, %v", p, err)
	}
	for name, im := range map[string]pipeline.Improve{
		"apply without an engine":  {Publish: pipeline.PublishApply},
		"patch-file without a dir": {Publish: pipeline.PublishPatchFile},
		"an unknown action":        {Publish: "git-pr-gh"},
	} {
		if p, err := New(im, Deps{Packages: packages}); err == nil {
			t.Fatalf("%s: %v", name, p)
		}
	}
	p, err := New(pipeline.Improve{Publish: pipeline.PublishPatchFile, PatchFile: pipeline.PatchFileConfig{Dir: "d"}},
		Deps{Packages: packages})
	if err != nil || p.Name() != pipeline.PublishPatchFile {
		t.Fatalf("New = %v, %v", p, err)
	}
}

// TestPatchFile: patch-file writes the validated bytes as <id>.patch; publishing again is the same publication, and
// other bytes under that name are refused.
func TestPatchFile(t *testing.T) {
	t.Parallel()
	_, packages, acc := fixture(t)
	dir := filepath.Join(t.TempDir(), "proposals")
	p, err := New(pipeline.Improve{Publish: pipeline.PublishPatchFile, PatchFile: pipeline.PatchFileConfig{Dir: dir}},
		Deps{Packages: packages})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		res, err := p.Publish(t.Context(), acc)
		if err != nil || res.Where != filepath.Join(dir, "pr_1.patch") || res.Publisher != pipeline.PublishPatchFile {
			t.Fatalf("Publish = %+v, %v", res, err)
		}
	}
	if data, err := os.ReadFile(filepath.Join(dir, "pr_1.patch")); err != nil || patch.Hash(data) != acc.PatchSHA256 {
		t.Fatalf("the file holds other bytes: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pr_2.patch"), []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	acc.ProposalID = "pr_2"
	if _, err := p.Publish(t.Context(), acc); !errors.Is(err, ErrRefused) {
		t.Fatalf("another file in the way: %v", err)
	}
	acc.ProposalID = "../escape"
	if _, err := p.Publish(t.Context(), acc); !errors.Is(err, ErrRefused) {
		t.Fatalf("an id naming a path: %v", err)
	}
}

// TestApply: apply hands the patched files to the engine on the proposal's base.
func TestApply(t *testing.T) {
	t.Parallel()
	_, packages, acc := fixture(t)
	a := &fakeApplier{}
	p, err := New(pipeline.Improve{Publish: pipeline.PublishApply}, Deps{Packages: packages, Applier: a})
	if err != nil {
		t.Fatal(err)
	}
	res, err := p.Publish(t.Context(), acc)
	if err != nil || res.Where != "7" || a.base != acc.BaseRevision ||
		!strings.Contains(string(a.files["pipeline.yaml"]), `expr: "false"`) {
		t.Fatalf("Publish = %+v, %v; applied on %s", res, err, a.base)
	}
}

type fakeApplier struct {
	base  string
	files map[string][]byte
}

func (f *fakeApplier) ApplyProposal(_ context.Context, _, _, base string, files map[string][]byte) (store.RevisionID,
	error,
) {
	f.base, f.files = base, files
	return 7, nil
}

// TestProposal: the hook publishes an accepted proposal with the patch the caller saw, once, recording where it went;
// asked again it answers that, and any other state or patch is refused.
func TestProposal(t *testing.T) {
	t.Parallel()
	_, packages, acc := fixture(t)
	a := &fakeApplier{}
	p, err := New(pipeline.Improve{Publish: pipeline.PublishApply}, Deps{Packages: packages, Applier: a})
	if err != nil {
		t.Fatal(err)
	}
	st := &fakeProposals{p: store.Proposal{NewProposal: store.NewProposal{ID: acc.ProposalID, Pipeline: acc.Pipeline,
		Kind: acc.Kind, BaseRevision: acc.BaseRevision, Patch: acc.Patch, PatchSHA256: acc.PatchSHA256},
		State: store.ProposalReady}}
	if _, _, err := Proposal(t.Context(), st, p, acc.ProposalID, acc.PatchSHA256); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("a ready proposal: %v", err)
	}
	st.p.State = store.ProposalAccepted
	if _, _, err := Proposal(t.Context(), st, p, acc.ProposalID, "other"); !errors.Is(err, ErrRefused) {
		t.Fatalf("another patch: %v", err)
	}
	for i, wantAgain := range []bool{false, true} {
		res, again, err := Proposal(t.Context(), st, p, acc.ProposalID, acc.PatchSHA256)
		if err != nil || again != wantAgain || res.Where != "7" || res.Publisher != pipeline.PublishApply ||
			len(st.events) != 1 {
			t.Fatalf("publication %d = %+v, again %v, %v; events %d", i, res, again, err, len(st.events))
		}
	}
	if a.base == "" {
		t.Fatal("never applied")
	}
	// apply stopped between its revision and the record: the proposalApplied event counts as the publication.
	a.base, st.events = "", []store.Event{{Kind: store.EventProposalApplied,
		Data: json.RawMessage(`{"id":"` + acc.ProposalID + `","revision":8,"validated":true}`)}}
	res, again, err := Proposal(t.Context(), st, p, acc.ProposalID, acc.PatchSHA256)
	if err != nil || !again || res.Where != "8" || a.base != "" || len(st.events) != 2 {
		t.Fatalf("after an interrupted apply = %+v, again %v, %v; applied again %v", res, again, err, a.base != "")
	}
}

// fakeProposals holds one proposal and the proposalPublished events recorded.
type fakeProposals struct {
	p      store.Proposal
	events []store.Event
}

func (f *fakeProposals) Proposal(context.Context, string) (store.Proposal, error) { return f.p, nil }

func (f *fakeProposals) PipelineEvents(context.Context, store.EventFilter) ([]store.Event, error) {
	return f.events, nil
}

func (f *fakeProposals) RecordPublished(_ context.Context, id, publisher, where string) error {
	data, _ := json.Marshal(map[string]string{"id": id, "publisher": publisher, "where": where})
	f.events = append(f.events, store.Event{Kind: store.EventProposalPublished, Data: data})
	return nil
}

// TestBuildCommand: a distill proposal's publication says how to build the Go action it publishes; no other kind, and
// no distill patch that adds no action, says anything.
func TestBuildCommand(t *testing.T) {
	t.Parallel()
	add := func(path string) string {
		return "--- /dev/null\n+++ b/" + path + "\n@@ -0,0 +1 @@\n+x\n"
	}
	distillPatch := add("actions/score-rules/main.go") + add("actions/score-rules/action.yaml")
	want := "cd actions/score-rules && go build -o bin/$(go env GOOS)-$(go env GOARCH)/score-rules ."
	if got := BuildCommand(store.ProposalDistill, distillPatch); got != want {
		t.Errorf("BuildCommand(distill) = %q, want %q", got, want)
	}
	for name, c := range map[string]struct {
		kind  store.ProposalKind
		patch string
	}{
		"config kind": {store.ProposalConfig, distillPatch},
		"no action":   {store.ProposalDistill, diff},
		"unparsed":    {store.ProposalDistill, "not a patch"},
		"empty patch": {store.ProposalDistill, ""},
		"modify only": {store.ProposalDistill, strings.Replace(distillPatch, "/dev/null", "a/actions/x/main.go", 2)},
	} {
		if got := BuildCommand(c.kind, c.patch); got != "" {
			t.Errorf("%s: BuildCommand = %q, want none", name, got)
		}
	}
	// Proposal carries it on the result, published now or before.
	st := &fakeProposals{p: store.Proposal{NewProposal: store.NewProposal{ID: "pr_2", Pipeline: "extra",
		Kind: store.ProposalDistill, Patch: distillPatch, PatchSHA256: patch.Hash([]byte(distillPatch))},
		State: store.ProposalAccepted}}
	for i, wantAgain := range []bool{false, true} {
		res, again, err := Proposal(t.Context(), st, stubPublisher{}, "pr_2", st.p.PatchSHA256)
		if err != nil || again != wantAgain || res.Build != want {
			t.Errorf("publication %d: build %q, again %v, %v; want %q", i, res.Build, again, err, want)
		}
	}
}

// stubPublisher publishes anything, as a patch file.
type stubPublisher struct{}

func (stubPublisher) Name() string { return pipeline.PublishPatchFile }

func (stubPublisher) Publish(context.Context, Accepted) (Result, error) {
	return Result{Publisher: pipeline.PublishPatchFile, Where: "x.patch"}, nil
}
