package publish

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
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
		BaseRevision: base.Hash, Patch: diff, PatchSHA256: patch.Hash([]byte(diff)),
		Rationale: "Nothing passes.\nMillstream-Proposal: pr_forged", Report: []byte(`{"passed":true}`)}
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
	git := pipeline.GitConfig{Repo: "r", Image: "i", Env: []string{"GH_TOKEN"}}
	tests := map[string]struct {
		im   pipeline.Improve
		jobs jobRunner
	}{
		"apply without an engine":   {pipeline.Improve{Publish: pipeline.PublishApply}, nil},
		"patch-file without a dir":  {pipeline.Improve{Publish: pipeline.PublishPatchFile}, nil},
		"git without a runtime":     {pipeline.Improve{Publish: pipeline.PublishGitPRGH, Git: pipeline.GitConfig{Repo: "r", Image: "i"}}, nil},
		"git without its variables": {pipeline.Improve{Publish: pipeline.PublishGitPRGLab, Git: git}, &fakeForge{}},
		"git without a repository":  {pipeline.Improve{Publish: pipeline.PublishGitPRGH}, &fakeForge{}},
	}
	for name, tt := range tests {
		if p, err := New(tt.im, Deps{Packages: packages, jobs: tt.jobs}); err == nil {
			t.Fatalf("%s: %v", name, p)
		}
	}
	p, err := New(pipeline.Improve{Publish: pipeline.PublishGitPRGH, Git: git}, Deps{Packages: packages,
		Env: func(k string) (string, bool) { return "secret", k == "GH_TOKEN" }, jobs: &fakeForge{}})
	if err != nil || p.Name() != pipeline.PublishGitPRGH {
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
		if err != nil || res.RevisionOrURL != filepath.Join(dir, "pr_1.patch") || res.Publisher != pipeline.PublishPatchFile {
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
	if err != nil || res.RevisionOrURL != "7" || a.base != acc.BaseRevision ||
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

// TestGitPR: the git action clones, applies exactly the validated patch to the package directory, and commits it
// with the trailer and the report in the pull request's body, with only the named credentials; a branch whose files
// differ where the patch applies, or holding a symlink there, is refused before anything is pushed.
func TestGitPR(t *testing.T) {
	t.Parallel()
	_, packages, acc := fixture(t)
	for _, forge := range []string{pipeline.PublishGitPRGH, pipeline.PublishGitPRGLab} {
		f := &fakeForge{files: map[string]string{"pipelines/extra/pipeline.yaml": baseSrc}}
		p, err := New(pipeline.Improve{Publish: forge, Git: pipeline.GitConfig{Repo: "https://forge.example/p.git",
			Path: "pipelines/{pipeline}", Image: "i", Env: []string{"TOKEN"}}}, Deps{Packages: packages,
			Env: func(k string) (string, bool) { return "s3cret", k == "TOKEN" }, jobs: f})
		if err != nil {
			t.Fatal(err)
		}
		res, err := p.Publish(t.Context(), acc)
		if err != nil || res.RevisionOrURL != "https://forge.example/pr/1" {
			t.Fatalf("%s: Publish = %+v, %v", forge, res, err)
		}
		if !strings.Contains(f.published, `expr: "false"`) || !slices.Contains(f.env, "TOKEN=s3cret") ||
			!slices.Contains(f.env, "MS_BRANCH=millstream/pr_1") || !slices.Contains(f.env, "MS_PATH=pipelines/extra") ||
			!slices.Contains(f.env, "MS_FORGE="+strings.TrimPrefix(forge, "git-pr-")) {
			t.Fatalf("%s: published %q with %q", forge, f.published, f.env)
		}
		if !strings.HasSuffix(f.message, "\n\nMillstream-Proposal: pr_1\n") || strings.Contains(f.message, "pr_forged") ||
			!strings.Contains(f.body, `"passed": true`) || !strings.Contains(f.body, "is the base revision") {
			t.Fatalf("%s: message %q body %q", forge, f.message, f.body)
		}
	}
	for name, files := range map[string]map[string]string{
		"moved lines": {"pipelines/extra/pipeline.yaml": "# moved\n" + baseSrc},
		"no package":  {},
	} {
		f := &fakeForge{files: files}
		p, err := New(pipeline.Improve{Publish: pipeline.PublishGitPRGH, Git: pipeline.GitConfig{Repo: "r",
			Path: "pipelines/extra", Image: "i"}}, Deps{Packages: packages, jobs: f})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Publish(t.Context(), acc); !errors.Is(err, ErrRefused) || f.published != "" {
			t.Fatalf("%s: %v, published %q", name, err, f.published)
		}
	}
	f := &fakeForge{files: map[string]string{"real.yaml": baseSrc}, link: "pipeline.yaml"}
	p, err := New(pipeline.Improve{Publish: pipeline.PublishGitPRGH, Git: pipeline.GitConfig{Repo: "r", Image: "i"}},
		Deps{Packages: packages, jobs: f})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Publish(t.Context(), acc); !errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), "regular") {
		t.Fatalf("a symlink on the branch: %v", err)
	}
}

// fakeForge runs the git jobs in-process: the clone writes files (and a symlink link to real.yaml) under the work
// directory's repo, and the publication records what it would push.
type fakeForge struct {
	files                    map[string]string
	link                     string
	env                      []string
	published, message, body string
}

func (f *fakeForge) run(_ context.Context, j gitJob) (string, error) {
	repo := filepath.Join(j.work, "repo")
	if j.script == cloneScript {
		for p, src := range f.files {
			if err := os.MkdirAll(filepath.Dir(filepath.Join(repo, p)), 0o750); err != nil {
				return "", err
			}
			if err := os.WriteFile(filepath.Join(repo, p), []byte(src), 0o600); err != nil {
				return "", err
			}
		}
		if f.link != "" {
			return "", os.Symlink(filepath.Join(repo, "real.yaml"), filepath.Join(repo, f.link))
		}
		return "", os.MkdirAll(repo, 0o750)
	}
	f.env = j.env
	dir := ""
	for _, kv := range j.env {
		if v, ok := strings.CutPrefix(kv, "MS_PATH="); ok {
			dir = v
		}
	}
	read := func(p string) string {
		b, _ := os.ReadFile(p) // #nosec G304 -- the test's own files
		return string(b)
	}
	f.published = read(filepath.Join(repo, dir, "pipeline.yaml"))
	f.message, f.body = read(filepath.Join(j.work, "message")), read(filepath.Join(j.work, "body.md"))
	return "Creating pull request\nhttps://forge.example/pr/1\n", nil
}

// TestTrailers: only the last paragraph's Millstream-Proposal lines with a proposal id count, each once.
func TestTrailers(t *testing.T) {
	t.Parallel()
	msg := "extra: config proposal pr_1\n\nMillstream-Proposal: pr_body\n\nSigned-off-by: a\n" +
		"Millstream-Proposal: pr_1\nmillstream-proposal: pr_2\nMillstream-Proposal: pr_1\nMillstream-Proposal: ../x\n"
	if got := Trailers(msg); !slices.Equal(got, []string{"pr_1", "pr_2"}) {
		t.Fatalf("Trailers = %q", got)
	}
	if got := Trailers(""); got != nil {
		t.Fatalf("Trailers of nothing = %q", got)
	}
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
		if err != nil || again != wantAgain || res.RevisionOrURL != "7" || res.Publisher != pipeline.PublishApply ||
			len(st.events) != 1 {
			t.Fatalf("publication %d = %+v, again %v, %v; events %d", i, res, again, err, len(st.events))
		}
	}
	if a.base == "" {
		t.Fatal("never applied")
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

// TestContainerJobs: a git job runs in the action's image with the network, its work directory at /work, the
// variables it was given, and its error output's end in the error.
func TestContainerJobs(t *testing.T) {
	t.Parallel()
	f := &fakeRunner{stdout: "https://forge.example/pr/1\n"}
	c := containerJobs{rt: f, image: "img@sha256:abc"}
	out, err := c.run(t.Context(), gitJob{work: "/host/work", script: "true", env: []string{"TOKEN=x"}})
	if err != nil || out != f.stdout || !f.job.Network || f.job.Image != "img@sha256:abc" ||
		f.job.Mounts[0] != (exec.Mount{Source: "/host/work", Target: jobMount}) || !slices.Contains(f.job.Env, "TOKEN=x") ||
		!slices.Contains(f.job.Env, "WORK=/work") {
		t.Fatalf("run = %q, %v; job %+v", out, err, f.job)
	}
	f.err, f.stderr = errors.New("exit status 128"), strings.Repeat("x", 3000)+"fatal: denied"
	if _, err := c.run(t.Context(), gitJob{work: "/w"}); err == nil || !strings.HasSuffix(err.Error(), "fatal: denied") ||
		len(err.Error()) > 2100 {
		t.Fatalf("a failing job: %v", err)
	}
}

type fakeRunner struct {
	job            container.Job
	stdout, stderr string
	err            error
}

func (f *fakeRunner) RunJob(_ context.Context, j container.Job, stdout, stderr io.Writer) error {
	f.job = j
	_, _ = io.WriteString(stdout, f.stdout)
	_, _ = io.WriteString(stderr, f.stderr)
	return f.err
}

// TestGitPRDistill: a distill's new action directory is created on the branch, and refused when the branch has it.
func TestGitPRDistill(t *testing.T) {
	t.Parallel()
	base, packages, acc := fixture(t)
	after := map[string][]byte{"pipeline.yaml": []byte(strings.Replace(baseSrc, "action: filter, in: trigger.adhoc, config: { expr: \"true\" }",
		"action: ./actions/coded, in: trigger.adhoc", 1)), "actions/coded/action.yaml": []byte("name: coded\nkind: exec\n")}
	text, err := patch.Format(base.Hash, base.Files, after)
	if err != nil {
		t.Fatal(err)
	}
	acc.Kind, acc.Patch, acc.PatchSHA256 = store.ProposalDistill, string(text), patch.Hash(text)
	for _, exists := range []bool{false, true} {
		files := map[string]string{"pipeline.yaml": baseSrc}
		if exists {
			files["actions/coded/action.yaml"] = "x"
		}
		f := &fakeForge{files: files}
		p, err := New(pipeline.Improve{Publish: pipeline.PublishGitPRGH, Git: pipeline.GitConfig{Repo: "r", Image: "i"}},
			Deps{Packages: packages, jobs: f})
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.Publish(t.Context(), acc)
		if exists != (err != nil) || !exists && !strings.Contains(f.published, "./actions/coded") {
			t.Fatalf("branch has the action %v: %v, published %q", exists, err, f.published)
		}
	}
}
