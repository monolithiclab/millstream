package publish

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/proposals"
	"github.com/monolithiclab/millstream/internal/runtime/container"
	"github.com/monolithiclab/millstream/internal/runtime/exec"
)

// The git actions (spec section 11.7, rule 2) run in a container of their own, from improve.git.image, which holds
// git and the forge's CLI (gh or glab), with the network and the variables improve.git.env names, and nothing else:
// no package, no store, no other credential. Their work directory is mounted at /work. They clone the base branch,
// and Millstream, outside the container, applies exactly the validated patch to the package's files there (the
// patch package's Apply: each hunk at its lines, with no offset or fuzz), refusing a symbolic link or a file that is
// not regular; then the container commits it with a Millstream-Proposal trailer, pushes a branch and opens the pull
// request, its body the validation report. git runs with hooks disabled (core.hooksPath=/dev/null) and its commit
// hooks skipped, and nothing is built or run from the repository.

// TrailerProposal is the commit trailer naming the proposal a commit publishes (spec section 11.3), and TrailerPatch
// the one naming its patch's hash, by which a publication interrupted after its push is recognised.
const (
	TrailerProposal = "Millstream-Proposal"
	TrailerPatch    = "Millstream-Patch"
)

// jobMount is where a git job's work directory is mounted in its container.
const jobMount = "/work"

// gitJob is one step of a git action: a POSIX shell script run with WORK naming the work directory, with env.
type gitJob struct {
	work   string
	script string
	env    []string
}

// jobRunner runs a git job and returns its standard output.
type jobRunner interface {
	run(ctx context.Context, j gitJob) (string, error)
}

// containerJobs runs the jobs in the action's container.
type containerJobs struct {
	rt    container.JobRunner
	image string
	// mounts are mounted besides the work directory: tests mount a local repository and fake CLIs.
	mounts []exec.Mount
}

func (c containerJobs) run(ctx context.Context, j gitJob) (string, error) {
	var stdout, stderr bytes.Buffer
	err := c.rt.RunJob(ctx, container.Job{Image: c.image,
		Entrypoint: "/bin/sh", Command: []string{"-euc", j.script}, Workdir: jobMount, Network: true,
		Mounts: append([]exec.Mount{{Source: j.work, Target: jobMount}}, c.mounts...),
		Env:    append([]string{"WORK=" + jobMount, "HOME=/tmp"}, j.env...)}, &stdout, &stderr)
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, tail(stderr.String()))
	}
	return stdout.String(), nil
}

// tail is the end of a job's error output, for an error message.
func tail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 2000 {
		s = "…" + s[len(s)-2000:]
	}
	return s
}

// gitPR is git-pr-gh or git-pr-glab.
type gitPR struct {
	forge string // gh or glab
	cfg   pipeline.GitConfig
	creds []string // the variables improve.git.env names, KEY=VALUE
	jobs  jobRunner
}

func newGitPR(name string, cfg pipeline.GitConfig, d Deps) (*gitPR, error) {
	g := &gitPR{forge: "gh", cfg: cfg, jobs: d.jobs}
	if name == pipeline.PublishGitPRGLab {
		g.forge = "glab"
	}
	if cfg.Repo == "" || cfg.Image == "" {
		return nil, fmt.Errorf("publish: %s needs improve.git.repo and improve.git.image", name)
	}
	if g.jobs == nil {
		if d.Runtime == nil {
			return nil, fmt.Errorf("publish: %s runs in a container (spec section 11.7), and no container runtime "+
				"is installed", name)
		}
		g.jobs = containerJobs{rt: d.Runtime, image: cfg.Image}
	}
	for _, k := range cfg.Env {
		v, ok := "", false
		if d.Env != nil {
			v, ok = d.Env(k)
		}
		if !ok {
			return nil, fmt.Errorf("publish: improve.git.env names %s, which the engine's environment does not set", k)
		}
		g.creds = append(g.creds, k+"="+v)
	}
	return g, nil
}

// gitPrelude defines g: git with hooks disabled, the forge's CLI as its only credential helper, no prompt, and no
// system or user configuration. The settings are also its global configuration, which the git processes it starts
// for a local remote keep (they drop the command line's).
const gitPrelude = `export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL="$WORK/gitconfig" GIT_TERMINAL_PROMPT=0 GH_PROMPT_DISABLED=1
printf '[core]\n\thooksPath = /dev/null\n[safe]\n\tdirectory = *\n[protocol "ext"]\n\tallow = never\n[commit]\n\tgpgsign = false\n[credential]\n\thelper = !%s auth git-credential\n' "$MS_FORGE" > "$GIT_CONFIG_GLOBAL"
g() { git -c core.hooksPath=/dev/null "$@"; }
`

// cloneScript clones the base branch alone and starts the proposal's branch.
const cloneScript = gitPrelude + `g clone --quiet --depth 1 --no-tags --single-branch --branch "$MS_BASE" -- "$MS_REPO" "$WORK/repo"
g -C "$WORK/repo" checkout --quiet -b "$MS_BRANCH"
`

// resumeScript finds a publication of the proposal an earlier run left: when its branch exists, it prints the
// Millstream-Patch trailer of the branch's commit and the URL of the pull request open on it, if any.
const resumeScript = gitPrelude + `g ls-remote --exit-code --heads -- "$MS_REPO" "refs/heads/$MS_BRANCH" >/dev/null || exit 0
g init --quiet "$WORK/resume"
cd "$WORK/resume"
g remote add origin "$MS_REPO"
g fetch --quiet --depth 1 --no-tags origin "refs/heads/$MS_BRANCH"
echo "patch $(g log -1 --format=%B FETCH_HEAD | sed -n 's/^Millstream-Patch: *//p' | tail -n 1)"
case "$MS_FORGE" in
gh) url=$(gh pr view "$MS_BRANCH" --json url --jq .url 2>/dev/null || true) ;;
glab) url=$(glab mr view "$MS_BRANCH" --output json 2>/dev/null | sed -n 's/.*"web_url": *"\([^"]*\)".*/\1/p' | head -n 1) ;;
esac
echo "url $url"
`

// publishScript commits the package directory's changes, pushes the branch (never forced, and not at all when
// MS_PUSHED says an earlier run did) and opens the pull request, printing its URL.
const publishScript = gitPrelude + `cd "$WORK/repo"
g add --all -- "$MS_PATH"
g -c user.name="$MS_AUTHOR_NAME" -c user.email="$MS_AUTHOR_EMAIL" commit --quiet --no-verify -F "$WORK/message"
[ "${MS_PUSHED:-}" = 1 ] || g push --quiet origin "HEAD:refs/heads/$MS_BRANCH"
case "$MS_FORGE" in
gh) gh pr create --base "$MS_BASE" --head "$MS_BRANCH" --title "$MS_TITLE" --body-file "$WORK/body.md" ;;
glab) glab mr create --source-branch "$MS_BRANCH" --target-branch "$MS_BASE" --title "$MS_TITLE" \
  --description "$(cat "$WORK/body.md")" --yes ;;
esac
`

func (g *gitPR) publish(ctx context.Context, acc Accepted, prep proposals.Prepared) (string, error) {
	if !proposalID.MatchString(acc.ProposalID) {
		return "", fmt.Errorf("%w: proposal id %q cannot name a branch", ErrRefused, acc.ProposalID)
	}
	work, err := os.MkdirTemp("", "millstream-publish-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(work) }()
	pkgPath := strings.ReplaceAll(cmp.Or(g.cfg.Path, "."), "{pipeline}", acc.Pipeline)
	if pkgPath != path.Clean(pkgPath) || strings.HasPrefix(pkgPath, "../") || path.IsAbs(pkgPath) {
		return "", fmt.Errorf("%w: improve.git.path %q is not inside the repository", ErrRefused, pkgPath)
	}
	title := fmt.Sprintf("%s: %s proposal %s", acc.Pipeline, acc.Kind, acc.ProposalID)
	env := append([]string{"MS_FORGE=" + g.forge, "MS_REPO=" + g.cfg.Repo,
		"MS_BASE=" + cmp.Or(g.cfg.Base, pipeline.DefaultGitBase), "MS_BRANCH=millstream/" + acc.ProposalID,
		"MS_PATH=" + pkgPath, "MS_TITLE=" + title, "MS_AUTHOR_NAME=" + cmp.Or(g.cfg.Author.Name, "Millstream"),
		"MS_AUTHOR_EMAIL=" + cmp.Or(g.cfg.Author.Email, "millstream@localhost")}, g.creds...)
	// A run that stopped after its push left the branch: the same patch resumes there, never pushed again.
	out, err := g.jobs.run(ctx, gitJob{work: work, script: resumeScript, env: env})
	if err != nil {
		return "", fmt.Errorf("look for an earlier publication on %s: %w", g.cfg.Repo, err)
	}
	if hash, url, found := resumed(out); found {
		switch {
		case hash != acc.PatchSHA256:
			return "", fmt.Errorf("%w: branch millstream/%s exists with another patch (%.12s)", ErrRefused,
				acc.ProposalID, hash)
		case url != "":
			return url, nil
		}
		env = append(env, "MS_PUSHED=1")
	}
	if _, err := g.jobs.run(ctx, gitJob{work: work, script: cloneScript, env: env}); err != nil {
		return "", fmt.Errorf("clone %s: %w", g.cfg.Repo, err)
	}
	dir := filepath.Join(work, "repo", filepath.FromSlash(pkgPath))
	onBase := baseNote(dir, acc.BaseRevision)
	if err := applyTo(dir, acc.Patch, prep.Changes); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(work, "message"), []byte(commitMessage(title, acc)), 0o600); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(work, "body.md"), []byte(body(acc, onBase)), 0o600); err != nil {
		return "", err
	}
	out, err = g.jobs.run(ctx, gitJob{work: work, script: publishScript, env: env})
	if err != nil {
		return "", fmt.Errorf("open the pull request: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	url := strings.TrimSpace(lines[len(lines)-1])
	if url == "" {
		return "", fmt.Errorf("%s printed no pull request URL", g.forge)
	}
	return url, nil
}

// resumed reads resumeScript's output: whether the branch exists, the patch hash its commit names and the open
// pull request's URL.
func resumed(out string) (hash, url string, found bool) {
	for l := range strings.Lines(out) {
		if v, ok := strings.CutPrefix(strings.TrimSpace(l), "patch"); ok {
			hash, found = strings.TrimSpace(v), true
		} else if v, ok := strings.CutPrefix(strings.TrimSpace(l), "url"); ok {
			url = strings.TrimSpace(v)
		}
	}
	return hash, url, found
}

// applyTo applies the validated patch to the package directory dir, exactly: each changed file must be a regular
// file (a created one absent), read and written through an os.Root so no path leaves dir.
func applyTo(dir, text string, changes []patch.File) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("%w: the package directory: %w", ErrRefused, err)
	}
	defer func() { _ = root.Close() }()
	files := map[string][]byte{}
	for _, c := range changes {
		p := filepath.FromSlash(c.Path)
		info, err := root.Lstat(p)
		switch {
		case c.Op == patch.OpCreate && errors.Is(err, fs.ErrNotExist):
			continue
		case c.Op == patch.OpCreate && err == nil:
			return fmt.Errorf("%w: %s: created by the patch, but the branch has it", ErrRefused, c.Path)
		case err != nil:
			return fmt.Errorf("%w: %s: %w", ErrRefused, c.Path, err)
		case !info.Mode().IsRegular():
			return fmt.Errorf("%w: %s is not a regular file on the branch (%s)", ErrRefused, c.Path, info.Mode().Type())
		}
		if files[c.Path], err = root.ReadFile(p); err != nil {
			return err
		}
	}
	parsed, err := patch.Parse([]byte(text))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrRefused, err)
	}
	out, err := parsed.Apply(files)
	if err != nil {
		return fmt.Errorf("%w: the branch's files are not those the patch was validated on: %w", ErrRefused, err)
	}
	for _, c := range changes {
		p := filepath.FromSlash(c.Path)
		if c.Op == patch.OpDelete {
			if err := root.Remove(p); err != nil {
				return err
			}
			continue
		}
		if d := filepath.Dir(p); d != "." {
			if err := root.MkdirAll(d, 0o755); err != nil {
				return err
			}
		}
		if err := root.WriteFile(p, out[c.Path], 0o644); err != nil { // #nosec G306 -- a package file, as git keeps it
			return err
		}
	}
	return nil
}

// baseNote says whether the branch's package is the revision the proposal was validated on.
func baseNote(dir, base string) string {
	pkg, err := pipeline.Load(dir)
	switch {
	case err != nil:
		return "The branch's package could not be loaded (" + err.Error() + "): compare it with the base revision."
	case pkg.Hash == base:
		return "The branch's package is the base revision the proposal was validated on."
	}
	return fmt.Sprintf("The branch's package (%.12s) differs from the base revision the proposal was validated on: "+
		"this combination was not validated. Once applied, Millstream runs the evals and a backtest of it.", pkg.Hash)
}

// rationale is the proposal's rationale without lines that read as Millstream trailers: an agent wrote it, and it
// must not name another proposal.
func rationale(acc Accepted) string {
	lines := strings.Split(acc.Rationale, "\n")
	lines = slices.DeleteFunc(lines, func(l string) bool {
		return strings.HasPrefix(strings.ToLower(strings.TrimSpace(l)), "millstream-")
	})
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// commitMessage is the commit's message, ending with the proposal's trailer.
func commitMessage(title string, acc Accepted) string {
	var b strings.Builder
	b.WriteString(title + "\n\n")
	if r := rationale(acc); r != "" {
		b.WriteString(r + "\n\n")
	}
	fmt.Fprintf(&b, "%s: %s\n%s: %s\n", TrailerProposal, acc.ProposalID, TrailerPatch, acc.PatchSHA256)
	return b.String()
}

// body is the pull request's description: the rationale, the base, the validation report, and the trailer.
func body(acc Accepted, onBase string) string {
	var b strings.Builder
	if r := rationale(acc); r != "" {
		b.WriteString(r + "\n\n")
	}
	fmt.Fprintf(&b, "Validated by Millstream on base revision `%s`, patch SHA-256 `%s`. %s\n\n", acc.BaseRevision,
		acc.PatchSHA256, onBase)
	report := []byte(acc.Report)
	var indented bytes.Buffer
	if json.Indent(&indented, report, "", "  ") == nil {
		report = indented.Bytes()
	}
	fmt.Fprintf(&b, "<details><summary>Validation report</summary>\n\n```json\n%s\n```\n\n</details>\n\n", report)
	fmt.Fprintf(&b, "%s: %s\n", TrailerProposal, acc.ProposalID)
	return b.String()
}

// Trailers returns the proposals the Millstream-Proposal trailers of a commit message name: the lines of its last
// paragraph, as git reads trailers, holding a proposal id.
func Trailers(message string) []string {
	paragraphs := strings.Split(strings.TrimSpace(strings.ReplaceAll(message, "\r\n", "\n")), "\n\n")
	var out []string
	for _, l := range strings.Split(paragraphs[len(paragraphs)-1], "\n") {
		k, v, ok := strings.Cut(l, ":")
		v = strings.TrimSpace(v)
		if ok && strings.EqualFold(strings.TrimSpace(k), TrailerProposal) && proposalID.MatchString(v) &&
			!slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	return out
}
