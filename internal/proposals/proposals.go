// Package proposals holds the engine's proposal rules (spec section 11.3): a proposal's patch (the patch format of
// internal/patch) applied to its base revision's files, or rebased onto a newer one, the paths each kind may touch
// (spec section 11.7), the steps a patch touches, the fingerprint a rejection suppresses, and the validation the
// engine runs before a proposal is ready: check on the patched package, the base revision's evals before and after,
// and a backtest.
//
// The store's units keep the states (store.Store's CreateProposal through RejectProposal); the engine
// (engine/proposals.go) calls this package before them. Every refusal of a patch wraps patch.ErrPatch.
package proposals

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Prepared is a patch checked against its kind's path rules and applied to its base: what a proposal records.
type Prepared struct {
	// Kind is the proposal's kind, empty for a revision's change (Diff).
	Kind store.ProposalKind
	// Patch is the patch, which SHA256 hashes (patch.Hash).
	Patch  string
	SHA256 string
	// Changes are the files it changes, in the order it lists them.
	Changes []patch.File
	// Base and Patched are the packages before and after the patch, their Dir empty: nothing is on disk yet.
	Base, Patched *pipeline.Package
	Steps         []string
}

// Paths returns the paths the patch touches, sorted.
func (p Prepared) Paths() []string { return (&patch.Patch{Files: p.Changes}).Paths() }

// Prepare applies a patch to the base package's files, exactly at its lines (patch.Patch.Apply), refusing any path its
// kind does not allow (spec section 11.7), and derives the steps it touches. It is called when a proposal is drafted
// and again when it is accepted. A patch naming a base revision (its Millstream-Base trailer) applies only to that
// one. The patched package must parse; check is the validation's.
func Prepare(kind store.ProposalKind, base *pipeline.Package, text string) (Prepared, error) {
	if !slices.Contains(store.ProposalKinds, kind) {
		return Prepared{}, fmt.Errorf("%w: kind %q is not one of %v", patch.ErrPatch, kind, store.ProposalKinds)
	}
	p, err := patch.Parse([]byte(text))
	if err != nil {
		return Prepared{}, err
	}
	if p.Base != "" && p.Base != base.Hash {
		return Prepared{}, fmt.Errorf("%w: the patch is written against revision %.12s, not %.12s", patch.ErrPatch,
			p.Base, base.Hash)
	}
	files, err := p.Apply(base.Files)
	if err != nil {
		return Prepared{}, err
	}
	return prepared(kind, base, text, p, files)
}

// prepared checks a parsed patch against its kind's path rules and builds what it prepared, files the patched
// package's.
func prepared(kind store.ProposalKind, base *pipeline.Package, text string, p *patch.Patch, files map[string][]byte,
) (Prepared, error) {
	if err := allowed(kind, base, p.Files); err != nil {
		return Prepared{}, err
	}
	patched, err := pipeline.FromFiles("", files)
	if err != nil {
		return Prepared{}, fmt.Errorf("%w: the patched package does not parse: %w", patch.ErrPatch, err)
	}
	return Prepared{Kind: kind, Patch: text, SHA256: patch.Hash([]byte(text)), Changes: p.Files, Base: base,
		Patched: patched, Steps: touchedSteps(base.Pipeline, patched.Pipeline, p.Files)}, nil
}

// Rebase moves a stale proposal's patch onto a newer base (spec section 11.3): each hunk applies where its context
// now is (patch.Patch.Rebase), and the patch rendered from the base to the result, with the base's Millstream-Base
// trailer, is checked as Prepare checks a patch: it becomes the proposal's patch, with a new hash.
func Rebase(kind store.ProposalKind, base *pipeline.Package, text string) (Prepared, error) {
	if !slices.Contains(store.ProposalKinds, kind) {
		return Prepared{}, fmt.Errorf("%w: kind %q is not one of %v", patch.ErrPatch, kind, store.ProposalKinds)
	}
	p, err := patch.Parse([]byte(text))
	if err != nil {
		return Prepared{}, err
	}
	files, err := p.Rebase(base.Files)
	if err != nil {
		return Prepared{}, err
	}
	rebased, err := patch.Format(base.Hash, base.Files, files)
	switch {
	case err != nil:
		return Prepared{}, err
	case rebased == nil:
		return Prepared{}, fmt.Errorf("%w: the revision already holds the patch's changes", patch.ErrPatch)
	}
	if p, err = patch.Parse(rebased); err != nil {
		return Prepared{}, err
	}
	return prepared(kind, base, string(rebased), p, files)
}

// Diff is the change from one revision's package to another's, prepared as a proposal's patch is but under no kind's
// path rules: what the engine validates when a revision carries proposals in a combination nobody validated (spec
// section 11.3).
func Diff(base, patched *pipeline.Package) (Prepared, error) {
	text, err := patch.Format(base.Hash, base.Files, patched.Files)
	switch {
	case err != nil:
		return Prepared{}, err
	case text == nil:
		return Prepared{}, fmt.Errorf("%w: revisions %.12s and %.12s hold the same files", patch.ErrPatch, base.Hash,
			patched.Hash)
	}
	p, err := patch.Parse(text)
	if err != nil {
		return Prepared{}, err
	}
	return Prepared{Patch: string(text), SHA256: patch.Hash(text), Changes: p.Files, Base: base, Patched: patched,
		Steps: touchedSteps(base.Pipeline, patched.Pipeline, p.Files)}, nil
}

// Fingerprint identifies a proposal's idea when its drafter names none: its pipeline, kind, the insights it answers
// and the paths it touches. A rejection suppresses it (spec section 11.3).
func Fingerprint(pipelineName string, kind store.ProposalKind, insights, paths []string) string {
	ins := slices.Sorted(slices.Values(insights))
	ps := slices.Sorted(slices.Values(paths))
	return patch.Hash([]byte(strings.Join([]string{pipelineName, string(kind), strings.Join(ins, ","),
		strings.Join(ps, ",")}, "\x00")))[:32]
}

// allowed checks each change against its kind's paths (spec section 11.7):
//
//	lessons            lessons/<step>.yaml, for a step of the base revision
//	prompt             actions/<action>/prompt.md, for an action directory of the base revision, never deleted
//	config, topology   pipeline.yaml, changed
//	distill            pipeline.yaml changed, and files added under one new actions/<name>/ directory
func allowed(kind store.ProposalKind, base *pipeline.Package, changes []patch.File) error {
	newDir := ""
	for _, c := range changes {
		refuse := func(why string) error {
			return fmt.Errorf("%w: a %s proposal may not %s %s: %s", patch.ErrPatch, kind, c.Op, c.Path, why)
		}
		if c.Path == pipeline.FileName {
			if c.Op != patch.OpModify {
				return refuse("the pipeline file is only changed")
			}
			if kind == store.ProposalConfig || kind == store.ProposalTopology || kind == store.ProposalDistill {
				continue
			}
			return refuse("only config, topology and distill proposals change pipeline.yaml")
		}
		switch kind {
		case store.ProposalLessons:
			step, ok := strings.CutPrefix(c.Path, "lessons/")
			step, yaml := strings.CutSuffix(step, ".yaml")
			if _, exists := base.Pipeline.Steps[step]; !ok || !yaml || !exists {
				return refuse("want lessons/<step>.yaml for a step of the pipeline")
			}
		case store.ProposalPrompt:
			action, ok := actionFile(c.Path, "prompt.md")
			if !ok || !hasDir(base.Files, "actions/"+action+"/") {
				return refuse("want actions/<action>/prompt.md of an action of the package")
			}
			if c.Op == patch.OpDelete {
				return refuse("an agent action needs its prompt")
			}
		case store.ProposalDistill:
			dir, ok := actionDir(c.Path)
			switch {
			case !ok:
				return refuse("want a file of a new actions/<name>/ directory, or pipeline.yaml")
			case hasDir(base.Files, "actions/"+dir+"/"):
				return refuse("actions/" + dir + "/ exists: a distill adds a new action")
			case newDir != "" && newDir != dir:
				return refuse("a distill adds one action directory, and adds " + newDir + " already")
			case c.Op != patch.OpCreate:
				return refuse("the new action's files are created")
			}
			newDir = dir
		default:
			return refuse("config and topology proposals change pipeline.yaml alone")
		}
	}
	if kind == store.ProposalDistill && newDir == "" {
		return fmt.Errorf("%w: a distill proposal adds a new actions/<name>/ directory", patch.ErrPatch)
	}
	return nil
}

// actionDir returns the action directory name of a path under actions/<name>/.
func actionDir(p string) (string, bool) {
	rest, ok := strings.CutPrefix(p, "actions/")
	dir, file, found := strings.Cut(rest, "/")
	return dir, ok && found && dir != "" && file != ""
}

// actionFile returns the action of actions/<action>/<name>.
func actionFile(p, name string) (string, bool) {
	dir, ok := actionDir(p)
	return dir, ok && p == "actions/"+dir+"/"+name
}

func hasDir(files map[string][]byte, prefix string) bool {
	for p := range files {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

// touchedSteps returns the steps a patch touches, sorted: those added, removed or changed in the pipeline file, those
// whose action or fallback is an action directory it changes, and those whose lessons file it changes.
func touchedSteps(base, patched *pipeline.Pipeline, changes []patch.File) []string {
	set := map[string]bool{}
	for name, s := range base.Steps {
		if p, ok := patched.Steps[name]; !ok || !reflect.DeepEqual(s, p) {
			set[name] = true
		}
	}
	for name := range patched.Steps {
		if _, ok := base.Steps[name]; !ok {
			set[name] = true
		}
	}
	for _, c := range changes {
		if step, ok := strings.CutPrefix(c.Path, "lessons/"); ok {
			set[strings.TrimSuffix(step, ".yaml")] = true
		}
		dir, ok := actionDir(c.Path)
		if !ok {
			continue
		}
		ref := "./actions/" + dir
		for _, p := range []*pipeline.Pipeline{base, patched} {
			for name, s := range p.Steps {
				if s.Action == ref || (s.Fallback != nil && s.Fallback.Action == ref) {
					set[name] = true
				}
			}
		}
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}
