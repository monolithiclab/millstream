// Package proposals holds the engine's proposal rules (spec section 11.3): a proposal's patch applied to its base
// revision's files (ApplyPatch), the paths each kind may touch (spec section 11.7), the steps a patch touches, the
// fingerprint a rejection suppresses, and the validation the engine runs before a proposal is ready: check on the
// patched package, the base revision's evals before and after, and a backtest.
//
// The store's units keep the states (store.Store's CreateProposal through RejectProposal); the engine
// (engine/proposals.go) calls this package before them.
package proposals

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Prepared is a patch checked against its kind's path rules and applied to its base: what a proposal records.
type Prepared struct {
	Applied
	// Patch is the patch as given, which SHA256 hashes.
	Patch string
	// Base and Patched are the packages before and after the patch, their Dir empty: nothing is on disk yet.
	Base, Patched *pipeline.Package
	SHA256        string
	Steps         []string
}

// Prepare applies a patch to the base package's files, refusing any path its kind does not allow (spec section
// 11.7), and derives the steps it touches. It is called when a proposal is drafted, when it is rebased and again when
// it is accepted. The patched package must parse; check is the validation's.
func Prepare(kind store.ProposalKind, base *pipeline.Package, patch string) (Prepared, error) {
	if !slices.Contains(store.ProposalKinds, kind) {
		return Prepared{}, fmt.Errorf("%w: kind %q is not one of %v", ErrPatch, kind, store.ProposalKinds)
	}
	a, err := ApplyPatch(base.Files, patch)
	if err != nil {
		return Prepared{}, err
	}
	if err := allowed(kind, base, a.Changes); err != nil {
		return Prepared{}, err
	}
	patched, err := pipeline.FromFiles("", a.Files)
	if err != nil {
		return Prepared{}, fmt.Errorf("%w: the patched package does not parse: %w", ErrPatch, err)
	}
	return Prepared{Applied: a, Patch: patch, Base: base, Patched: patched, SHA256: Hash(patch),
		Steps: touchedSteps(base.Pipeline, patched.Pipeline, a.Changes)}, nil
}

// Hash is a patch's content hash: the SHA-256 of its text, hex.
func Hash(patch string) string {
	sum := sha256.Sum256([]byte(patch))
	return hex.EncodeToString(sum[:])
}

// Fingerprint identifies a proposal's idea when its drafter names none: its pipeline, kind, the insights it answers
// and the paths it touches. A rejection suppresses it (spec section 11.3).
func Fingerprint(pipelineName string, kind store.ProposalKind, insights, paths []string) string {
	ins := slices.Sorted(slices.Values(insights))
	ps := slices.Sorted(slices.Values(paths))
	return Hash(strings.Join([]string{pipelineName, string(kind), strings.Join(ins, ","), strings.Join(ps, ",")},
		"\x00"))[:32]
}

// allowed checks each change against its kind's paths (spec section 11.7):
//
//	lessons            lessons/<step>.yaml, for a step of the base revision
//	prompt             actions/<action>/prompt.md, for an action directory of the base revision, never deleted
//	config, topology   pipeline.yaml, changed
//	distill            pipeline.yaml changed, and files added under one new actions/<name>/ directory
func allowed(kind store.ProposalKind, base *pipeline.Package, changes []Change) error {
	newDir := ""
	for _, c := range changes {
		refuse := func(why string) error {
			return fmt.Errorf("%w: a %s proposal may not %s %s: %s", ErrPatch, kind, c.Op, c.Path, why)
		}
		if c.Path == pipeline.FileName {
			if c.Op != OpModify {
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
			if c.Op == OpDelete {
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
			case c.Op != OpAdd:
				return refuse("the new action's files are added")
			}
			newDir = dir
		default:
			return refuse("config and topology proposals change pipeline.yaml alone")
		}
	}
	if kind == store.ProposalDistill && newDir == "" {
		return fmt.Errorf("%w: a distill proposal adds a new actions/<name>/ directory", ErrPatch)
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
func touchedSteps(base, patched *pipeline.Pipeline, changes []Change) []string {
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
