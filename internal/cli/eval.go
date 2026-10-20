package cli

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"text/tabwriter"

	"github.com/monolithiclab/millstream/internal/agent"
	"github.com/monolithiclab/millstream/internal/eval"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// EvalCmd is "millstream eval".
type EvalCmd struct {
	Action      string `arg:"" type:"path" help:"The action directory, holding evals/*.yaml."`
	Package     string `type:"path" help:"The package directory (default: the nearest directory above the action holding pipeline.yaml)."`
	HarnessMode string `name:"harness-mode" enum:"replay,record,live" default:"replay" help:"Harness mode of the agent's and the judge's calls: replay, record or live."`
	HarnessDir  string `name:"harness-dir" type:"path" help:"Recordings directory (default: <action>/evals/recordings)."`
	Case        string `help:"Run only this case."`
}

// Run runs the action's eval cases and prints a score per case and the total, with the previous score of each case
// when --db is given, which also records the scores. It fails when an assertion does not hold or a case cannot be
// scored.
func (c EvalCmd) Run(rt *Runtime) error {
	cases, err := eval.Load(c.Action)
	if err != nil {
		return err
	}
	if c.Case != "" {
		cases = selectCase(cases, c.Case)
	}
	if len(cases) == 0 {
		return usageError(fmt.Sprintf("no eval case to run in %s", filepath.Join(c.Action, eval.Dir)))
	}
	opts, err := c.options(rt)
	if err != nil {
		return err
	}
	results := make([]eval.Result, len(cases))
	for i, ec := range cases {
		results[i] = eval.Run(rt.Ctx, opts, ec)
	}
	var previous map[string]float64
	record, err := recordsEvals(rt)
	if err != nil {
		return err
	}
	if record {
		if previous, err = recordEval(rt, opts, results); err != nil {
			return err
		}
	}
	if err := printEval(rt, results, previous); err != nil {
		return err
	}
	for _, r := range results {
		if !r.Passes() {
			return errFailed
		}
	}
	return nil
}

func selectCase(cases []*eval.Case, name string) []*eval.Case {
	for _, ec := range cases {
		if ec.Name == name {
			return []*eval.Case{ec}
		}
	}
	return nil
}

// options resolves the action in its package and the harness settings.
func (c EvalCmd) options(rt *Runtime) (eval.Options, error) {
	opts, err := actionOptions(rt, c.Action, c.Package, false)
	if err != nil {
		return eval.Options{}, err
	}
	opts.Mode, opts.Dir = c.HarnessMode, harnessDir(c.HarnessMode, c.HarnessDir, c.Action, eval.RecordingsDir)
	return opts, nil
}

// harnessDir is the recordings directory of an agent's harness calls: dir when given, else rel under the action
// directory; none in live mode.
func harnessDir(mode, dir, action, rel string) string {
	switch {
	case mode == agent.ModeLive:
		return ""
	case dir != "":
		return dir
	}
	return filepath.Join(action, filepath.FromSlash(rel))
}

// actionOptions resolves the action directory dir in its package, pkgDir or else the nearest directory above dir
// holding a pipeline file, for running it as eval does. With standalone, an action without a package above runs as
// a lone action (pipeline.LoadAction), which serves exec and container actions; an agent reads its package.
func actionOptions(rt *Runtime, dir, pkgDir string, standalone bool) (eval.Options, error) {
	env, err := environment(rt)
	if err != nil {
		return eval.Options{}, err
	}
	pkg, ref, err := actionPackage(dir, pkgDir, standalone)
	if err != nil {
		return eval.Options{}, err
	}
	act, err := resolver(rt, env, false).Resolve(pkg, ref)
	if err != nil {
		return eval.Options{}, err
	}
	if act.Kind == pipeline.KindAgent && pkg.PipelineFile == "" {
		return eval.Options{}, usageError(fmt.Sprintf("agent action %s runs in its package: name it with --package", dir))
	}
	opts := eval.Options{
		Package: pkg, Action: act, Ref: ref, Env: env.SecretEnvLookup(pkg.Pipeline.Name, rt.Env),
		Logger: slog.New(slog.NewTextHandler(rt.IO.Err, nil)), Launchers: launchers(rt, env),
		RecordHint: agent.FlagRecordHint, // eval and test action take the mode as a flag
	}
	if act.Agent != nil {
		if price, ok := env.Prices[act.Agent.Model]; ok {
			opts.Price = &price
		}
	}
	return opts, nil
}

// actionPackage loads the package of the action directory dir and returns the reference naming the action in it.
func actionPackage(dir, pkgDir string, standalone bool) (*pipeline.Package, string, error) {
	if pkgDir == "" {
		var err error
		if pkgDir, err = packageAbove(dir); err != nil {
			if standalone && errors.As(err, new(usageError)) {
				return pipeline.LoadAction(dir)
			}
			return nil, "", err
		}
	}
	rel, err := filepath.Rel(pkgDir, dir)
	if err != nil || !filepath.IsLocal(rel) {
		return nil, "", usageError(fmt.Sprintf("action %s is not inside the package %s", dir, pkgDir))
	}
	pkg, err := pipeline.Load(pkgDir)
	if err != nil {
		return nil, "", err
	}
	return pkg, "./" + filepath.ToSlash(rel), nil
}

// packageAbove returns the nearest directory above dir holding a pipeline file.
func packageAbove(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for d := filepath.Dir(abs); ; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, pipeline.FileName)); err == nil {
			return d, nil
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		if d == filepath.Dir(d) {
			return "", usageError(fmt.Sprintf("no %s above %s: name the package with --package", pipeline.FileName, dir))
		}
	}
}

// recordsEvals reports whether eval records its scores: when --db names the database, or the environment file's
// store key names the store.
func recordsEvals(rt *Runtime) (bool, error) {
	if rt.DBSet {
		return true, nil
	}
	env, err := environment(rt)
	if err != nil {
		return false, err
	}
	return env.Store.Set(), nil
}

// recordEval records the scores in the state database and returns each case's previous score.
func recordEval(rt *Runtime, opts eval.Options, results []eval.Result) (map[string]float64, error) {
	previous := map[string]float64{}
	err := withWriter(rt, func(st store.Store) error {
		past, err := st.EvalResults(rt.Ctx, opts.Action.Name)
		if err != nil {
			return err
		}
		for _, p := range past { // newest first: keep the first seen
			if _, ok := previous[p.Case]; !ok {
				previous[p.Case] = p.Score
			}
		}
		digest := opts.Package.ActionDigest(opts.Action.Dir)
		rows := make([]store.EvalResult, len(results))
		for i, r := range results {
			rows[i] = store.EvalResult{Action: opts.Action.Name, ActionDigest: digest, Case: r.Case, Score: r.Score}
		}
		return st.RecordEval(rt.Ctx, rows)
	})
	return previous, err
}

// printEval prints a line per case and the total score, then, per case, the assertions that failed, the judge's
// reason and the error.
func printEval(rt *Runtime, results []eval.Result, previous map[string]float64) error {
	tw := tabwriter.NewWriter(rt.IO.Out, 0, 0, 2, ' ', 0)
	header := "CASE\tASSERTIONS\tJUDGE\tCOST\tSCORE"
	if previous != nil {
		header += "\tPREVIOUS"
	}
	fmt.Fprintln(tw, header)
	var total float64
	for _, r := range results {
		judge := "-"
		if r.Judge != nil {
			judge = score(r.Judge.Score)
		}
		line := fmt.Sprintf("%s\t%d/%d\t%s\t$%s\t%s", r.Case, r.Passed, r.Total, judge,
			strconv.FormatFloat(r.CostUSD, 'f', 4, 64), score(r.Score))
		if previous != nil {
			prev, ok := previous[r.Case]
			if ok {
				line += "\t" + score(prev)
			} else {
				line += "\t-"
			}
		}
		fmt.Fprintln(tw, line)
		total += r.Score
	}
	fmt.Fprintf(tw, "TOTAL\t\t\t\t%s\n", score(total/float64(len(results))))
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, r := range results {
		for _, f := range r.Failed {
			fmt.Fprintf(rt.IO.Out, "%s: failed: %s\n", r.Case, f)
		}
		if r.Judge != nil {
			fmt.Fprintf(rt.IO.Out, "%s: judge: %s\n", r.Case, oneLine(r.Judge.Reason, 1000))
		}
		if r.Err != "" {
			fmt.Fprintf(rt.IO.Out, "%s: error: %s\n", r.Case, oneLine(r.Err, 1000))
		}
	}
	return nil
}

func score(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
