package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/evidence"
	"github.com/monolithiclab/millstream/internal/patch"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// BacktestCmd is "millstream backtest".
type BacktestCmd struct {
	Patch    string `arg:"" type:"existingfile" help:"The patch: a unified diff against a revision's package, its base named by a Millstream-Base trailer."`
	Pipeline string `help:"The pipeline (default: the one with the patch's base revision, else the only one)."`
	Sample   int    `help:"Past deliveries to sample (default: improve.backtest.sample, else 200)."`
	JSON     bool   `name:"json" help:"Print the report as JSON."`
}

// newBacktester is the Backtester of run, serve and the backtest command, over the deployment's store, artifact
// store and settings, running actions as the embedded node does.
func newBacktester(rt *Runtime, env *pipeline.Environment, st store.Store, arts artifacts.Store, mode string,
	log *slog.Logger,
) *backtest.Backtester {
	return backtest.New(backtest.Options{Store: st, Artifacts: arts, Mode: mode, Improve: env.Improve,
		Resolver: resolver(rt, env, false), Launchers: launchers(rt, env), Agents: agents(rt, env), Env: rt.Env,
		Secrets: secrets(rt, env), Check: checkOptions(rt, env), Logger: log})
}

// Run backtests the patch from this process, reading the store, and prints the report: a patch without a base
// trailer applies to the pipeline's active revision.
func (c BacktestCmd) Run(rt *Runtime) error {
	data, err := os.ReadFile(c.Patch)
	if err != nil {
		return err
	}
	p, err := patch.Parse(data)
	if err != nil {
		return err
	}
	env, arts, err := localArtifacts(rt)
	if err != nil {
		return err
	}
	return withStore(rt, func(st store.Store) error {
		name, base, err := backtestTarget(rt, st, c.Pipeline, p.Base)
		if err != nil {
			return err
		}
		b := newBacktester(rt, env, st, arts, env.EffectiveMode(pipeline.ModeDevelopment), nil)
		rep, err := b.Run(rt.Ctx, backtest.Request{Pipeline: name, BaseRevision: base, Patch: data, Sample: c.Sample})
		if err != nil {
			return err
		}
		if c.JSON {
			enc := json.NewEncoder(rt.IO.Out)
			enc.SetIndent("", "  ")
			return enc.Encode(rep)
		}
		return printBacktest(rt.IO.Out, rep)
	})
}

// backtestTarget finds the pipeline and base revision: the base the patch states, in the pipeline named or the one
// holding it; without one, the active revision of the pipeline named or of the only one.
func backtestTarget(rt *Runtime, st store.Store, name, base string) (string, string, error) {
	names := []string{name}
	if name == "" {
		ps, err := st.Pipelines(rt.Ctx)
		if err != nil {
			return "", "", err
		}
		names = names[:0]
		for _, p := range ps {
			names = append(names, p.Name)
		}
	}
	if base == "" {
		if len(names) != 1 {
			return "", "", usageError("the patch names no base revision: name the pipeline with --pipeline")
		}
		id, err := st.ActiveRevision(rt.Ctx, names[0])
		if err != nil {
			return "", "", err
		}
		rev, err := st.Revision(rt.Ctx, id)
		return names[0], rev.PackageHash, err
	}
	var holders []string
	for _, n := range names {
		revs, err := st.Revisions(rt.Ctx, n)
		if err != nil {
			return "", "", err
		}
		if slices.ContainsFunc(revs, func(r store.Revision) bool { return r.PackageHash == base }) {
			holders = append(holders, n)
		}
	}
	switch len(holders) {
	case 0:
		return "", "", fmt.Errorf("no pipeline has revision %.12s: %w", base, store.ErrNotFound)
	case 1:
		return holders[0], base, nil
	}
	return "", "", usageError(fmt.Sprintf("pipelines %s hold revision %.12s: name one with --pipeline",
		strings.Join(holders, ", "), base))
}

// printBacktest prints a report: the sample, then before and after side by side, overall and per changed step.
func printBacktest(out io.Writer, r *backtest.Report) error {
	var strata []string
	for _, s := range evidence.Strata {
		if n := r.Strata[s]; n > 0 {
			strata = append(strata, fmt.Sprintf("%s %d", s, n))
		}
	}
	fmt.Fprintf(out, "backtest of %s: patch %.12s, base %.12s, patched %.12s\n", r.Pipeline, r.PatchHash,
		r.BaseRevision, r.PatchedRevision)
	fmt.Fprintf(out, "%d deliveries sampled from %s history (%s), %s replayed; changed: %s\n", r.Sampled,
		r.Source, strings.Join(strata, ", "), plural(r.Runs, "run"), strings.Join(r.ChangedSteps, ", "))
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "\tbefore\tafter\t")
	printMetrics(w, "overall", r.Before, r.After)
	for _, s := range r.Steps {
		printMetrics(w, s.Step, s.Before, s.After)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(out, "cost %.4f USD\n", r.CostUSD)
	for _, warning := range r.Warnings {
		fmt.Fprintf(out, "warning: %s\n", warning)
	}
	return nil
}

// printMetrics prints one scope's rows.
func printMetrics(w io.Writer, scope string, b, a backtest.Metrics) {
	pct := func(v float64, n int) string {
		if n == 0 {
			return "-"
		}
		return fmt.Sprintf("%.0f%% of %d", 100*v, n)
	}
	fmt.Fprintf(w, "%s\t\t\t\n", scope)
	fmt.Fprintf(w, "  agreement\t%s\t%s\t\n", pct(b.Agreement, b.Compared), pct(a.Agreement, a.Compared))
	fmt.Fprintf(w, "  review agreement\t%s\t%s\t\n", pct(b.ReviewAgreement, b.Reviewed),
		pct(a.ReviewAgreement, a.Reviewed))
	fmt.Fprintf(w, "  failure rate\t%s\t%s\t\n", pct(b.FailureRate, b.Ran), pct(a.FailureRate, a.Ran))
	fmt.Fprintf(w, "  newly reached\t%d\t%d\t\n", b.NewlyReached, a.NewlyReached)
	fmt.Fprintf(w, "  no longer reached\t%d\t%d\t\n", b.NoLongerReached, a.NoLongerReached)
	fmt.Fprintf(w, "  cost\t%.4f USD\t%.4f USD\t\n", b.CostUSD, a.CostUSD)
	fmt.Fprintf(w, "  latency\t%.0f ms\t%.0f ms\t\n", b.LatencyMs, a.LatencyMs)
}
