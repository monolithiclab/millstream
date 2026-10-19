package cli

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/backtest"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/proposals"
	"github.com/monolithiclab/millstream/internal/store"
)

// ProposalsCmd is "millstream proposals" (spec section 11.3).
type ProposalsCmd struct {
	List       ProposalsListCmd       `cmd:"" default:"withargs" help:"List proposals (the default)."`
	Show       ProposalsShowCmd       `cmd:"" help:"Show a proposal: its patch and its validation report."`
	Create     ProposalsCreateCmd     `cmd:"" help:"Propose a patch of a pipeline's package; it is validated before it is ready."`
	Accept     ProposalsAcceptCmd     `cmd:"" help:"Accept a ready proposal on its base revision."`
	Reject     ProposalsRejectCmd     `cmd:"" help:"Reject a proposal with a reason, given to later drafts."`
	Revalidate ProposalsRevalidateCmd `cmd:"" help:"Validate a proposal again; a stale one is rebased onto the active revision first."`
	Token      ProposalsTokenCmd      `cmd:"" help:"Mint the proposals-only API token of the improve package, printed once."`
}

// proposalOptions are what an engine validates proposals with: the evals of the affected actions run as eval runs
// them, with the launchers, secrets and prices of the environment, and the backtests run by bt.
func proposalOptions(rt *Runtime, env *pipeline.Environment, bt backtest.Runner) engine.ProposalOptions {
	return engine.ProposalOptions{Evals: proposals.Evals{Launchers: launchers(rt, env), Prices: env.Prices,
		Env: func(name string) func(string) (string, bool) { return env.SecretEnvLookup(name, rt.Env) }},
		Backtest: backtest.ForProposals(bt, env.Improve.FilterMaxFalseDrop)}
}

// withProposalEngine runs fn with an engine over a store written from this process, as apply builds it.
func withProposalEngine(rt *Runtime, st store.Store, fn func(*engine.Engine) error) error {
	env, arts, err := localArtifacts(rt)
	if err != nil {
		return err
	}
	bt := newBacktester(rt, env, st, arts, env.EffectiveMode(pipeline.ModeDevelopment), nil)
	e, err := engine.New(engine.Options{Store: st, Env: rt.Env, Secrets: secrets(rt, env),
		Resolver: resolver(rt, env, false), Check: checkOptions(rt, env), PackageDir: packageDir(rt, env),
		Agents: agents(rt, env), Proposals: proposalOptions(rt, env, bt)})
	if err != nil {
		return err
	}
	return fn(e)
}

// ProposalsListCmd is "millstream proposals [list]".
type ProposalsListCmd struct {
	Pipeline string `help:"Only this pipeline's proposals."`
	State    string `enum:"drafting,validating,ready,accepted,rejected,stale,all" default:"all" help:"Only proposals in this state."`
	JSON     bool   `name:"json" help:"Print each proposal as a JSON line."`
}

// Run lists proposals, newest first; a rejected one shows its reason.
func (c ProposalsListCmd) Run(rt *Runtime) error {
	f := store.ProposalFilter{Pipeline: c.Pipeline}
	if c.State != "all" {
		f.State = store.ProposalState(c.State)
	}
	return withStore(rt, func(st store.Store) error {
		ps, err := st.Proposals(rt.Ctx, f)
		if err != nil {
			return err
		}
		if c.JSON {
			enc := json.NewEncoder(rt.IO.Out)
			for _, p := range ps {
				if err := enc.Encode(api.ToProposal(p)); err != nil {
					return err
				}
			}
			return nil
		}
		tw := tabwriter.NewWriter(rt.IO.Out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tPIPELINE\tKIND\tSTATE\tSTEPS\tPATHS\tREASON")
		for _, p := range ps {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", p.ID, p.Pipeline, p.Kind, p.State, dash(p.Steps),
				dash(p.Paths), cmp.Or(p.Reason, "-"))
		}
		return tw.Flush()
	})
}

func dash(l []string) string { return cmp.Or(strings.Join(l, ","), "-") }

// ProposalsShowCmd is "millstream proposals show <id>".
type ProposalsShowCmd struct {
	ID   string `arg:"" help:"The proposal."`
	JSON bool   `name:"json" help:"Print the proposal as JSON."`
}

// Run prints a proposal, its validation summarised, then its patch.
func (c ProposalsShowCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st store.Store) error {
		p, err := st.Proposal(rt.Ctx, c.ID)
		if err != nil {
			return err
		}
		if c.JSON {
			return json.NewEncoder(rt.IO.Out).Encode(api.ToProposal(p))
		}
		return printProposal(rt.IO.Out, api.ToProposal(p), true)
	})
}

// printProposal prints a proposal's summary line, and with full its details, validation and patch.
func printProposal(w io.Writer, p api.Proposal, full bool) error {
	if _, err := fmt.Fprintf(w, "%s %s %s on %s (base %.12s, patch %.12s): %s\n", p.ID, p.Kind, p.State, p.Pipeline,
		p.BaseRevision, p.PatchSHA256, validationSummary(p.Report)); err != nil || !full {
		return err
	}
	fmt.Fprintf(w, "paths: %s\nsteps: %s\ninsights: %s\ncreated by %s\n", dash(p.Paths), dash(p.Steps),
		dash(p.Insights), p.CreatedBy)
	if p.Rationale != "" {
		fmt.Fprintf(w, "rationale: %s\n", p.Rationale)
	}
	if p.DecidedBy != "" {
		fmt.Fprintf(w, "decided by %s", p.DecidedBy)
		if p.Reason != "" {
			fmt.Fprintf(w, ": %s", p.Reason)
		}
		fmt.Fprintln(w)
	}
	_, err := fmt.Fprintf(w, "\n%s", p.Patch)
	return err
}

// validationSummary sums a validation report up in a line.
func validationSummary(raw json.RawMessage) string {
	var r struct {
		proposals.Report
		Error string `json:"error"`
	}
	switch {
	case len(raw) == 0 || string(raw) == "null" || json.Unmarshal(raw, &r) != nil:
		return "not validated"
	case r.Error != "":
		return "validation failed to run: " + r.Error
	}
	verdict := map[bool]string{true: "passed", false: "failed"}
	parts := []string{"check " + verdict[r.Check.Passed]}
	if r.Check.Passed {
		ok := 0
		for _, e := range r.Evals {
			if e.Passed {
				ok++
			}
		}
		parts = append(parts, fmt.Sprintf("evals %d/%d passed", ok, len(r.Evals)))
		if len(r.NewEvals) > 0 {
			parts = append(parts, plural(len(r.NewEvals), "new eval"))
		}
		bt := "backtest " + verdict[r.Backtest.Passed]
		if r.Backtest.Skipped != "" {
			bt += " (" + r.Backtest.Skipped + ")"
		}
		parts = append(parts, bt)
	}
	return "validation " + verdict[r.Passed] + ": " + strings.Join(parts, ", ")
}

// ProposalsCreateCmd is "millstream proposals create <patch>".
type ProposalsCreateCmd struct {
	Patch       string   `arg:"" type:"existingfile" help:"The patch: a unified diff of the package, as git diff writes it."`
	Pipeline    string   `required:"" help:"The pipeline whose active revision it patches."`
	Kind        string   `required:"" enum:"config,prompt,lessons,topology,distill" help:"What it changes: config, prompt, lessons, topology or distill."`
	Rationale   string   `help:"Why."`
	Insight     []string `help:"The id of an insight it answers (repeatable)."`
	Fingerprint string   `help:"Names the idea, whose rejection suppresses it (default: from the pipeline, kind, insights and paths)."`
	reviewerFlag
}

// Run creates the proposal and validates it, through the served engine's API with --server, else from this process,
// and prints it with its validation.
func (c ProposalsCreateCmd) Run(rt *Runtime) error {
	patch, err := os.ReadFile(c.Patch)
	if err != nil {
		return err
	}
	by, err := c.reviewer(rt)
	if err != nil {
		return err
	}
	var p api.Proposal
	err = changeState(rt, func(cl *apiClient) error {
		cl.user = by
		_, err := cl.post(rt.Ctx, "/proposals", api.ProposalRequest{Pipeline: c.Pipeline, Kind: c.Kind,
			Patch: string(patch), Rationale: c.Rationale, Insights: c.Insight, Fingerprint: c.Fingerprint}, &p)
		return err
	}, func(st store.Store) error {
		return withProposalEngine(rt, st, func(e *engine.Engine) error {
			sp, err := e.CreateProposal(rt.Ctx, engine.ProposalRequest{Pipeline: c.Pipeline,
				Kind: store.ProposalKind(c.Kind), Patch: string(patch), Rationale: c.Rationale, Insights: c.Insight,
				Fingerprint: c.Fingerprint, By: by})
			p = api.ToProposal(sp)
			return err
		})
	})
	if err != nil {
		return err
	}
	return printProposal(rt.IO.Out, p, false)
}

// ProposalsAcceptCmd is "millstream proposals accept <id>".
type ProposalsAcceptCmd struct {
	ID          string `arg:"" help:"The proposal."`
	PatchSHA256 string `name:"patch-sha256" help:"The hash of the patch you reviewed, as proposals show prints it (default: its current one, printed)."`
	reviewerFlag
}

// Run accepts the proposal: only while its base is the pipeline's active revision, which makes the open proposals
// it overlaps stale; on a moved base the proposal becomes stale and the command fails.
func (c ProposalsAcceptCmd) Run(rt *Runtime) error {
	by, err := c.reviewer(rt)
	if err != nil {
		return err
	}
	sha := c.PatchSHA256 // empty: the current hash, read where the decision goes, and printed with the outcome
	var p api.Proposal
	err = changeState(rt, func(cl *apiClient) error {
		cl.user = by
		if sha == "" {
			var cur api.Proposal
			if _, err := cl.do(rt.Ctx, "GET", "/proposals/"+url.PathEscape(c.ID), nil, &cur); err != nil {
				return err
			}
			sha = cur.PatchSHA256
		}
		_, err := cl.post(rt.Ctx, "/proposals/"+url.PathEscape(c.ID)+"/accept", api.ProposalDecision{PatchSHA256: sha}, &p)
		return err
	}, func(st store.Store) error {
		if sha == "" {
			cur, err := st.Proposal(rt.Ctx, c.ID)
			if err != nil {
				return err
			}
			sha = cur.PatchSHA256
		}
		return withProposalEngine(rt, st, func(e *engine.Engine) error {
			sp, err := e.AcceptProposal(rt.Ctx, c.ID, sha, by)
			p = api.ToProposal(sp)
			return err
		})
	})
	if err != nil {
		return err
	}
	return printProposal(rt.IO.Out, p, false)
}

// ProposalsRejectCmd is "millstream proposals reject <id> --reason".
type ProposalsRejectCmd struct {
	ID     string `arg:"" help:"The proposal."`
	Reason string `required:"" help:"Why: recorded and given to later drafts."`
	reviewerFlag
}

// Run rejects the proposal; its fingerprint is suppressed until an insight it answers doubles in severity.
func (c ProposalsRejectCmd) Run(rt *Runtime) error {
	by, err := c.reviewer(rt)
	if err != nil {
		return err
	}
	var p api.Proposal
	err = changeState(rt, func(cl *apiClient) error {
		cl.user = by
		_, err := cl.post(rt.Ctx, "/proposals/"+url.PathEscape(c.ID)+"/reject", api.ProposalDecision{Reason: c.Reason}, &p)
		return err
	}, func(st store.Store) error {
		sp, err := st.RejectProposal(rt.Ctx, c.ID, c.Reason, by)
		p = api.ToProposal(sp)
		return err
	})
	if err != nil {
		return err
	}
	return printProposal(rt.IO.Out, p, false)
}

// ProposalsRevalidateCmd is "millstream proposals revalidate <id>".
type ProposalsRevalidateCmd struct {
	ID string `arg:"" help:"The proposal."`
}

// Run validates the proposal again, rebasing a stale one onto the active revision first.
func (c ProposalsRevalidateCmd) Run(rt *Runtime) error {
	var p api.Proposal
	err := changeState(rt, func(cl *apiClient) error {
		_, err := cl.post(rt.Ctx, "/proposals/"+url.PathEscape(c.ID)+"/revalidate", struct{}{}, &p)
		return err
	}, func(st store.Store) error {
		return withProposalEngine(rt, st, func(e *engine.Engine) error {
			sp, err := e.ValidateProposal(rt.Ctx, c.ID)
			p = api.ToProposal(sp)
			return err
		})
	})
	if err != nil {
		return err
	}
	return printProposal(rt.IO.Out, p, false)
}

// ProposalsTokenCmd is "millstream proposals token".
type ProposalsTokenCmd struct{}

// Run mints the proposals-only token, replacing the earlier one, and prints it once: the store keeps its hash only.
func (ProposalsTokenCmd) Run(rt *Runtime) error {
	var token string
	err := changeState(rt, func(cl *apiClient) error {
		var t api.ProposalToken
		_, err := cl.post(rt.Ctx, "/proposal-token", struct{}{}, &t)
		token = t.Token
		return err
	}, func(st store.Store) error {
		return withProposalEngine(rt, st, func(e *engine.Engine) error {
			var err error
			token, err = e.MintProposalToken(rt.Ctx)
			return err
		})
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(rt.IO.Out, token)
	return err
}
