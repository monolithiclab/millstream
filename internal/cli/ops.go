package cli

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// TriggerCmd is "millstream trigger".
type TriggerCmd struct {
	Pipeline  string            `arg:"" help:"Pipeline name."`
	Trigger   string            `arg:"" help:"Trigger name."`
	Payload   string            `default:"{}" help:"Trigger input, as JSON."`
	Var       map[string]string `help:"Set a variable (name=value)."`
	RequestID string            `name:"request-id" help:"Request id of the firing; firing the same id again is a no-op."`
}

// Run fires the trigger of the pipeline's active revision straight into the store: a running engine picks the run's
// deliveries up. It prints the run id.
func (c TriggerCmd) Run(rt *Runtime) error {
	if !json.Valid([]byte(c.Payload)) {
		return usageError("--payload: not valid JSON")
	}
	return withStore(rt, func(st *sqlite.Store) error {
		if err := checkFlagsDeclared(rt, st, c.Pipeline, c.Var); err != nil {
			return err
		}
		e, err := engine.New(engine.Options{Store: st, Env: rt.Env, Flags: c.Var})
		if err != nil {
			return err
		}
		id, created, err := e.Fire(rt.Ctx, c.Pipeline, c.Trigger, c.RequestID, json.RawMessage(c.Payload))
		if err != nil {
			return err
		}
		suffix := ""
		if !created {
			suffix = " (already fired)"
		}
		_, err = fmt.Fprintf(rt.IO.Out, "%s%s\n", id, suffix)
		return err
	})
}

// checkFlagsDeclared fails when a --var names a variable the pipeline's active revision does not declare.
func checkFlagsDeclared(rt *Runtime, st store.Store, pipelineName string, flags map[string]string) error {
	if len(flags) == 0 {
		return nil
	}
	revID, err := st.ActiveRevision(rt.Ctx, pipelineName)
	if err != nil {
		return err
	}
	rev, err := st.Revision(rt.Ctx, revID)
	if err != nil {
		return err
	}
	p, err := rev.Decode()
	if err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(flags)) {
		if _, ok := p.Vars[name]; !ok {
			return usageError(fmt.Sprintf("--var %s: pipeline %s declares no such variable", name, pipelineName))
		}
	}
	return nil
}

// CancelCmd is "millstream cancel".
type CancelCmd struct {
	Pipeline string `arg:"" help:"Pipeline name."`
	RunID    string `arg:"" name:"run" help:"Run id."`
}

// Run cancels the run.
func (c CancelCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st *sqlite.Store) error {
		if err := st.Cancel(rt.Ctx, c.Pipeline, c.RunID); err != nil {
			return err
		}
		_, err := fmt.Fprintf(rt.IO.Out, "%s cancelled\n", c.RunID)
		return err
	})
}

// ReplayCmd is "millstream replay".
type ReplayCmd struct {
	From     string   `enum:"dlq" required:"" help:"What to replay from: dlq."`
	Pipeline string   `required:"" help:"Pipeline name."`
	RunID    string   `name:"run" required:"" help:"Run id."`
	Migrate  bool     `help:"Move the run to the pipeline's active revision first."`
	IDs      []string `arg:"" optional:"" name:"id" help:"Dead letters to replay; all of the run's open ones when none."`
}

// Run replays the run's open dead letters, reopening it; a running engine dispatches what they send back.
func (c ReplayCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st *sqlite.Store) error {
		n, err := st.ReplayDeadLetters(rt.Ctx, c.Pipeline, c.RunID, c.IDs, c.Migrate)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(rt.IO.Out, "%s replayed\n", plural(n, "dead letter"))
		return err
	})
}

// DLQCmd is "millstream dlq".
type DLQCmd struct {
	List    DLQListCmd    `cmd:"" help:"List dead letters."`
	Discard DLQDiscardCmd `cmd:"" help:"Discard dead letters, accepting the loss."`
}

// DLQListCmd is "millstream dlq list".
type DLQListCmd struct {
	Pipeline string `help:"Only this pipeline's dead letters."`
	RunID    string `name:"run" help:"Only this run's dead letters."`
	State    string `enum:"open,replayed,discarded,all" default:"open" help:"State to list: open, replayed, discarded or all."`
}

// Run prints the dead letters as a table.
func (c DLQListCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st *sqlite.Store) error {
		f := store.DeadLetterFilter{Pipeline: c.Pipeline, RunID: c.RunID}
		if c.State != "all" {
			f.State = store.DeadLetterState(c.State)
		}
		dls, err := st.DeadLetters(rt.Ctx, f)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(rt.IO.Out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tPIPELINE\tRUN\tSTEP\tKIND\tSTATE\tERROR")
		for _, dl := range dls {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", dl.ID, dl.Pipeline, dl.RunID, dl.OriginStep, dl.Kind,
				dl.State, oneLine(dl.Error, 80))
		}
		return tw.Flush()
	})
}

// oneLine returns s on one line, cut to at most n runes.
func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}

// DLQDiscardCmd is "millstream dlq discard".
type DLQDiscardCmd struct {
	Pipeline string   `required:"" help:"Pipeline name."`
	IDs      []string `arg:"" name:"id" help:"Dead letters to discard."`
}

// Run discards the dead letters.
func (c DLQDiscardCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st *sqlite.Store) error {
		n, err := st.DiscardDeadLetters(rt.Ctx, c.Pipeline, c.IDs)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(rt.IO.Out, "%s discarded\n", plural(n, "dead letter"))
		return err
	})
}

// PauseCmd is "millstream pause".
type PauseCmd struct {
	Pipeline string `arg:"" help:"Pipeline name."`
	Step     string `arg:"" optional:"" help:"Step name; the whole pipeline when omitted."`
}

// Run pauses the pipeline or step: its deliveries stay pending.
func (c PauseCmd) Run(rt *Runtime) error { return setPaused(rt, c.Pipeline, c.Step, true) }

// ResumeCmd is "millstream resume".
type ResumeCmd struct {
	Pipeline string `arg:"" help:"Pipeline name."`
	Step     string `arg:"" optional:"" help:"Step name; the whole pipeline when omitted."`
}

// Run resumes the pipeline or step.
func (c ResumeCmd) Run(rt *Runtime) error { return setPaused(rt, c.Pipeline, c.Step, false) }

func setPaused(rt *Runtime, pipelineName, step string, paused bool) error {
	return withStore(rt, func(st *sqlite.Store) error {
		if err := st.SetPaused(rt.Ctx, pipelineName, step, paused); err != nil {
			return err
		}
		what, verb := pipelineName, "resumed"
		if step != "" {
			what += "." + step
		}
		if paused {
			verb = "paused"
		}
		_, err := fmt.Fprintf(rt.IO.Out, "%s %s\n", what, verb)
		return err
	})
}
