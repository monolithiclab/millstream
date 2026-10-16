package cli

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
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
	var id string
	var fired bool
	err := changeState(rt, func(cl *apiClient) error {
		if len(c.Var) > 0 {
			return errNoVarsRemote
		}
		var run api.Run
		status, err := cl.post(rt.Ctx, pipelinePath(c.Pipeline, "runs"), api.TriggerRequest{Trigger: c.Trigger,
			Payload: json.RawMessage(c.Payload), RequestID: c.RequestID}, &run)
		id, fired = run.ID, created(status)
		return err
	}, func(st store.Store) error {
		if err := checkFlagsDeclared(rt, st, c.Pipeline, c.Var); err != nil {
			return err
		}
		env, err := environment(rt)
		if err != nil {
			return err
		}
		e, err := engine.New(engine.Options{Store: st, Env: rt.Env, Secrets: secrets(rt, env), Flags: c.Var})
		if err != nil {
			return err
		}
		id, fired, err = e.Fire(rt.Ctx, c.Pipeline, c.Trigger, c.RequestID, json.RawMessage(c.Payload))
		return err
	})
	if err != nil {
		return err
	}
	suffix := ""
	if !fired {
		suffix = " (already fired)"
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s%s\n", id, suffix)
	return err
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
	err := changeState(rt, func(cl *apiClient) error {
		_, err := cl.post(rt.Ctx, pipelinePath(c.Pipeline, "runs", c.RunID, "cancel"), nil, nil)
		return err
	}, func(st store.Store) error { return st.Cancel(rt.Ctx, c.Pipeline, c.RunID) })
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s cancelled\n", c.RunID)
	return err
}

// ReplayCmd is "millstream replay": dead-letter replay into the origin run (--from dlq, spec section 5.6), a replay
// run of a step over past items (--step, spec section 5.7), or a back-fill of an imported stream over the producer's
// retained items (--stream, spec section 4.12).
type ReplayCmd struct {
	From      string            `placeholder:"dlq" help:"Replay dead letters into their run: dlq. Exclusive with --step and --stream."`
	Step      string            `help:"Start a replay run of this step over past items on its input streams."`
	Stream    string            `help:"Start a replay run back-filling this imported stream from the producer's retained items on it."`
	Pipeline  string            `required:"" help:"Pipeline name."`
	RunID     string            `name:"run" help:"With --from dlq, the run whose dead letters to replay; with --step, replay only this run's items."`
	Migrate   bool              `help:"With --from dlq, move the run to the pipeline's active revision first."`
	Since     string            `help:"With --step or --stream, replay only the items published within this duration (7d, 12h)."`
	Where     string            `help:"With --step or --stream, replay only the items for which this CEL expression holds."`
	Revision  int64             `help:"With --step or --stream, the revision to run on (default: the active one)."`
	RequestID string            `name:"request-id" help:"With --step or --stream, the request id naming the replay run; repeating it starts nothing new."`
	Var       map[string]string `help:"With --step or --stream, set a variable (name=value)."`
	IDs       []string          `arg:"" optional:"" name:"id" help:"With --from dlq, the dead letters to replay; all of the run's open ones when none."`
}

// Run replays the run's open dead letters, reopening it, or starts a replay run and prints its id; a running engine
// dispatches the deliveries either creates.
func (c ReplayCmd) Run(rt *Runtime) error {
	switch {
	case len(slices.DeleteFunc([]string{c.From, c.Step, c.Stream}, func(s string) bool { return s == "" })) > 1:
		return usageError("--from, --step and --stream are mutually exclusive")
	case c.From == "dlq":
		return c.deadLetters(rt)
	case c.From != "":
		return usageError(fmt.Sprintf("--from %s: only dlq is supported", c.From))
	case c.Step != "", c.Stream != "":
		return c.replayRun(rt)
	default:
		return usageError("replay needs --from dlq, --step or --stream")
	}
}

func (c ReplayCmd) deadLetters(rt *Runtime) error {
	switch {
	case c.RunID == "":
		return usageError("--from dlq needs --run")
	case c.Since != "" || c.Where != "" || c.Revision != 0 || c.RequestID != "" || len(c.Var) > 0:
		return usageError("--since, --where, --revision, --request-id and --var go with --step or --stream, not --from dlq")
	}
	var n int
	err := changeState(rt, func(cl *apiClient) error {
		ids := c.IDs
		if len(ids) == 0 { // the run's open dead letters, as the store's unit takes them
			var groups []api.DeadLetterGroup
			if _, err := cl.do(rt.Ctx, http.MethodGet, pipelinePath(c.Pipeline, "dead-letters"), nil, &groups); err != nil {
				return err
			}
			for _, g := range groups {
				for _, dl := range g.DeadLetters {
					if dl.RunID == c.RunID {
						ids = append(ids, dl.ID)
					}
				}
			}
			if len(ids) == 0 {
				return nil
			}
		}
		var count api.Count
		_, err := cl.post(rt.Ctx, pipelinePath(c.Pipeline, "dead-letters", "replay"),
			api.DeadLetterSelection{IDs: ids, Migrate: c.Migrate}, &count)
		n = count.Count
		return err
	}, func(st store.Store) error {
		var err error
		n, err = st.ReplayDeadLetters(rt.Ctx, c.Pipeline, c.RunID, c.IDs, c.Migrate)
		return err
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s replayed\n", plural(n, "dead letter"))
	return err
}

func (c ReplayCmd) replayRun(rt *Runtime) error {
	switch {
	case c.Migrate || len(c.IDs) > 0:
		return usageError("--migrate and dead-letter ids go with --from dlq, not --step or --stream")
	case c.Stream != "" && c.RunID != "":
		return usageError("--run goes with --step or --from dlq: a back-fill selects by --since and --where")
	}
	var since time.Duration
	if c.Since != "" {
		var err error
		if since, err = pipeline.ParseDuration(c.Since); err != nil {
			return usageError("--since: " + err.Error())
		}
	}
	var id string
	var started bool
	err := changeState(rt, func(cl *apiClient) error {
		if len(c.Var) > 0 {
			return errNoVarsRemote
		}
		var run api.Run
		status, err := cl.post(rt.Ctx, pipelinePath(c.Pipeline, "replay"), api.ReplayRequest{Step: c.Step,
			Since: c.Since, Run: c.RunID, Where: c.Where, Revision: c.Revision, RequestID: c.RequestID}, &run)
		id, started = run.ID, created(status)
		return err
	}, func(st store.Store) error {
		if err := checkFlagsDeclared(rt, st, c.Pipeline, c.Var); err != nil {
			return err
		}
		env, err := environment(rt)
		if err != nil {
			return err
		}
		e, err := engine.New(engine.Options{Store: st, Env: rt.Env, Secrets: secrets(rt, env), Flags: c.Var})
		if err != nil {
			return err
		}
		id, started, err = e.Replay(rt.Ctx, engine.ReplayRequest{
			Pipeline: c.Pipeline, Step: c.Step, Stream: c.Stream, Revision: store.RevisionID(c.Revision), Since: since,
			RunID: c.RunID,
			Where: c.Where, RequestID: c.RequestID,
		})
		return err
	})
	if err != nil {
		return err
	}
	suffix := ""
	if !started {
		suffix = " (already started)"
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s%s\n", id, suffix)
	return err
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
	return withStore(rt, func(st store.Store) error {
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
	var n int
	err := changeState(rt, func(cl *apiClient) error {
		var count api.Count
		_, err := cl.post(rt.Ctx, pipelinePath(c.Pipeline, "dead-letters", "discard"), api.DeadLetterSelection{IDs: c.IDs},
			&count)
		n = count.Count
		return err
	}, func(st store.Store) error {
		var err error
		n, err = st.DiscardDeadLetters(rt.Ctx, c.Pipeline, c.IDs)
		return err
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s discarded\n", plural(n, "dead letter"))
	return err
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

// Run resumes the pipeline or step; resuming a disabled pipeline enables it.
func (c ResumeCmd) Run(rt *Runtime) error { return setPaused(rt, c.Pipeline, c.Step, false) }

func setPaused(rt *Runtime, pipelineName, step string, paused bool) error {
	what, verb := pipelineName, "resume"
	if paused {
		verb = "pause"
	}
	err := changeState(rt, func(cl *apiClient) error {
		path := pipelinePath(pipelineName, verb)
		if step != "" {
			path = pipelinePath(pipelineName, "steps", step, verb)
		}
		_, err := cl.post(rt.Ctx, path, nil, nil)
		return err
	}, func(st store.Store) error { return st.SetPaused(rt.Ctx, pipelineName, step, paused) })
	if err != nil {
		return err
	}
	if step != "" {
		what += "." + step
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s %sd\n", what, verb)
	return err
}
