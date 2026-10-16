package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// StatusCmd is "millstream status".
type StatusCmd struct {
	Pipeline string `help:"Only this pipeline."`
	Limit    int    `default:"10" help:"Most recent runs to show."`
}

// Run prints the pipelines with their state, active revision and paused steps (with the budget that paused one,
// when a budget did), then the most recent runs with their state, item
// count and open dead letters.
func (c StatusCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st store.Store) error {
		pipelines, err := st.Pipelines(rt.Ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(rt.IO.Out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "PIPELINE\tSTATE\tREVISION\tPAUSED STEPS")
		for _, ps := range pipelines {
			if c.Pipeline != "" && ps.Name != c.Pipeline {
				continue
			}
			state, paused := "active", "-"
			switch {
			case ps.Paused:
				state = "paused"
			case ps.Disabled:
				state = "disabled"
			}
			if len(ps.PausedSteps) > 0 {
				steps := make([]string, len(ps.PausedSteps))
				for i, step := range ps.PausedSteps {
					steps[i] = step
					if cause := ps.PauseCauses[step]; cause != "" {
						steps[i] += " (" + cause + ")"
					}
				}
				paused = strings.Join(steps, ",")
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", ps.Name, state, ps.ActiveRevision, paused)
		}
		fmt.Fprintln(tw)
		runs, err := st.Runs(rt.Ctx, store.RunFilter{Pipeline: c.Pipeline, Limit: c.Limit})
		if err != nil {
			return err
		}
		fmt.Fprintln(tw, "RUN\tPIPELINE\tSTATE\tSTARTED\tITEMS\tOPEN DEAD LETTERS")
		for _, r := range runs {
			counts, err := streamCounts(rt.Ctx, st, r)
			if err != nil {
				return err
			}
			dls, err := st.DeadLetters(rt.Ctx, store.DeadLetterFilter{
				Pipeline: r.Pipeline, RunID: r.ID, State: store.DeadLetterOpen,
			})
			if err != nil {
				return err
			}
			var total int64
			for _, n := range counts {
				total += n
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\n", r.ID, r.Pipeline, r.State, r.StartedAt.Format(time.RFC3339),
				total, len(dls))
		}
		return tw.Flush()
	})
}

// streamCounts counts a run's items per stream.
func streamCounts(ctx context.Context, st store.Store, r store.Run) (map[string]int64, error) {
	return st.StreamCounts(ctx, r.Pipeline, r.ID)
}

// TailCmd is "millstream tail".
type TailCmd struct {
	From     int64         `default:"0" help:"Print events after this sequence number."`
	Follow   bool          `short:"f" help:"Keep polling for new events until interrupted."`
	Interval time.Duration `default:"500ms" help:"Polling interval with --follow."`
}

// event is an event as tail prints it, one JSON object per line.
type event struct {
	Seq      int64           `json:"seq"`
	TS       time.Time       `json:"ts"`
	Pipeline string          `json:"pipeline"`
	RunID    string          `json:"runId,omitempty"`
	Kind     string          `json:"kind"`
	Data     json.RawMessage `json:"data"`
}

// Run prints the events after --from and, with --follow, the new ones as they commit, until interrupted.
func (c TailCmd) Run(rt *Runtime) error {
	ctx, stop := signalContext(rt.Ctx)
	defer stop()
	return withStore(rt, func(st store.Store) error {
		enc := json.NewEncoder(rt.IO.Out)
		after := c.From
		for {
			evs, err := st.Events(ctx, after, 1000)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			for _, ev := range evs {
				if err := enc.Encode(event(ev)); err != nil {
					return err
				}
				after = ev.Seq
			}
			if len(evs) == 1000 {
				continue
			}
			if !c.Follow {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(c.Interval):
			}
		}
	})
}

// sortedKeys returns m's keys in order.
func sortedKeys[V any](m map[string]V) []string { return slices.Sorted(maps.Keys(m)) }
