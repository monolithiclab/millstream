package cli

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/monolithiclab/millstream/internal/insights"
	"github.com/monolithiclab/millstream/internal/metrics"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// MetricsCmd is "millstream metrics".
type MetricsCmd struct {
	Pipeline   string `help:"Only this pipeline's metrics."`
	Name       string `help:"Only this metric."`
	Since      string `default:"1h" help:"The minutes since this long ago (10m, 1h, 7d), or all."`
	Prometheus bool   `help:"Print the Prometheus text exposition, aggregated over the period, instead of the rollups."`
}

// Run prints the one-minute metric rollups the engine's rollup projector wrote (spec section 10.2), or their
// Prometheus exposition.
func (c MetricsCmd) Run(rt *Runtime) error {
	var since time.Time
	if c.Since != "all" {
		d, err := pipeline.ParseDuration(c.Since)
		if err != nil {
			return usageError("--since: " + err.Error())
		}
		since = time.Now().Add(-d)
	}
	return withStore(rt, func(st store.Store) error {
		rollups, err := st.MetricRollups(rt.Ctx, store.MetricFilter{Pipeline: c.Pipeline, Name: c.Name, Since: since})
		if err != nil {
			return err
		}
		if c.Prometheus {
			return metrics.Prometheus(rt.IO.Out, rollups)
		}
		tw := tabwriter.NewWriter(rt.IO.Out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "PIPELINE\tNAME\tLABELS\tMINUTE\tCOUNT\tSUM\tMIN\tMAX")
		for _, r := range rollups {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n", r.Pipeline, r.Name, labels(r.Labels),
				r.Minute.Format("2006-01-02T15:04Z"), r.Count, num(r.Sum), num(r.Min), num(r.Max))
		}
		return tw.Flush()
	})
}

// labels renders a label set as k=v pairs sorted by name, "-" when empty.
func labels(ls map[string]string) string {
	if len(ls) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(ls))
	for _, k := range slices.Sorted(maps.Keys(ls)) {
		parts = append(parts, k+"="+ls[k])
	}
	return strings.Join(parts, ",")
}

func num(v float64) string { return strconv.FormatFloat(v, 'g', 6, 64) }

// InsightsCmd is "millstream insights".
type InsightsCmd struct {
	List InsightsListCmd `cmd:"" default:"withargs" help:"List insights (the default)."`
	Ack  InsightsAckCmd  `cmd:"" help:"Acknowledge an insight: the projector keeps updating it and resolves it when it clears."`
	Mute InsightsMuteCmd `cmd:"" help:"Mute an insight: it stays muted whatever the projector finds."`
}

// InsightsListCmd is "millstream insights [list]".
type InsightsListCmd struct {
	Pipeline string `help:"Only this pipeline's insights."`
	State    string `enum:"open,acknowledged,resolved,muted,all" default:"all" help:"Only insights in this state: open, acknowledged, resolved, muted or all."`
	JSON     bool   `name:"json" help:"Print each insight with its evidence as a JSON line."`
}

// insightJSON is an insight as --json prints it.
type insightJSON struct {
	Pipeline    string          `json:"pipeline"`
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Scope       string          `json:"scope"`
	Severity    string          `json:"severity"`
	State       string          `json:"state"`
	WindowStart time.Time       `json:"windowStart"`
	WindowEnd   time.Time       `json:"windowEnd"`
	OpenedAt    time.Time       `json:"openedAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
	Evidence    json.RawMessage `json:"evidence"`
}

// Run lists the insights the engine's projector recorded (spec section 11.1), most recently updated first.
func (c InsightsListCmd) Run(rt *Runtime) error {
	state := store.InsightState(c.State)
	if c.State == "all" {
		state = ""
	}
	return withStore(rt, func(st store.Store) error {
		ins, err := st.Insights(rt.Ctx, store.InsightFilter{Pipeline: c.Pipeline, State: state})
		if err != nil {
			return err
		}
		if c.JSON {
			enc := json.NewEncoder(rt.IO.Out)
			for _, in := range ins {
				if err := enc.Encode(insightJSON{
					Pipeline: in.Pipeline, ID: in.ID, Kind: in.Kind, Scope: in.Scope, Severity: in.Severity,
					State: string(in.State), WindowStart: in.WindowStart, WindowEnd: in.WindowEnd,
					OpenedAt: in.OpenedAt, UpdatedAt: in.UpdatedAt, Evidence: in.Evidence,
				}); err != nil {
					return err
				}
			}
			return nil
		}
		tw := tabwriter.NewWriter(rt.IO.Out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tPIPELINE\tKIND\tSCOPE\tSEVERITY\tSTATE\tUPDATED\tSUMMARY")
		for _, in := range ins {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", in.ID, in.Pipeline, in.Kind, in.Scope, in.Severity,
				in.State, in.UpdatedAt.Format(time.RFC3339), insights.Summary(in))
		}
		return tw.Flush()
	})
}

// insightRef names an insight for ack and mute.
type insightRef struct {
	ID       string `arg:"" help:"Insight id."`
	Pipeline string `help:"The insight's pipeline, when several pipelines have an insight with this id."`
}

// InsightsAckCmd is "millstream insights ack <id>".
type InsightsAckCmd struct{ insightRef }

// InsightsMuteCmd is "millstream insights mute <id>".
type InsightsMuteCmd struct{ insightRef }

// Run acknowledges the insight.
func (c InsightsAckCmd) Run(rt *Runtime) error { return c.set(rt, store.InsightAcknowledged) }

// Run mutes the insight.
func (c InsightsMuteCmd) Run(rt *Runtime) error { return c.set(rt, store.InsightMuted) }

// set finds the insight, in --pipeline or in the one pipeline that has it, and sets its state.
func (c insightRef) set(rt *Runtime, state store.InsightState) error {
	err := changeState(rt, func(cl *apiClient) error {
		if c.Pipeline == "" {
			return errRemotePipeline("insight")
		}
		verb := map[store.InsightState]string{store.InsightAcknowledged: "acknowledge", store.InsightMuted: "mute"}[state]
		_, err := cl.post(rt.Ctx, pipelinePath(c.Pipeline, "insights", c.ID, verb), nil, nil)
		return err
	}, func(st store.Store) error {
		ins, err := st.Insights(rt.Ctx, store.InsightFilter{Pipeline: c.Pipeline})
		if err != nil {
			return err
		}
		var owners []string
		for _, in := range ins {
			if in.ID == c.ID {
				owners = append(owners, in.Pipeline)
			}
		}
		switch len(owners) {
		case 0:
			return fmt.Errorf("insight %s: %w", c.ID, store.ErrNotFound)
		case 1:
		default:
			return usageError(fmt.Sprintf("insight %s exists in %s: name one with --pipeline", c.ID,
				strings.Join(owners, ", ")))
		}
		return st.SetInsightState(rt.Ctx, owners[0], c.ID, state)
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s %s\n", c.ID, state)
	return err
}

// RunReportCmd is "millstream run report".
type RunReportCmd struct {
	Pipeline string `arg:"" help:"Pipeline name."`
	RunID    string `arg:"" name:"run" help:"Run id."`
	JSON     bool   `name:"json" help:"Print the report as JSON."`
}

// Run prints the run's report (spec section 11.1).
func (c RunReportCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st store.Store) error {
		now := time.Now()
		r, err := insights.BuildReport(rt.Ctx, st, c.Pipeline, c.RunID, now)
		if err != nil {
			return err
		}
		if c.JSON {
			enc := json.NewEncoder(rt.IO.Out)
			enc.SetIndent("", "  ")
			return enc.Encode(r)
		}
		return r.WriteText(rt.IO.Out, now)
	})
}
