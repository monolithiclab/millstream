package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// InboxCmd is "millstream inbox": the reviews and review samples of review steps (spec section 8).
type InboxCmd struct {
	List    InboxListCmd    `cmd:"" default:"withargs" help:"List the open reviews (the default)."`
	Show    InboxShowCmd    `cmd:"" help:"Show a review: its item, its display and what may be edited."`
	Approve InboxApproveCmd `cmd:"" help:"Approve a review, optionally editing the item first."`
	Reject  InboxRejectCmd  `cmd:"" help:"Reject a review, with a reason code when the step lists some."`
	Samples InboxSamplesCmd `cmd:"" help:"List the open review samples."`
	Sample  InboxSampleCmd  `cmd:"" help:"Decide a review sample: agree or disagree with the rule that approved it."`
}

// InboxListCmd is "millstream inbox [list]".
type InboxListCmd struct {
	Inbox    string `help:"Only this inbox's reviews."`
	Pipeline string `help:"Only this pipeline's reviews."`
	JSON     bool   `name:"json" help:"Print each review with its item as a JSON line."`
}

// reviewJSON is a review as --json prints it.
type reviewJSON struct {
	Pipeline   string          `json:"pipeline"`
	DeliveryID string          `json:"deliveryId"`
	RunID      string          `json:"runId"`
	Step       string          `json:"step"`
	Inbox      string          `json:"inbox"`
	State      string          `json:"state"`
	Version    int64           `json:"version"`
	ParkedAt   time.Time       `json:"parkedAt"`
	Deadline   *time.Time      `json:"deadline,omitempty"`
	Editable   []string        `json:"editable"`
	Reasons    []string        `json:"reasons"`
	Display    string          `json:"display"`
	Decision   string          `json:"decision,omitempty"`
	Reviewer   string          `json:"reviewer,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	Patch      json.RawMessage `json:"patch,omitempty"`
	Item       itemJSON        `json:"item"`
}

// itemJSON is an item as the inbox prints it.
type itemJSON struct {
	ID      string          `json:"id"`
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
}

func toReviewJSON(r store.Review) reviewJSON {
	out := reviewJSON{
		Pipeline: r.Pipeline, DeliveryID: r.DeliveryID, RunID: r.RunID, Step: r.Step, Inbox: r.Spec.Inbox,
		State: string(r.State), Version: r.Version, ParkedAt: r.ParkedAt, Editable: r.Spec.Editable,
		Reasons: r.Spec.Reasons, Display: r.Display, Decision: r.Decision, Reviewer: r.Reviewer, Reason: r.Reason,
		Item: itemJSON{ID: r.Item.ID, Key: r.Item.Key, Payload: r.Item.Payload},
	}
	if !r.Deadline.IsZero() {
		out.Deadline = &r.Deadline
	}
	if r.State != store.ReviewOpen {
		out.Patch = r.Patch
	}
	return out
}

// Run lists the open reviews, oldest first.
func (c InboxListCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st store.Store) error {
		rs, err := st.Reviews(rt.Ctx, store.ReviewFilter{Pipeline: c.Pipeline, Inbox: c.Inbox})
		if err != nil {
			return err
		}
		if c.JSON {
			enc := json.NewEncoder(rt.IO.Out)
			for _, r := range rs {
				if err := enc.Encode(toReviewJSON(r)); err != nil {
					return err
				}
			}
			return nil
		}
		tw := tabwriter.NewWriter(rt.IO.Out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "DELIVERY\tPIPELINE\tSTEP\tINBOX\tITEM\tPARKED\tDEADLINE\tDISPLAY")
		for _, r := range rs {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", r.DeliveryID, r.Pipeline, r.Step, r.Spec.Inbox,
				r.Item.Key, r.ParkedAt.Format(time.RFC3339), orDash(r.Deadline), firstLine(r.Display))
		}
		return tw.Flush()
	})
}

func orDash(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339)
}

// firstLine is a display's first line, cut to 60 characters, "-" when empty.
func firstLine(s string) string {
	s, _, _ = strings.Cut(s, "\n")
	if s = oneLine(s, 60); s == "" {
		return "-"
	}
	return s
}

// deliveryRef names a review or review sample by its delivery.
type deliveryRef struct {
	Delivery string `arg:"" help:"Delivery id."`
	Pipeline string `help:"The delivery's pipeline, when several pipelines have a delivery with this id."`
}

// InboxShowCmd is "millstream inbox show <delivery>".
type InboxShowCmd struct {
	deliveryRef
	JSON bool `name:"json" help:"Print the review as JSON."`
}

// Run prints the review: where it waits, until when, its display, what may be edited and the item's payload.
func (c InboxShowCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st store.Store) error {
		r, err := st.Review(rt.Ctx, c.Pipeline, c.Delivery)
		if err != nil {
			return err
		}
		if c.JSON {
			enc := json.NewEncoder(rt.IO.Out)
			enc.SetIndent("", "  ")
			return enc.Encode(toReviewJSON(r))
		}
		payload, err := json.MarshalIndent(json.RawMessage(r.Item.Payload), "", "  ")
		if err != nil {
			return err
		}
		w := rt.IO.Out
		fmt.Fprintf(w, "Review of delivery %s (%s/%s), version %d: %s", r.DeliveryID, r.Pipeline, r.Step, r.Version,
			r.State)
		if r.Decision != "" {
			fmt.Fprintf(w, ", %s by %s", r.Decision, r.Reviewer)
		}
		fmt.Fprintf(w, "\nInbox: %s\nParked: %s\nDeadline: %s\nItem: %s (key %s)\n", r.Spec.Inbox,
			r.ParkedAt.Format(time.RFC3339), orDash(r.Deadline), r.Item.ID, r.Item.Key)
		fmt.Fprintf(w, "Editable: %s\nReasons: %s\n", listOrDash(r.Spec.Editable), listOrDash(r.Spec.Reasons))
		if r.Display != "" {
			fmt.Fprintf(w, "\n%s\n", strings.TrimRight(r.Display, "\n"))
		}
		_, err = fmt.Fprintf(w, "\n%s\n", payload)
		return err
	})
}

func listOrDash(l []string) string {
	if len(l) == 0 {
		return "-"
	}
	return strings.Join(l, ", ")
}

// reviewerFlag is who decides: --as, else $USER, else the system's account name (authentication arrives with M4).
type reviewerFlag struct {
	As string `name:"as" help:"Who decides (default: $USER, else the account's name)."`
}

func (f reviewerFlag) reviewer(rt *Runtime) (string, error) {
	if f.As != "" {
		return f.As, nil
	}
	if u := identity(rt); u != "" {
		return u, nil
	}
	return "", usageError("--as: name who decides (no $USER, and the system names no account)")
}

// versionFlag is the version of the review the reviewer saw (inbox show prints it): a review escalated since is a
// conflict.
type versionFlag struct {
	Version int64 `placeholder:"N" help:"The review's version you saw, as inbox show prints it (default: its current one, printed)."`
}

// InboxApproveCmd is "millstream inbox approve <delivery>".
type InboxApproveCmd struct {
	deliveryRef
	reviewerFlag
	versionFlag
	Edit   string   `placeholder:"PATCH" help:"Apply this JSON Patch file (RFC 6902) to the item before approving."`
	Set    []string `placeholder:"/PTR=VALUE" help:"Set a value before approving (repeatable): JSON, or else a string."`
	Reason string   `help:"Reason code."`
	Text   string   `help:"Free-text reason."`
}

// Run approves the review: the item, as edited, passes on to the step's outputs.
func (c InboxApproveCmd) Run(rt *Runtime) error {
	patch, err := c.patch()
	if err != nil {
		return err
	}
	return decide(rt, c.deliveryRef, c.reviewerFlag, store.ReviewDecisionRequest{
		Decision: store.DecisionApprove, Version: c.Version, Patch: patch, Reason: c.Reason, Text: c.Text,
	})
}

// patch builds the edit: the --edit file, or the --set values as add operations.
func (c InboxApproveCmd) patch() (json.RawMessage, error) {
	switch {
	case c.Edit != "" && len(c.Set) > 0:
		return nil, usageError("--edit and --set are mutually exclusive")
	case c.Edit != "":
		data, err := os.ReadFile(c.Edit) // #nosec G304 -- the reviewer names the patch file
		if err != nil {
			return nil, err
		}
		if _, err := review.ParsePatch(data); err != nil {
			return nil, usageError("--edit: " + err.Error())
		}
		return data, nil
	case len(c.Set) == 0:
		return nil, nil
	}
	ops := make([]review.Op, 0, len(c.Set))
	for _, s := range c.Set {
		ptr, raw, ok := strings.Cut(s, "=")
		if !ok || !strings.HasPrefix(ptr, "/") {
			return nil, usageError(fmt.Sprintf("--set %s: want /pointer=value", s))
		}
		var v any = raw
		if json.Valid([]byte(raw)) {
			_ = json.Unmarshal([]byte(raw), &v) // valid JSON decodes
		}
		ops = append(ops, review.Op{Op: "add", Path: ptr, Value: v})
	}
	return json.Marshal(ops)
}

// InboxRejectCmd is "millstream inbox reject <delivery>".
type InboxRejectCmd struct {
	deliveryRef
	reviewerFlag
	versionFlag
	Reason string `help:"Reason code: required when the step lists reason codes."`
	Text   string `help:"Free-text reason."`
}

// Run rejects the review: the delivery is filtered with the reason.
func (c InboxRejectCmd) Run(rt *Runtime) error {
	return decide(rt, c.deliveryRef, c.reviewerFlag, store.ReviewDecisionRequest{
		Decision: store.DecisionReject, Version: c.Version, Reason: c.Reason, Text: c.Text,
	})
}

// decide applies a decision to the review at the version named, or at the one it has now, saying so, and prints the
// outcome.
func decide(rt *Runtime, ref deliveryRef, who reviewerFlag, req store.ReviewDecisionRequest) error {
	reviewer, err := who.reviewer(rt)
	if err != nil {
		return err
	}
	current := func(version int64) {
		fmt.Fprintf(rt.IO.Err, "Deciding version %d of the review, its current one: pass --version to decide the "+
			"version you saw.\n", version)
	}
	var state string
	err = changeState(rt, func(cl *apiClient) error {
		if ref.Pipeline == "" {
			return errRemotePipeline("delivery")
		}
		path := pipelinePath(ref.Pipeline, "reviews", ref.Delivery)
		if req.Version == 0 {
			var r api.Review
			if _, err := cl.do(rt.Ctx, http.MethodGet, path, nil, &r); err != nil {
				return err
			}
			req.Version = r.Version
			current(r.Version)
		}
		cl.user = reviewer // the API answers the review, not its delivery's state
		_, err := cl.post(rt.Ctx, path+"/decision", api.ReviewDecision{Decision: req.Decision, Version: req.Version,
			Patch: req.Patch, Reason: req.Reason, Text: req.Text}, nil)
		return err
	}, func(st store.Store) error {
		r, err := st.Review(rt.Ctx, ref.Pipeline, ref.Delivery)
		if err != nil {
			return err
		}
		if req.Version == 0 {
			req.Version = r.Version
			current(r.Version)
		}
		req.Pipeline, req.DeliveryID, req.Reviewer = r.Pipeline, r.DeliveryID, reviewer
		s, err := st.DecideReview(rt.Ctx, req)
		state = string(s)
		return err
	})
	if err != nil {
		return err
	}
	verb := map[string]string{store.DecisionApprove: "approved", store.DecisionReject: "rejected"}[req.Decision]
	if state != "" {
		verb += " (delivery " + state + ")"
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s %s\n", ref.Delivery, verb)
	return err
}

// InboxSamplesCmd is "millstream inbox samples".
type InboxSamplesCmd struct {
	List InboxSamplesListCmd `cmd:"" default:"withargs" help:"List the open review samples (the default)."`
}

// InboxSamplesListCmd is "millstream inbox samples [list]".
type InboxSamplesListCmd struct {
	Inbox    string `help:"Only this inbox's samples."`
	Pipeline string `help:"Only this pipeline's samples."`
	JSON     bool   `name:"json" help:"Print each sample with its item as a JSON line."`
}

// sampleJSON is a review sample as --json prints it.
type sampleJSON struct {
	Pipeline   string     `json:"pipeline"`
	DeliveryID string     `json:"deliveryId"`
	Step       string     `json:"step"`
	Inbox      string     `json:"inbox"`
	State      string     `json:"state"`
	CreatedAt  time.Time  `json:"createdAt"`
	Deadline   *time.Time `json:"deadline,omitempty"`
	Display    string     `json:"display"`
	Item       itemJSON   `json:"item"`
}

// Run lists the open review samples, oldest first.
func (c InboxSamplesListCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st store.Store) error {
		ss, err := st.ReviewSamples(rt.Ctx, store.SampleFilter{Pipeline: c.Pipeline, Inbox: c.Inbox})
		if err != nil {
			return err
		}
		if c.JSON {
			enc := json.NewEncoder(rt.IO.Out)
			for _, s := range ss {
				out := sampleJSON{Pipeline: s.Pipeline, DeliveryID: s.DeliveryID, Step: s.Step, Inbox: s.Inbox,
					State: string(s.State), CreatedAt: s.CreatedAt, Display: s.Display,
					Item: itemJSON{ID: s.Item.ID, Key: s.Item.Key, Payload: s.Item.Payload}}
				if !s.Deadline.IsZero() {
					out.Deadline = &s.Deadline
				}
				if err := enc.Encode(out); err != nil {
					return err
				}
			}
			return nil
		}
		tw := tabwriter.NewWriter(rt.IO.Out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "DELIVERY\tPIPELINE\tSTEP\tINBOX\tITEM\tCREATED\tDEADLINE\tDISPLAY")
		for _, s := range ss {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", s.DeliveryID, s.Pipeline, s.Step, s.Inbox, s.Item.Key,
				s.CreatedAt.Format(time.RFC3339), orDash(s.Deadline), firstLine(s.Display))
		}
		return tw.Flush()
	})
}

// InboxSampleCmd is "millstream inbox sample <delivery> agree|disagree".
type InboxSampleCmd struct {
	deliveryRef
	reviewerFlag
	Decision string `arg:"" enum:"agree,disagree" help:"agree or disagree with the rule that approved the item."`
	Reason   string `help:"Why, as a reason code or free text."`
}

// Run records the sample's decision: it measures the rule and changes nothing else.
func (c InboxSampleCmd) Run(rt *Runtime) error {
	reviewer, err := c.reviewer(rt)
	if err != nil {
		return err
	}
	err = changeState(rt, func(cl *apiClient) error {
		if c.Pipeline == "" {
			return errRemotePipeline("delivery")
		}
		cl.user = reviewer
		_, err := cl.post(rt.Ctx, pipelinePath(c.Pipeline, "samples", c.Delivery, "decision"),
			api.SampleDecision{Decision: c.Decision, Reason: c.Reason}, nil)
		return err
	}, func(st store.Store) error {
		s, err := st.ReviewSample(rt.Ctx, c.Pipeline, c.Delivery)
		if err != nil {
			return err
		}
		return st.DecideSample(rt.Ctx, store.SampleDecisionRequest{Pipeline: s.Pipeline, DeliveryID: s.DeliveryID,
			Decision: c.Decision, Reviewer: reviewer, Reason: c.Reason})
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s %s\n", c.Delivery, c.Decision)
	return err
}
