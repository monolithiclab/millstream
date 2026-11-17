package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/api"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// reviewPipeline sends crawled listings through a review step.
const reviewPipeline = `pipeline: deals
triggers: { adhoc: { manual: {} } }
steps:
  crawl: { action: ./actions/crawl, in: trigger.adhoc, out: raw }
  gate: { action: review, in: raw, out: ok }
  sink: { action: ./actions/sink, in: ok }
`

// newReviewState is a state whose run parked listings a, b and c in inbox analysts and auto-approved d as a review
// sample, written through the store as the engine would.
func newReviewState(t *testing.T) (*state, map[string]string) {
	t.Helper()
	s := newEmptyState(t)
	st := s.st
	if st == nil { // the e2e build's commands open --db: seed that file
		if err := os.MkdirAll(filepath.Dir(s.db), 0o750); err != nil {
			t.Fatal(err)
		}
		var err error
		if st, err = sqlite.Open(s.db, sqlite.Options{}); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = st.Close() }()
	}
	return s, seedReviews(t, st, "deals")
}

// reviewPipelines are the pipelines seedReviews may seed: each lease leaves the others' steps alone.
var reviewPipelines = []string{"deals", "audit"}

// seedReviews applies reviewPipeline as pipeline name, one of reviewPipelines, and parks the items of its run r1:
// a, b and c in inbox analysts, d auto-approved as a review sample. It returns their delivery ids, which depend on
// neither the pipeline nor the store: two pipelines seeded alike hold the same ids.
func seedReviews(t *testing.T, st store.Store, name string) map[string]string {
	t.Helper()
	ctx := context.Background()
	src := strings.Replace(reviewPipeline, "pipeline: deals", "pipeline: "+name, 1)
	p, err := pipeline.Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(src))
	if _, err := st.ApplyRevision(ctx, store.PackageRecord{Hash: hex.EncodeToString(sum[:]), Bundle: []byte("{}"),
		Pipeline: p}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Fire(ctx, store.Firing{Pipeline: name, Trigger: "adhoc", Key: "r1", RunID: "r1"}); err != nil {
		t.Fatal(err)
	}
	lease := func(step string) store.Lease {
		t.Helper()
		slots := map[store.StepRef]int{}
		for _, other := range reviewPipelines {
			for _, s := range []string{"crawl", "gate", "sink"} {
				if other != name || s != step {
					slots[store.StepRef{Pipeline: other, Step: s}] = 0
				}
			}
		}
		ls, err := st.Lease(ctx, store.LeaseRequest{Owner: "n", Max: 1, TTL: time.Minute, Slots: slots})
		if err != nil || len(ls) != 1 {
			t.Fatalf("lease %s: %v, %v", step, ls, err)
		}
		return ls[0]
	}
	c := lease("crawl")
	var outs []store.Output
	for _, k := range []string{"a", "b", "c", "d"} {
		outs = append(outs, store.Output{Key: k, Payload: json.RawMessage(`{"city":"Lyon","price":100}`)})
	}
	if _, err := st.Done(ctx, store.DoneRequest{LeaseRef: c.Ref(), Attempt: 1, Outputs: outs}); err != nil {
		t.Fatal(err)
	}
	spec := store.ReviewSpec{Inbox: "analysts", Timeout: 72 * time.Hour, Editable: []string{"/price", "/notes"},
		Reasons: []string{"overpriced"}}
	ids := map[string]string{}
	for range outs {
		l := lease("gate")
		_, err := st.Park(ctx, store.ParkRequest{LeaseRef: l.Ref(), Attempt: 1, Spec: spec, Display: "Lyon\n100 EUR",
			AutoApprove: l.Item.Key == "d", Sample: true})
		if err != nil {
			t.Fatal(err)
		}
		ids[l.Item.Key] = l.DeliveryID
	}
	return ids
}

func Test_inboxCommands(t *testing.T) {
	t.Parallel()
	s, ids := newReviewState(t)
	user := map[string]string{"USER": "ana"}
	run := func(want int, env map[string]string, argv ...string) string {
		t.Helper()
		var out bytes.Buffer
		code, stderr := s.exec(context.Background(), env, &out, argv...)
		if code != want {
			t.Fatalf("millstream %s: code %d, want %d\n%s%s", strings.Join(argv, " "), code, want, out.String(), stderr)
		}
		return out.String() + stderr
	}

	// Each command runs through the parser once; the other cases call the command directly, the parser costing more
	// than their work.
	call := func(env map[string]string, cmd runner) string {
		t.Helper()
		out, stderr, err := s.call(env, cmd)
		if err != nil {
			return out + stderr + err.Error()
		}
		return out + stderr
	}
	out := call(nil, InboxListCmd{})
	if !strings.Contains(out, "DELIVERY") || strings.Count(out, "analysts") != 3 || !strings.Contains(out, "Lyon") {
		t.Fatalf("inbox:\n%s", out)
	}
	out = run(0, nil, "inbox", "--json", "--inbox", "analysts", "--pipeline", "deals")
	var first reviewJSON
	if err := json.Unmarshal([]byte(strings.SplitN(out, "\n", 2)[0]), &first); err != nil || first.Version != 1 ||
		first.Item.Key != "a" || first.Deadline == nil || first.Editable[0] != "/price" {
		t.Fatalf("inbox --json: %+v, %v\n%s", first, err, out)
	}
	out = run(0, nil, "inbox", "show", ids["a"])
	for _, want := range []string{"version 1: open", "Inbox: analysts", "Editable: /price, /notes",
		"Reasons: overpriced", "Lyon\n100 EUR", `"price": 100`} {
		if !strings.Contains(out, want) {
			t.Fatalf("show lacks %q:\n%s", want, out)
		}
	}

	ref := func(k string) deliveryRef { return deliveryRef{Delivery: ids[k]} }
	if out := call(user, InboxApproveCmd{deliveryRef: ref("a"), versionFlag: versionFlag{Version: 1}}); out != ids["a"]+
		" approved (delivery done)\n" {
		t.Fatalf("approve: %q", out)
	}
	// Without --version, the review's current version is decided, and the command says so.
	if out := run(0, nil, "inbox", "approve", ids["b"], "--as", "bo", "--set", "/price=150", "--set",
		"/notes=checked twice", "--reason", "overpriced", "--text", "fixed the price"); !strings.Contains(out,
		"Deciding version 1 of the review, its current one") {
		t.Fatalf("approve without --version: %q", out)
	}
	if out := call(user, InboxRejectCmd{deliveryRef: ref("c"), versionFlag: versionFlag{Version: 2},
		Reason: "overpriced"}); !strings.Contains(out, "at version 1, not 2") {
		t.Fatalf("reject at another version: %s", out)
	}
	out = call(nil, InboxShowCmd{deliveryRef: deliveryRef{Delivery: ids["b"], Pipeline: "deals"}, JSON: true})
	if !strings.Contains(out, `"reviewer": "bo"`) || !strings.Contains(out, `"path": "/price"`) ||
		!strings.Contains(out, `"value": "checked twice"`) {
		t.Fatalf("show --json after an edit:\n%s", out)
	}
	if out := call(user, InboxRejectCmd{deliveryRef: ref("c")}); !strings.Contains(out,
		"a rejection needs a reason code: one of overpriced") {
		t.Fatalf("reject without a reason: %s", out)
	}
	patch := fixture("inbox-patch.json")
	if out := call(user, InboxApproveCmd{deliveryRef: ref("c"), Edit: patch}); !strings.Contains(out, "is not editable") {
		t.Fatalf("approve outside editable: %s", out)
	}
	run(0, user, "inbox", "reject", ids["c"], "--reason", "overpriced")
	if out := call(user, InboxApproveCmd{deliveryRef: ref("c")}); !strings.Contains(out, "already decided: reject by ana") {
		t.Fatalf("deciding twice: %s", out)
	}

	out = call(nil, InboxSamplesCmd{Inbox: "analysts"})
	if !strings.Contains(out, ids["d"]) {
		t.Fatalf("samples:\n%s", out)
	}
	out = run(0, nil, "inbox", "samples", "--json")
	if !strings.Contains(out, `"deliveryId":"`+ids["d"]+`"`) || !strings.Contains(out, `"deadline"`) {
		t.Fatalf("samples --json:\n%s", out)
	}
	if out := run(0, user, "inbox", "sample", ids["d"], "disagree", "--reason", "too cheap"); out != ids["d"]+" disagree\n" {
		t.Fatalf("sample: %q", out)
	}
	if out := call(nil, InboxListCmd{}); strings.Contains(out, "analysts") {
		t.Fatalf("the inbox after every decision:\n%s", out)
	}

	s.checkRefusals(t, nil, []refusal{
		{InboxApproveCmd{deliveryRef: deliveryRef{Delivery: ids["a"]}}, true, "--as: name who decides"},
		{InboxApproveCmd{deliveryRef: deliveryRef{Delivery: ids["a"]}, Edit: patch, Set: []string{"/x=1"}}, true,
			"mutually exclusive"},
		{InboxApproveCmd{deliveryRef: deliveryRef{Delivery: ids["a"]}, Set: []string{"x=1"}}, true, "want /pointer=value"},
		{InboxSampleCmd{deliveryRef: deliveryRef{Delivery: "dl_missing"}, reviewerFlag: reviewerFlag{As: "ana"},
			Decision: "agree"}, false, "not found"},
		{InboxShowCmd{deliveryRef: deliveryRef{Delivery: "dl_missing"}}, false, "not found"},
	})
}

// Through the server, the inbox decisions and insight states find the pipeline in the API's listings when
// --pipeline names none; an id two pipelines hold asks for --pipeline.
func Test_decisionsThroughTheServer(t *testing.T) {
	t.Parallel()
	st := memoryStore(t)
	ids := seedReviews(t, st, "deals")
	srv, err := api.New(api.Options{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	call := viaServer(srv, map[string]string{ServerEnv: "http://127.0.0.1:7070"})
	ana := reviewerFlag{As: "ana"}
	ref := func(k string) deliveryRef { return deliveryRef{Delivery: ids[k]} }
	insight := func(name, id string) store.Insight {
		return store.Insight{Pipeline: name, ID: id, Kind: "failureCluster", Scope: "crawl", Severity: "warning",
			Evidence: json.RawMessage(`{}`)}
	}
	if err := st.RecordInsights(context.Background(), store.InsightReport{Pipeline: "deals",
		Kinds: []string{"failureCluster"}, Insights: []store.Insight{insight("deals", "in_1")}}); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		cmd runner
		out string
	}{
		{InboxApproveCmd{deliveryRef: ref("a"), reviewerFlag: ana, versionFlag: versionFlag{Version: 1}},
			ids["a"] + " approved\n"},
		{InboxSampleCmd{deliveryRef: ref("d"), reviewerFlag: ana, Decision: "agree"}, ids["d"] + " agree\n"},
		{InsightsAcknowledgeCmd{insightRef{ID: "in_1"}}, "in_1 acknowledged\n"},
	} {
		if out, err := call(step.cmd); err != nil || out != step.out {
			t.Fatalf("%+v: %q, %v; want %q", step.cmd, out, err, step.out)
		}
	}
	// A second pipeline holding the same ids.
	seedReviews(t, st, "audit")
	if err := st.RecordInsights(context.Background(), store.InsightReport{Pipeline: "audit",
		Kinds: []string{"failureCluster"}, Insights: []store.Insight{insight("audit", "in_1")}}); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		cmd  runner
		want string // the error, or what it prints
	}{
		{InboxRejectCmd{deliveryRef: ref("b"), reviewerFlag: ana, Reason: "overpriced"},
			"open review of delivery " + ids["b"] + " exists in audit, deals: name one with --pipeline"},
		{InsightsMuteCmd{insightRef{ID: "in_1"}}, "insight in_1 exists in audit, deals: name one with --pipeline"},
		{InboxApproveCmd{deliveryRef: deliveryRef{Delivery: "dl_missing"}, reviewerFlag: ana},
			"open review of delivery dl_missing: not found"},
		{InboxRejectCmd{deliveryRef: deliveryRef{Delivery: ids["b"], Pipeline: "audit"}, reviewerFlag: ana,
			Reason: "overpriced"}, ids["b"] + " rejected\n"},
		// deals' sample is decided: audit's is the one open.
		{InboxSampleCmd{deliveryRef: ref("d"), reviewerFlag: ana, Decision: "disagree"}, ids["d"] + " disagree\n"},
	} {
		out, err := call(step.cmd)
		if err != nil {
			out = err.Error()
		}
		if !strings.Contains(out, step.want) {
			t.Errorf("%+v: %q; want %q", step.cmd, out, step.want)
		}
	}
}
