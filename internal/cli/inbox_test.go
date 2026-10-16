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
	ctx := context.Background()
	p, err := pipeline.Parse([]byte(reviewPipeline))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(reviewPipeline))
	if _, err := st.ApplyRevision(ctx, store.PackageRecord{Hash: hex.EncodeToString(sum[:]), Bundle: []byte("{}"),
		Pipeline: p}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Fire(ctx, store.Firing{Pipeline: "deals", Trigger: "adhoc", Key: "r1", RunID: "r1"}); err != nil {
		t.Fatal(err)
	}
	lease := func(step string) store.Lease {
		t.Helper()
		slots := map[store.StepRef]int{}
		for _, other := range []string{"crawl", "gate", "sink"} {
			if other != step {
				slots[store.StepRef{Pipeline: "deals", Step: other}] = 0
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
	return s, ids
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
	if out := call(user, InboxApproveCmd{deliveryRef: ref("a")}); out != ids["a"]+" approved (delivery done)\n" {
		t.Fatalf("approve: %q", out)
	}
	run(0, nil, "inbox", "approve", ids["b"], "--as", "bo", "--set", "/price=150", "--set", "/notes=checked twice",
		"--reason", "overpriced", "--text", "fixed the price")
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

	out = call(nil, InboxSamplesListCmd{Inbox: "analysts"})
	if !strings.Contains(out, ids["d"]) {
		t.Fatalf("samples:\n%s", out)
	}
	out = run(0, nil, "inbox", "samples", "list", "--json")
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
