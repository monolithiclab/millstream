package engine

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/review"
	"github.com/monolithiclab/millstream/internal/store"
)

// webhook records the notifications posted to it, in process.
type webhook struct {
	mu     sync.Mutex
	bodies []string
}

func (w *webhook) RoundTrip(r *http.Request) (*http.Response, error) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.bodies = append(w.bodies, r.URL.String()+" "+string(b))
	w.mu.Unlock()
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}},
		nil
}

func (w *webhook) posts() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bodies
}

// A review step parks what autoApprove does not approve, without holding a slot, tells its webhook, and passes the
// rest at once as samples; the engine's timeout tick escalates, then rejects, what nobody decided.
func TestReviewStepInTheEngine(t *testing.T) {
	hook := &webhook{}
	notifier := &review.Notifier{BaseURL: "https://ms.example", Client: &http.Client{Transport: hook}}
	v := newEnv(t, Options{Notifier: notifier})
	pkg := newPackage(t, manualPipeline("p", `  src: { action: ./actions/a, in: trigger.adhoc, out: raw }
  gate:
    action: review
    in: raw
    out: ok
    config: { inbox: analysts, timeout: 1h, onTimeout: { escalate: leads }, autoApprove: "item.n >= 9",
      sampleRate: 1, display: "n={{.item.n}}", notify: "https://hooks.example/x" }
  sink: { action: ./actions/a, in: ok }
`), "a")
	pkg.Files["actions/a/action.yaml"] = []byte(fakeManifest + "output: { type: object }\n")
	v.apply(pkg)
	id := v.fire("p", "adhoc", "r1")
	v.emit(output("a", `{"n":1}`), output("b", `{"n":9}`), output("c", `{"n":"x"}`))
	as, err := v.e.Pull(v.ctx, "node-a", 10)
	if err != nil || len(as) != 1 || as[0].Lease.Step != "sink" || as[0].Lease.Item.Key != "b" {
		t.Fatalf("pull = %v, %v: only the auto-approved item reaches the sink", as, err)
	}
	if v.e.inflightCount() != 1 {
		t.Fatalf("in flight = %d: a parked review holds no slot", v.e.inflightCount())
	}
	parked, err := v.st.Reviews(v.ctx, store.ReviewFilter{Inbox: "analysts"})
	if err != nil || len(parked) != 1 || parked[0].Item.Key != "a" || parked[0].Display != "n=1" ||
		len(parked[0].Spec.Schemas) != 1 || !parked[0].Deadline.Equal(t0.Add(time.Hour)) {
		t.Fatalf("parked = %+v, %v", parked, err)
	}
	if s, err := v.st.ReviewSamples(v.ctx, store.SampleFilter{}); err != nil || len(s) != 1 || s[0].Item.Key != "b" {
		t.Fatalf("samples = %+v, %v", s, err)
	}
	if c := v.byStep(id, "gate")[2]; c.State != store.DeliveryDead || c.ErrorClass != ClassExpr ||
		!strings.Contains(c.ErrorMessage, "autoApprove of step gate") {
		t.Fatalf("a failing autoApprove: %+v", c)
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 1}); err != nil {
		t.Fatal(err)
	}
	if r := v.run("p", id); r.State != store.RunWaitingReview {
		t.Fatalf("run = %s", r.State)
	}
	if ok, err := v.e.Settled(v.ctx, []RunRef{{Pipeline: "p", ID: id}}); !ok || err != nil {
		t.Fatalf("a run waiting only on reviews is quiet: %v, %v", ok, err)
	}

	v.clock.Advance(61 * time.Minute)
	v.e.reviewTimeouts(v.ctx)
	notifier.Wait()
	posts := hook.posts()
	if len(posts) != 2 || !strings.HasPrefix(posts[0], "https://hooks.example/x ") ||
		!strings.Contains(posts[0], `Review needed in inbox analysts: p/gate, item a. `+
			`https://ms.example/inbox/analysts/`+parked[0].DeliveryID) ||
		!strings.Contains(posts[1], "Review escalated in inbox leads") {
		t.Fatalf("notifications:\n%s", strings.Join(posts, "\n"))
	}
	v.clock.Advance(61 * time.Minute)
	v.e.reviewTimeouts(v.ctx)
	if r := v.run("p", id); r.State != store.RunCompletedWithErrors {
		t.Fatalf("run after the second timeout = %s", r.State)
	}
	if logs := v.logs.String(); !strings.Contains(logs, `"applied":"escalate"`) ||
		!strings.Contains(logs, `"applied":"reject"`) {
		t.Fatalf("timeouts not logged:\n%s", logs)
	}
}
