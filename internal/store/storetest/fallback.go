package storetest

import (
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// fallbackPipeline has a score step backed by a fallback for abstentions and non-retryable errors, auditing every
// delivery its primary covers, a plain step without a fallback, and a report reading what score publishes.
const fallbackPipeline = `
pipeline: fb
triggers:
  daily: { cron: "0 6 * * *" }
steps:
  score:
    action: ./actions/rules
    in: trigger.daily
    out: scored
    retry: { maxAttempts: 2, backoff: 1s, maxBackoff: 1s }
    fallback: { action: ./actions/agent, when: [abstain, error], auditRate: 1 }
  plain:
    action: ./actions/plain
    in: trigger.daily
  report:
    action: ./actions/report
    in: scored
`

var fallbackSteps = []string{"score", "plain", "report"}

// leaseFB leases the oldest due delivery of a step of fallbackPipeline.
func (e *env) leaseFB(step string) store.Lease {
	e.t.Helper()
	return e.leaseOnly("fb", fallbackSteps, step)
}

func (e *env) abstain(l store.Lease, reason string) store.DeliveryState {
	e.t.Helper()
	state, err := e.s.Abstain(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Message: reason})
	if err != nil {
		e.t.Fatalf("Abstain %s: %v", l.Step, err)
	}
	return state
}

func (e *env) fbDelivery(runID, step string) store.Delivery {
	e.t.Helper()
	ds, err := e.s.Deliveries(e.ctx, store.DeliveryFilter{Pipeline: "fb", RunID: runID, Step: step})
	if err != nil || len(ds) != 1 {
		e.t.Fatalf("Deliveries %s %s = %v, %v", runID, step, ds, err)
	}
	return ds[0]
}

// TestAbstainAndFallback covers the Abstain unit and the fallback (spec section 11.5): an abstention or a
// non-retryable error hands the delivery to the fallback with its retry budget restarted, its outputs keep their
// ids and record producedBy, and a step without a fallback, or a fallback abstaining, dead-letters the item.
func TestAbstainAndFallback(t *testing.T, open Opener) {
	e := newEnv(t, open, fallbackPipeline)
	e.firePipeline("fb", "daily", "r1")

	l := e.leaseFB("score")
	stale := l
	if state := e.abstain(l, "unsure"); state != store.DeliveryPending {
		t.Fatalf("abstained delivery is %s, want pending for its fallback", state)
	}
	_, err := e.s.Abstain(e.ctx, store.FailRequest{LeaseRef: stale.Ref(), Attempt: stale.Attempt})
	wantErr(t, err, store.ErrStaleLease)
	d := e.fbDelivery("r1", "score")
	if !d.Fallback || d.Attempt != 2 || d.ErrorClass != store.ClassAbstain || d.ErrorMessage != "unsure" {
		t.Fatalf("handed-off delivery = %+v", d)
	}
	if evs := e.eventKinds(store.EventDeliveryFallback); len(evs) != 1 || !strings.Contains(string(evs[0].Data), `"cause":"abstain"`) {
		t.Fatalf("deliveryFallback events = %+v", evs)
	}
	l = e.leaseFB("score")
	if !l.Fallback || l.Attempt != 2 {
		t.Fatalf("fallback lease = %+v", l)
	}
	e.done(l, out("", `{"score":3}`))
	scored := e.itemsIn("fb", "r1", "scored")
	if len(scored) != 1 || scored[0].ProducedBy != store.ProducedByFallback ||
		scored[0].ID != ids.Item("score", "scored", l.Item.ID, 0, "", 0) {
		t.Fatalf("fallback output = %+v", scored)
	}
	if as, _ := e.s.Audits(e.ctx, store.AuditFilter{Pipeline: "fb"}); len(as) != 0 {
		t.Fatalf("a fallback output was audited: %+v", as)
	}

	// A step without a fallback dead-letters an abstention as a non-retryable error.
	if state := e.abstain(e.leaseFB("plain"), "no idea"); state != store.DeliveryDead {
		t.Fatalf("abstention without a fallback is %s, want dead", state)
	}
	if d := e.fbDelivery("r1", "plain"); d.ErrorClass != store.ClassAbstain {
		t.Fatalf("plain delivery = %+v", d)
	}

	// A non-retryable error falls back; the fallback's retries restart; a retryable error of the primary retries.
	e.firePipeline("fb", "daily", "r2")
	l = e.leaseFB("score")
	if state, err := e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: 1, Class: "test",
		Message: "boom", Retryable: true}); err != nil || state != store.DeliveryPending {
		t.Fatalf("retryable failure = %s, %v", state, err)
	}
	e.clock.Advance(time.Second)
	if l = e.leaseFB("score"); l.Fallback || e.fail(l, false) != store.DeliveryPending {
		t.Fatalf("a non-retryable failure of the primary did not fall back: %+v", l)
	}
	l = e.leaseFB("score")
	if !l.Fallback || e.fail(l, true) != store.DeliveryPending {
		t.Fatalf("the fallback's first retryable failure was not retried: %+v", l)
	}
	e.clock.Advance(time.Second)
	if state := e.abstain(e.leaseFB("score"), "still unsure"); state != store.DeliveryDead {
		t.Fatalf("an abstaining fallback is %s, want dead", state)
	}
	if d := e.fbDelivery("r2", "score"); d.ErrorMessage != "the fallback action abstained: still unsure" {
		t.Fatalf("dead fallback = %+v", d)
	}
}

// TestAudits covers fallback audits (spec section 11.5): sampled when the primary covers a delivery, leased after
// the deliveries, ended once with the fallback's outputs and their agreement, failed by an expired lease, never
// part of the run, and cancelled for an item a purge fenced.
func TestAudits(t *testing.T, open Opener) {
	e := newEnv(t, open, fallbackPipeline)
	e.firePipeline("fb", "daily", "r1")
	l := e.leaseFB("score")
	e.done(l, out("b", `{"score":2}`), out("a", `{"score":1}`))
	if scored := e.itemsIn("fb", "r1", "scored"); scored[0].ProducedBy != store.ProducedByPrimary {
		t.Fatalf("primary output = %+v", scored[0])
	}
	e.done(e.leaseFB("plain"))
	e.done(e.leaseFB("report"))
	e.done(e.leaseFB("report"))
	if r := e.runIn("fb", "r1"); r.State != store.RunSucceeded {
		t.Fatalf("run with a pending audit is %s, want succeeded", r.State)
	}
	as, err := e.s.Audits(e.ctx, store.AuditFilter{Pipeline: "fb", RunID: "r1"})
	if err != nil || len(as) != 1 || as[0].State != store.AuditPending || as[0].ID != ids.Audit(l.DeliveryID) ||
		string(as[0].Primary) != `[{"key":"a","payload":{"score":1}},{"key":"b","payload":{"score":2}}]` {
		t.Fatalf("Audits = %+v, %v", as, err)
	}

	al := e.lease(10)
	if len(al) != 1 || !al[0].Audit || !al[0].Fallback || al[0].DeliveryID != as[0].ID || al[0].Item.ID != l.Item.ID {
		t.Fatalf("audit leases = %+v", al)
	}
	if lost, err := e.s.Renew(e.ctx, "node1", []store.LeaseRef{al[0].Ref()}, time.Minute); err != nil || len(lost) != 0 {
		t.Fatalf("Renew audit = %v, %v", lost, err)
	}
	end := store.AuditEnd{LeaseRef: al[0].Ref(), CostUSD: 0.25,
		Outputs: []store.Output{out("a", `{"score": 1}`), out("b", `{"score":2}`)}}
	if err := e.s.EndAudit(e.ctx, end); err != nil {
		t.Fatal(err)
	}
	wantErr(t, e.s.EndAudit(e.ctx, end), store.ErrStaleLease)
	as, _ = e.s.Audits(e.ctx, store.AuditFilter{Pipeline: "fb", ItemID: l.Item.ID})
	if a := as[0]; a.State != store.AuditDone || a.Agreement == nil || *a.Agreement != 1 || a.CostUSD != 0.25 {
		t.Fatalf("agreeing audit = %+v", a)
	}
	if evs := e.eventKinds(store.EventAuditEnded); len(evs) != 1 || !strings.Contains(string(evs[0].Data), `"agreement":1`) ||
		evs[0].RunID != "" {
		t.Fatalf("auditEnded events = %+v", evs)
	}

	// A disagreeing audit, a failed one and an expired one.
	for _, run := range []string{"r2", "r3", "r4"} {
		e.firePipeline("fb", "daily", run)
		e.done(e.leaseFB("score"), out("", `{"score":5}`))
	}
	al = e.lease(10)
	var leased []store.Lease
	for _, x := range al {
		if x.Audit {
			leased = append(leased, x)
		}
	}
	if len(leased) != 3 {
		t.Fatalf("audit leases = %+v", al)
	}
	if err := e.s.EndAudit(e.ctx, store.AuditEnd{LeaseRef: leased[0].Ref(),
		Outputs: []store.Output{out("", `{"score":6}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := e.s.EndAudit(e.ctx, store.AuditEnd{LeaseRef: leased[1].Ref(), Class: store.ClassAbstain,
		Message: "unsure"}); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(2 * time.Minute)
	refs, err := e.s.ExpireLeases(e.ctx)
	if err != nil || !containsRef(refs, leased[2].Ref()) {
		t.Fatalf("ExpireLeases = %v, %v; want the audit's lease", refs, err)
	}
	for i, want := range []struct {
		state store.AuditState
		agree float64
		err   string
	}{{store.AuditDone, 0, ""}, {store.AuditFailed, -1, "abstain: unsure"}, {store.AuditFailed, -1, ""}} {
		as, _ := e.s.Audits(e.ctx, store.AuditFilter{Pipeline: "fb", ID: leased[i].DeliveryID})
		a := as[0]
		if a.State != want.state || (want.agree >= 0) != (a.Agreement != nil) ||
			(a.Agreement != nil && *a.Agreement != want.agree) || (want.err != "" && a.Error != want.err) {
			t.Fatalf("audit %d = %+v, want %+v", i, a, want)
		}
	}

	// A purge cancels the pending audit of the item it fences and drops what audits hold of it.
	e.firePipeline("fb", "daily", "r5")
	l = e.leaseFB("score")
	e.done(l, out("", `{"score":7}`))
	if _, _, err := e.s.StartPurge(e.ctx, store.PurgeRequest{ID: "p1", Item: l.Item.ID, Pipeline: "fb"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.ErasePurge(e.ctx, "p1"); err != nil {
		t.Fatal(err)
	}
	as, _ = e.s.Audits(e.ctx, store.AuditFilter{Pipeline: "fb", RunID: "r5"})
	if len(as) != 1 || as[0].State != store.AuditCancelled || string(as[0].Primary) != "null" {
		t.Fatalf("purged audit = %+v", as)
	}
}

func containsRef(refs []store.LeaseRef, ref store.LeaseRef) bool {
	for _, r := range refs {
		if r == ref {
			return true
		}
	}
	return false
}

// TestAuditBudget covers an agent fallback's audit under a day budget (spec sections 7.5 and 11.5): it reserves the
// fallback's maxCostUsd while leased, and is cancelled when the budget leaves no room for it.
func TestAuditBudget(t *testing.T, open Opener) {
	src := fallbackPipeline + "budget: { dayUsd: 1 }\n"
	e := newEnv(t, open, src)
	rev, err := e.s.ActiveRevision(e.ctx, "fb")
	if err != nil {
		t.Fatal(err)
	}
	reserve := map[store.AgentStep]float64{
		{StepRef: store.StepRef{Pipeline: "fb", Step: "score"}, Revision: rev, Fallback: true}: 0.6,
	}
	for _, run := range []string{"r1", "r2"} {
		e.firePipeline("fb", "daily", run)
		e.done(e.leaseFB("score"), out("", `{"score":1}`))
	}
	ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node1", Max: 10, TTL: time.Minute, Reserve: reserve,
		Slots: map[store.StepRef]int{{Pipeline: "fb", Step: "plain"}: 0, {Pipeline: "fb", Step: "report"}: 0}})
	if err != nil || len(ls) != 1 || !ls[0].Audit {
		t.Fatalf("Lease = %+v, %v; want one audit, the other over the day budget", ls, err)
	}
	as, _ := e.s.Audits(e.ctx, store.AuditFilter{Pipeline: "fb", State: store.AuditCancelled})
	if len(as) != 1 || as[0].Error == "" {
		t.Fatalf("cancelled audits = %+v", as)
	}
	var reserved float64
	if err := e.s.QueryRow(e.ctx, `SELECT SUM(reserved_usd) FROM audits`).Scan(&reserved); err != nil ||
		reserved != 0.6 {
		t.Fatalf("reserved = %v, %v", reserved, err)
	}
}

// TestFallbackEdges covers what the Abstain unit and audits refuse or skip: an abstention with invalid metrics, one
// for a cancelled run (cancelled, never handed off), audits of excluded, slot-less and expired items left alone or
// cancelled, an audit end with an invalid output or from another owner, and an audit lease ended by a revocation.
func TestFallbackEdges(t *testing.T, open Opener) {
	e := newEnv(t, open, fallbackPipeline)
	e.firePipeline("fb", "daily", "r1")
	l := e.leaseFB("score")
	_, err := e.s.Abstain(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: 1,
		Metrics: []store.Metric{{Name: ""}}})
	wantErr(t, err, store.ErrInvalid)
	if err := e.s.Cancel(e.ctx, "fb", "r1"); err != nil {
		t.Fatal(err)
	}
	if state := e.abstain(l, "unsure"); state != store.DeliveryCancelled {
		t.Fatalf("abstention in a cancelled run = %s, want cancelled", state)
	}

	for _, run := range []string{"r2", "r3"} {
		e.firePipeline("fb", "daily", run)
		e.done(e.leaseFB("score"), out("", `{"score":1}`))
	}
	score := store.StepRef{Pipeline: "fb", Step: "score"}
	for _, req := range []store.LeaseRequest{
		{Exclude: []store.StepRef{score}}, {Slots: map[store.StepRef]int{score: 0}},
		{PipelineSlots: map[string]int{"fb": 0}}, {Only: []store.StepRef{{Pipeline: "fb", Step: "plain"}}},
	} {
		req.Owner, req.Max, req.TTL = "node1", 10, time.Minute
		ls, err := e.s.Lease(e.ctx, req)
		if err != nil || slicesContainsAudit(ls) {
			t.Fatalf("Lease %+v = %+v, %v; want no audit", req, ls, err)
		}
	}
	as, _ := e.s.Audits(e.ctx, store.AuditFilter{Pipeline: "fb", RunID: "r2"})
	e.expire("fb", as[0].ItemID)
	ls, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node1", Max: 10, TTL: time.Minute,
		Slots: map[store.StepRef]int{{Pipeline: "fb", Step: "plain"}: 0, {Pipeline: "fb", Step: "report"}: 0}})
	if err != nil || len(ls) != 1 || !ls[0].Audit || ls[0].RunID != "r3" {
		t.Fatalf("Lease = %+v, %v; want r3's audit only", ls, err)
	}
	if as, _ := e.s.Audits(e.ctx, store.AuditFilter{Pipeline: "fb", RunID: "r2"}); as[0].State != store.AuditCancelled {
		t.Fatalf("audit of an expired item = %+v", as[0])
	}
	wantErr(t, e.s.EndAudit(e.ctx, store.AuditEnd{LeaseRef: ls[0].Ref(), Outputs: []store.Output{out("", `{`)}}),
		store.ErrInvalid)
	wantErr(t, e.s.EndAudit(e.ctx, store.AuditEnd{LeaseRef: ls[0].Ref(), Owner: "node2"}), store.ErrStaleLease)
	ended, err := e.s.SetNodeState(e.ctx, "node1", store.NodeRevoked, true)
	if err != nil || !containsRef(ended, ls[0].Ref()) {
		t.Fatalf("SetNodeState = %v, %v; want the audit's lease ended", ended, err)
	}
	if as, _ := e.s.Audits(e.ctx, store.AuditFilter{Pipeline: "fb", RunID: "r3"}); as[0].State != store.AuditFailed {
		t.Fatalf("audit of a revoked node = %+v", as[0])
	}
}

func slicesContainsAudit(ls []store.Lease) bool {
	for _, l := range ls {
		if l.Audit {
			return true
		}
	}
	return false
}

// TestAuditsSurfaceStoreErrors takes the audits table away under work in every state: the units that touch audits
// (Done sampling one, Lease, Renew, EndAudit, the expiry and a revocation ending audit leases, an erase dropping
// what audits hold) and the read fail with the store's error, and the store works again once it is back.
func TestAuditsSurfaceStoreErrors(t *testing.T, open Opener) {
	e := newEnv(t, open, fallbackPipeline)
	e.firePipeline("fb", "daily", "r1")
	e.done(e.leaseFB("score"), out("", `{"score":1}`))
	var audit store.Lease
	for _, l := range e.lease(10) {
		if l.Audit {
			audit = l
		}
	}
	e.firePipeline("fb", "daily", "r2")
	primary := e.leaseFB("score")
	if _, _, err := e.s.StartPurge(e.ctx, store.PurgeRequest{ID: "p1", Item: primary.Item.ID, Pipeline: "fb"}); err != nil {
		t.Fatal(err)
	}
	e.firePipeline("fb", "daily", "r3")
	covered := e.leaseFB("score")
	setTable(t, e, "audits", "audits_off")
	ref := audit.Ref()
	for name, err := range map[string]error{
		"done": second(e.s.Done(e.ctx, store.DoneRequest{LeaseRef: covered.Ref(), Attempt: 1,
			Outputs: []store.Output{out("", `{"score":2}`)}})),
		"renew":  second(e.s.Renew(e.ctx, "node1", []store.LeaseRef{ref}, time.Minute)),
		"end":    e.s.EndAudit(e.ctx, store.AuditEnd{LeaseRef: ref}),
		"audits": second(e.s.Audits(e.ctx, store.AuditFilter{})),
		"erase":  second(e.s.ErasePurge(e.ctx, "p1")),
		"revoke": second(e.s.SetNodeState(e.ctx, "node1", store.NodeRevoked, true)),
		"expire": second(e.s.ExpireLeases(e.ctx)),
		"lease": second(e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node2", Max: 1, TTL: time.Minute,
			Exclude: []store.StepRef{{Pipeline: "fb", Step: "plain"}, {Pipeline: "fb", Step: "report"}}})),
	} {
		if err == nil {
			t.Errorf("%s succeeded without the audits table", name)
		}
	}
	setTable(t, e, "audits_off", "audits")
	if err := e.s.EndAudit(e.ctx, store.AuditEnd{LeaseRef: ref}); err != nil {
		t.Fatalf("EndAudit once the table is back: %v", err)
	}
}

func second[T any](_ T, err error) error { return err }
