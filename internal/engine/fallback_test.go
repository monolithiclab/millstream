package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// TestFallbackAndAudit runs a step backed by a fallback through the engine (spec section 11.5): an abstention
// reported as a Fail of class abstain goes to the Abstain unit and the fallback action runs next; a delivery the
// primary covers is audited by the fallback with audit set, the audit's end routed to EndAudit, and run --once waits
// for it while the run itself is settled.
func TestFallbackAndAudit(t *testing.T) {
	v := newEnv(t, Options{})
	v.apply(newPackage(t, manualPipeline("p", `  score:
    action: ./actions/rules
    in: trigger.adhoc
    fallback: { action: ./actions/backup, auditRate: 1 }
`), "rules", "backup"))

	run1 := v.fire("p", "adhoc", "r1")
	as, err := v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 || as[0].Launch.Ref != "./actions/rules" || as[0].ItemMsg.Audit {
		t.Fatalf("primary assignment = %+v, %v", as, err)
	}
	if err := v.e.Fail(v.ctx, store.FailRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 1, Class: store.ClassAbstain,
		Message: "unsure"}); err != nil {
		t.Fatal(err)
	}
	as, err = v.e.Pull(v.ctx, "node-a", 1)
	if err != nil || len(as) != 1 || as[0].Launch.Ref != "./actions/backup" || !as[0].Lease.Fallback ||
		as[0].ItemMsg.Attempt != 2 {
		t.Fatalf("fallback assignment = %+v, %v", as, err)
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 2}); err != nil {
		t.Fatal(err)
	}
	if r := v.run("p", run1); r.State != store.RunSucceeded {
		t.Fatalf("run %s is %s", run1, r.State)
	}

	run2 := v.fire("p", "adhoc", "r2")
	as, _ = v.e.Pull(v.ctx, "node-a", 1)
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 1,
		Outputs: []store.Output{{Payload: json.RawMessage(`{"score":1}`)}}}); err != nil {
		t.Fatal(err)
	}
	refs := []RunRef{{Pipeline: "p", ID: run2}}
	if ok, err := v.e.Settled(v.ctx, refs); err != nil || !ok {
		t.Fatalf("the run with a pending audit is not settled: %v", err)
	}
	if ok, err := v.e.auditsEnded(v.ctx, refs); err != nil || ok {
		t.Fatalf("auditsEnded = %v, %v; want the pending audit", ok, err)
	}
	as, err = v.e.Pull(v.ctx, "node-a", 2)
	if err != nil || len(as) != 1 || !as[0].Lease.Audit || !as[0].ItemMsg.Audit ||
		as[0].Launch.Ref != "./actions/backup" {
		t.Fatalf("audit assignment = %+v, %v", as, err)
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: as[0].Lease.Ref(), Attempt: 1,
		Outputs: []store.Output{{Payload: json.RawMessage(`{"score":2}`)}}}); err != nil {
		t.Fatal(err)
	}
	if v.e.lookup(as[0].Lease.Ref()).token != 0 {
		t.Fatal("the audit still counts in flight")
	}
	ctx, cancel := context.WithTimeout(v.ctx, time.Second)
	defer cancel()
	if err := v.e.WaitSettled(ctx, refs); err != nil {
		t.Fatal(err)
	}
	audits, err := v.st.Audits(v.ctx, store.AuditFilter{Pipeline: "p", RunID: run2})
	if err != nil || len(audits) != 1 || audits[0].Agreement == nil || *audits[0].Agreement != 0 {
		t.Fatalf("audits = %+v, %v", audits, err)
	}
}
