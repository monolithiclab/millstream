package engine

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/store"

	"github.com/monolithiclab/millstream/internal/testdir"
)

const placedPipeline = `
pipeline: placed
triggers: { go: { manual: {} } }
placement: { labels: { zone: eu } }
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.go
    out: pages
  score:
    action: ./actions/score
    in: pages
    placement: { labels: { gpu: "true" } }
`

// TestNodePlacement: a registered node leases only the steps whose runtime and labels it matches; a step no node
// matches is unschedulable; draining stops its leases, a revocation ends them and refuses the node.
func TestNodePlacement(t *testing.T) {
	v := newEnv(t, Options{})
	pkg := newPackage(t, placedPipeline, "crawl", "score")
	v.apply(pkg)
	v.fire("placed", "go", "k1")

	if err := v.e.CheckNode("cpu"); !errors.Is(err, ErrUnregistered) {
		t.Fatalf("CheckNode before registering: %v", err)
	}
	for _, n := range []NodeInfo{
		{ID: "cpu", Version: "v", Labels: map[string]string{"zone": "us"}, Runtimes: []string{"exec"}, Slots: 2},
		{ID: "gpu", Version: "v", Labels: map[string]string{"zone": "us", "gpu": "true"}, Runtimes: []string{"exec"}},
	} {
		if err := v.e.RegisterNode(v.ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	if a, b, c := v.e.capSlots("cpu", 4), v.e.capSlots("gpu", 4), v.e.capSlots("unknown", 4); a != 2 || b != 4 || c != 4 {
		t.Fatalf("pulls capped at %d, %d, %d; want the registered slots, else what was asked", a, b, c)
	}
	un, err := v.e.Unschedulable(v.ctx)
	if err != nil || len(un) != 1 || un[0].Step != "crawl" || un[0].Pending != 1 ||
		un[0].Placement.String() != "runtime exec, labels zone=eu" {
		t.Fatalf("Unschedulable = %+v, %v; want crawl, its zone matched by no node", un, err)
	}
	if as, err := v.e.Pull(v.ctx, "cpu", 4); err != nil || len(as) != 0 {
		t.Fatalf("the cpu node in the wrong zone leased %d, %v", len(as), err)
	}
	if as, err := v.e.Pull(v.ctx, "gpu", 4); err != nil || len(as) != 0 {
		t.Fatalf("the gpu node leased crawl, whose zone it is not in: %d, %v", len(as), err)
	}
	if err := v.e.RegisterNode(v.ctx, NodeInfo{ID: "eu", Labels: map[string]string{"zone": "eu"},
		Runtimes: []string{"container", "exec"}}); err != nil {
		t.Fatal(err)
	}
	if un, _ := v.e.Unschedulable(v.ctx); len(un) != 0 {
		t.Fatalf("Unschedulable with an eu node = %+v", un)
	}
	as, err := v.e.Pull(v.ctx, "eu", 4)
	if err != nil || len(as) != 1 || as[0].Lease.Step != "crawl" || as[0].Launch.PackageHash != pkg.Hash ||
		as[0].Launch.Ref != "./actions/crawl" {
		t.Fatalf("eu node pulled %+v, %v", as, err)
	}
	held := as[0].Lease.Ref()

	// Capabilities: only the holder fetches the package, and only its run's.
	if _, err := v.e.HeldPackage(v.ctx, "gpu", held, pkg.Hash); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("another node fetched the package: %v", err)
	}
	if _, err := v.e.HeldPackage(v.ctx, "eu", held, strings.Repeat("0", 64)); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("the holder fetched another package: %v", err)
	}
	if b, err := v.e.HeldPackage(v.ctx, "eu", held, pkg.Hash); err != nil || len(b) == 0 {
		t.Fatalf("HeldPackage = %d bytes, %v", len(b), err)
	}
	if _, err := v.e.HeldInput(v.ctx, "eu", held, strings.Repeat("a", 64)); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("an artifact the delivery does not carry: %v", err)
	}
	if !v.e.HeldElsewhere("gpu", held) || v.e.HeldElsewhere("eu", held) {
		t.Fatal("HeldElsewhere does not tell the holder")
	}

	// Draining stops the node's leases; a revocation ends the lease it holds and refuses it.
	if err := v.e.SetNodeState(v.ctx, "eu", store.NodeDraining, false); err != nil {
		t.Fatal(err)
	}
	v.fire("placed", "go", "k2")
	if as, _ := v.e.Pull(v.ctx, "eu", 4); len(as) != 0 {
		t.Fatalf("a draining node leased %d", len(as))
	}
	if un, _ := v.e.Unschedulable(v.ctx); len(un) != 1 {
		t.Fatalf("Unschedulable with the eu node draining = %+v", un)
	}
	if err := v.e.SetNodeState(v.ctx, "eu", store.NodeRevoked, false); err != nil {
		t.Fatal(err)
	}
	if err := v.e.CheckNode("eu"); !errors.Is(err, store.ErrRevoked) {
		t.Fatalf("CheckNode after revoking: %v", err)
	}
	if d := v.deliveries("run_k1_go")[0]; d.State != store.DeliveryPending || d.ErrorClass != store.ClassLeaseExpired {
		t.Fatalf("the revoked node's delivery = %+v", d)
	}
	if err := v.e.RegisterNode(v.ctx, NodeInfo{ID: "eu"}); !errors.Is(err, store.ErrRevoked) {
		t.Fatalf("a revoked node registered again: %v", err)
	}

	// A revocation recorded by another process is refused at the node's next heartbeat.
	if _, err := v.st.SetNodeState(v.ctx, "cpu", store.NodeRevoked, true); err != nil {
		t.Fatal(err)
	}
	if _, err := v.e.NodeHeartbeat(v.ctx, "cpu", nil); !errors.Is(err, store.ErrRevoked) {
		t.Fatalf("heartbeat of a node revoked elsewhere: %v", err)
	}
	if _, err := v.e.NodeHeartbeat(v.ctx, "gpu", nil); err != nil {
		t.Fatal(err)
	}

	// Health follows the heartbeats; a node lost takes nothing.
	if h := Health(t0, t0.Add(20*time.Second), time.Minute); h != HealthLate {
		t.Fatalf("health after 20 s = %s", h)
	}
	if h := Health(time.Time{}, t0, time.Minute); h != HealthLost {
		t.Fatalf("health never heard from = %s", h)
	}
	v.clock.Advance(2 * time.Minute)
	if un, _ := v.e.Unschedulable(v.ctx); len(un) != 1 {
		t.Fatalf("Unschedulable = %+v", un)
	}
}

// A node holding a delivery gets the delivery's input artifacts, and may put outputs; another node neither.
func TestHeldInput(t *testing.T) {
	v := newEnv(t, Options{Artifacts: artifacts.New(testdir.New(t))})
	v.apply(newPackage(t, placedPipeline, "crawl", "score"))
	v.fire("placed", "go", "k1")
	for _, n := range []NodeInfo{
		{ID: "eu", Labels: map[string]string{"zone": "eu"}, Runtimes: []string{"exec"}},
		{ID: "gpu", Labels: map[string]string{"gpu": "true"}, Runtimes: []string{"exec"}},
	} {
		if err := v.e.RegisterNode(v.ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	as, err := v.e.Pull(v.ctx, "eu", 1)
	if err != nil || len(as) != 1 {
		t.Fatalf("pull crawl = %v, %v", as, err)
	}
	crawl := as[0].Lease.Ref()
	if err := v.e.HoldsLease("eu", crawl); err != nil {
		t.Fatal(err)
	}
	if err := v.e.HoldsLease("gpu", crawl); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("HoldsLease of another node: %v", err)
	}
	ref, err := v.e.PutArtifact(v.ctx, crawl, "page.html", strings.NewReader("<p>listing</p>"))
	if err != nil {
		t.Fatal(err)
	}
	if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: crawl, Attempt: 1, Outputs: []store.Output{
		{Key: "l1", Payload: []byte(`{}`), Artifacts: []store.ArtifactRef{ref}}}}); err != nil {
		t.Fatal(err)
	}
	as, err = v.e.Pull(v.ctx, "gpu", 1)
	if err != nil || len(as) != 1 || as[0].Lease.Step != "score" {
		t.Fatalf("pull score = %v, %v", as, err)
	}
	score := as[0].Lease.Ref()
	if got, err := v.e.HeldInput(v.ctx, "gpu", score, ref.SHA256); err != nil || got != ref {
		t.Fatalf("HeldInput = %+v, %v", got, err)
	}
	if _, err := v.e.HeldInput(v.ctx, "eu", score, ref.SHA256); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("HeldInput of another node: %v", err)
	}
	none := newEnv(t, Options{})
	none.e.inflight[score.Key()] = flight{token: score.FencingToken, owner: "gpu", itemID: as[0].Lease.Item.ID}
	if _, err := none.e.HeldInput(none.ctx, "gpu", score, ref.SHA256); err == nil {
		t.Fatal("HeldInput without the item")
	}
}

// The projector raises an unschedulable insight for a step no registered node can take.
func TestUnschedulableInsight(t *testing.T) {
	v := newEnv(t, Options{})
	v.apply(newPackage(t, placedPipeline, "crawl", "score"))
	v.fire("placed", "go", "k1")
	if err := v.e.RegisterNode(v.ctx, NodeInfo{ID: "cpu", Labels: map[string]string{"zone": "us"},
		Runtimes: []string{"exec"}}); err != nil {
		t.Fatal(err)
	}
	if err := v.e.Project(v.ctx); err != nil {
		t.Fatal(err)
	}
	ins, err := v.st.Insights(v.ctx, store.InsightFilter{Pipeline: "placed"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, in := range ins {
		found = found || (in.Kind == "unschedulable" && in.Scope == "crawl" &&
			strings.Contains(string(in.Evidence), `"needs":"runtime exec, labels zone=eu"`))
	}
	if !found {
		t.Fatalf("insights = %+v", ins)
	}
}

// TestBuiltinsIgnorePlacement: a builtin step runs in the engine whatever its pipeline's labels, so no node matching
// them makes it unschedulable.
func TestBuiltinsIgnorePlacement(t *testing.T) {
	v := newEnv(t, Options{})
	v.apply(newPackage(t, `pipeline: placed
triggers: { go: { manual: {} } }
placement: { labels: { zone: eu } }
steps:
  gate: { action: filter, in: trigger.go, config: { expr: "true" } }
`))
	id := v.fire("placed", "go", "k1")
	if err := v.e.RegisterNode(v.ctx, NodeInfo{ID: "cpu", Labels: map[string]string{"zone": "us"},
		Runtimes: []string{"exec"}}); err != nil {
		t.Fatal(err)
	}
	if un, err := v.e.Unschedulable(v.ctx); err != nil || len(un) != 0 {
		t.Fatalf("Unschedulable = %+v, %v; a builtin is the engine's", un, err)
	}
	v.builtins()
	if d := v.deliveries(id)[0]; d.State != store.DeliveryDone {
		t.Fatalf("gate delivery = %+v", d)
	}
}
