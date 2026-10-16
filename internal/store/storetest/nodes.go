package storetest

import (
	"slices"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func TestNodes(t *testing.T, open Opener) {
	e := newEnv(t, open, fixture)
	reg := store.Node{ID: "node1", Version: "1.0", Labels: map[string]string{"gpu": "true"},
		Runtimes: []string{"exec", "container"}, Slots: 4}
	if err := e.s.RegisterNode(e.ctx, reg); err != nil {
		t.Fatal(err)
	}
	e.fire("r1")
	e.fire("r2")
	ls := e.lease(2)
	e.clock.Advance(10 * time.Second)
	if _, err := e.s.Renew(e.ctx, "node1", nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	ns, err := e.s.Nodes(e.ctx)
	if err != nil || len(ns) != 1 {
		t.Fatalf("Nodes = %+v, %v", ns, err)
	}
	n := ns[0]
	if n.ID != "node1" || n.Version != "1.0" || n.Labels["gpu"] != "true" || !slices.Equal(n.Runtimes, reg.Runtimes) ||
		n.Slots != 4 || n.State != store.NodeActive || !n.RegisteredAt.Equal(t0) ||
		!n.LastHeartbeat.Equal(t0.Add(10*time.Second)) {
		t.Fatalf("node = %+v; want its registration, heard from at the renewal", n)
	}

	// Draining changes no lease; registering again from a new process makes it active.
	if ended, err := e.s.SetNodeState(e.ctx, "node1", store.NodeDraining, false); err != nil || len(ended) != 0 {
		t.Fatalf("drain = %v, %v", ended, err)
	}
	if ns, _ := e.s.Nodes(e.ctx); ns[0].State != store.NodeDraining {
		t.Fatalf("drained node = %+v", ns[0])
	}
	if err := e.s.RegisterNode(e.ctx, store.Node{ID: "node1", Version: "1.1"}); err != nil {
		t.Fatal(err)
	}
	if ns, _ := e.s.Nodes(e.ctx); ns[0].State != store.NodeActive || ns[0].Version != "1.1" ||
		len(ns[0].Labels) != 0 || ns[0].Runtimes == nil {
		t.Fatalf("registered again = %+v", ns[0])
	}

	// Revoking ends its leases at once, each attempt consumed as an expiry's; the id is refused from then on.
	ended, err := e.s.SetNodeState(e.ctx, "node1", store.NodeRevoked, true)
	if err != nil || len(ended) != 2 || ended[0] != ls[0].Ref() || ended[1] != ls[1].Ref() {
		t.Fatalf("revoke = %+v, %v; want both leases ended under their tokens", ended, err)
	}
	for _, run := range []string{"r1", "r2"} {
		d := e.deliveries(run)[0]
		if d.State != store.DeliveryPending || d.Attempt != 2 || d.ErrorClass != store.ClassLeaseExpired {
			t.Fatalf("delivery of a revoked node = %+v; want retried as a lease expiry", d)
		}
	}
	_, err = e.s.Done(e.ctx, store.DoneRequest{LeaseRef: ls[0].Ref(), Attempt: 1})
	wantErr(t, err, store.ErrStaleLease)
	wantErr(t, e.s.RegisterNode(e.ctx, reg), store.ErrRevoked)
	_, err = e.s.Renew(e.ctx, "node1", nil, time.Minute)
	wantErr(t, err, store.ErrRevoked)
	if _, err := e.s.SetNodeState(e.ctx, "node1", store.NodeDraining, false); err != nil {
		t.Fatal(err)
	}
	if ns, _ := e.s.Nodes(e.ctx); ns[0].State != store.NodeRevoked {
		t.Fatalf("a revoked node drained = %+v; want it revoked still", ns[0])
	}

	// Revoking an id never registered records it; draining one is not found. A node leaving ends its leases.
	if _, err := e.s.SetNodeState(e.ctx, "ghost", store.NodeRevoked, true); err != nil {
		t.Fatal(err)
	}
	wantErr(t, e.s.RegisterNode(e.ctx, store.Node{ID: "ghost"}), store.ErrRevoked)
	_, err = e.s.SetNodeState(e.ctx, "nobody", store.NodeDraining, false)
	wantErr(t, err, store.ErrNotFound)
	if err := e.s.RegisterNode(e.ctx, store.Node{ID: "node2"}); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(time.Minute) // past the retry backoff
	held, err := e.s.Lease(e.ctx, store.LeaseRequest{Owner: "node2", Max: 1, TTL: time.Minute})
	if err != nil || len(held) != 1 {
		t.Fatalf("lease again = %v, %v", held, err)
	}
	if ended, err := e.s.SetNodeState(e.ctx, "node2", store.NodeDraining, true); err != nil || len(ended) != 1 ||
		ended[0] != held[0].Ref() {
		t.Fatalf("leave = %v, %v", ended, err)
	}
	if ns, _ := e.s.Nodes(e.ctx); len(ns) != 3 || ns[0].ID != "ghost" || !ns[0].RegisteredAt.IsZero() ||
		ns[2].State != store.NodeDraining {
		t.Fatalf("nodes = %+v", ns)
	}

	for _, n := range []store.Node{{}, {ID: "x", Slots: -1}} {
		wantErr(t, e.s.RegisterNode(e.ctx, n), store.ErrInvalid)
	}
	_, err = e.s.SetNodeState(e.ctx, "node2", store.NodeActive, false)
	wantErr(t, err, store.ErrInvalid)
}
