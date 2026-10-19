//go:build e2e

package nodeapi_test

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/store"
	"github.com/monolithiclab/millstream/internal/store/sqlite"
)

// Variants the e2e build adds: the default build covers the same paths through the other tests.

// Retention and purges treat a remote node's lease as the embedded node's: the same store units decide, whoever
// the owner (spec section 9.3). Retention keeps the input of a delivery a remote node holds; a purge fences it, and
// the node's report then cancels the delivery and discards its outputs.
func TestRemoteLeaseUnderRetentionAndPurge(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Date(2026, 10, 17, 6, 0, 0, 0, time.UTC).UnixNano())
	d := newDeploymentWith(t, sqlite.Options{Now: func() time.Time { return time.Unix(0, now.Load()).UTC() }})
	n1 := d.transport("n1", "https://a:7070")
	run, _, err := d.e.Fire(d.ctx, "remote", "go", "k1", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	w := d.pull(n1, "n1")
	now.Add(int64(time.Second)) // the input is older than its stream's retention; the lease holds
	res, err := d.st.Retain(d.ctx, store.RetentionRequest{Streams: []store.StreamRetention{
		{Pipeline: "remote", Stream: "trigger.go", Age: time.Millisecond}}})
	if err != nil || res.Items != 0 {
		t.Fatalf("retention of a remote lease's input = %+v, %v", res, err)
	}
	p, _, err := d.e.Purge(d.ctx, engine.PurgeRequest{Key: "k1", RequestID: "remote"})
	if err != nil || p.State != store.PurgeDone || p.Erased != 1 {
		t.Fatalf("purge = %+v, %v", p, err)
	}
	if err := n1.Done(d.ctx, store.DoneRequest{LeaseRef: w.Lease.Ref(), Attempt: w.Lease.Attempt,
		Outputs: []store.Output{{Key: "p1", Payload: json.RawMessage(`{}`)}}}); err != nil {
		t.Fatal(err)
	}
	ds, err := d.st.Deliveries(d.ctx, store.DeliveryFilter{Pipeline: "remote", RunID: run})
	if err != nil || len(ds) != 1 || ds[0].State != store.DeliveryCancelled {
		t.Fatalf("deliveries after the report = %+v, %v", ds, err)
	}
	if items, err := d.st.Items(d.ctx, store.ItemFilter{Pipeline: "remote", Stream: "pages"}); err != nil ||
		len(items) != 0 {
		t.Fatalf("outputs of a fenced delivery = %+v, %v", items, err)
	}
}
