package nodewire

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

func TestConversionsRoundTrip(t *testing.T) {
	refs := []store.LeaseRef{
		{Pipeline: "listings", DeliveryID: "d1", Step: "score", FencingToken: 7},
		{Pipeline: "other", DeliveryID: "d2", Step: "fetch", FencingToken: 9},
	}
	if got := FromRef(refs[0]).ToRef(); got != refs[0] {
		t.Errorf("FromRef/ToRef = %+v", got)
	}
	if got := FromRef(refs[1]); got != (LeaseRef{Pipeline: "other", DeliveryID: "d2", Step: "fetch", FencingToken: 9}) {
		t.Errorf("FromRef = %+v", got)
	}
	if got := ToRefs(FromRefs(refs)); !reflect.DeepEqual(got, refs) {
		t.Errorf("FromRefs/ToRefs = %+v", got)
	}
	if got := ToRefs(FromRefs(nil)); got == nil || len(got) != 0 {
		t.Errorf("FromRefs/ToRefs of none = %#v, want an empty list", got)
	}

	arts := []store.ArtifactRef{{Name: "a.txt", SHA256: "ab", Size: 3, Type: "text/plain"}}
	lease := store.Lease{
		DeliveryID: "d1", Pipeline: "listings", RunID: "run_1", Revision: 4, Step: "score", Generation: 2,
		Attempt: 3, FencingToken: 7, ExpiresAt: time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC),
		Item: store.Item{ID: "it_1", Key: "key-1", Pipeline: "listings", Artifacts: arts},
	}
	wire := FromLease(lease)
	if wire.ItemID != "it_1" || wire.ItemKey != "key-1" || wire.Revision != 4 || !reflect.DeepEqual(wire.Artifacts, arts) {
		t.Errorf("FromLease = %+v", wire)
	}
	if got := wire.ToLease(); !reflect.DeepEqual(got, lease) {
		t.Errorf("FromLease/ToLease\n got %+v\nwant %+v", got, lease)
	}

	outs := []store.Output{
		{Key: "k1", Payload: json.RawMessage(`{"a":1}`), Artifacts: arts},
		{Key: "k2", Payload: json.RawMessage(`null`)},
	}
	if got := ToOutputs(FromOutputs(outs)); !reflect.DeepEqual(got, outs) {
		t.Errorf("FromOutputs/ToOutputs = %+v", got)
	}
	if got := FromOutputs(nil); got == nil || len(got) != 0 {
		t.Errorf("FromOutputs of none = %#v, want an empty list", got)
	}
}
