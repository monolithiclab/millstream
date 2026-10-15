package store

import (
	"errors"
	"testing"
)

func TestDeliveryStateTerminal(t *testing.T) {
	t.Parallel()
	tests := map[DeliveryState]bool{
		DeliveryPending: false, DeliveryLeased: false, DeliveryAwaitingReview: false,
		DeliveryDone: true, DeliveryFiltered: true, DeliveryDead: true, DeliveryCancelled: true,
	}
	for state, want := range tests {
		if got := state.Terminal(); got != want {
			t.Errorf("%s.Terminal() = %v, want %v", state, got, want)
		}
	}
}

func TestLeaseRef(t *testing.T) {
	t.Parallel()
	l := Lease{Pipeline: "p", DeliveryID: "dl_1", FencingToken: 4, RunID: "r"}
	if got, want := l.Ref(), (LeaseRef{Pipeline: "p", DeliveryID: "dl_1", FencingToken: 4}); got != want {
		t.Errorf("Ref() = %+v, want %+v", got, want)
	}
}

func TestRevisionDecode(t *testing.T) {
	t.Parallel()
	r := Revision{ID: 3, PipelineJSON: []byte(`{"Name":"listings","Vars":{"maxPrice":400000}}`)}
	p, err := r.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "listings" || p.Vars["maxPrice"] != int64(400000) {
		t.Errorf("decoded %+v", p)
	}
	if _, err := (Revision{ID: 4, PipelineJSON: []byte("{")}).Decode(); err == nil || errors.Unwrap(err) == nil {
		t.Errorf("Decode of bad JSON = %v", err)
	}
}
