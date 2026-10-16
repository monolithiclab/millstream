//go:build e2e

package sqlite

import (
	"strings"
	"testing"

	"github.com/monolithiclab/millstream/internal/store"
)

// Fail variants the e2e build adds: the default build covers the same paths through the other Fail tests.

func TestFailNonRetryableIsDeadAtOnce(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	wantState(t, "state", e.fail(e.lease(1)[0], false), store.DeliveryDead)
	if d := e.deliveries("r1")[0]; d.Attempt != 1 {
		t.Fatalf("attempt %d", d.Attempt)
	}
}

func TestFailTruncatesMessage(t *testing.T) {
	e := newEnv(t, fixture)
	e.fire("r1")
	l := e.lease(1)[0]
	long := strings.Repeat("é", store.MaxErrorMessage) // two bytes each
	if _, err := e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: 1, Message: long, CostUSD: 1.5}); err != nil {
		t.Fatal(err)
	}
	d := e.deliveries("r1")[0]
	if len(d.ErrorMessage) != store.MaxErrorMessage || d.ErrorMessage != long[:store.MaxErrorMessage] || d.CostUSD != 1.5 {
		t.Fatalf("message %d bytes, cost %v", len(d.ErrorMessage), d.CostUSD)
	}
	// Without a class, the dead letter carries the bare message.
	if dls := e.deadLetters("r1"); dls[0].Error != d.ErrorMessage {
		t.Fatal("dead-letter error differs from the delivery's")
	}
}
