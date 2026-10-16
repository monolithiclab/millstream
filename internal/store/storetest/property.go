package storetest

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/ids"
	"github.com/monolithiclab/millstream/internal/store"
)

// TestRandomUnitSequences runs seeded random sequences of units on the fixture and checks the store's invariants
// after every step: a sequence of propertySteps steps per seed of propertySeeds, a short set by default and the long
// run under the e2e tag.
func TestRandomUnitSequences(t *testing.T, open Opener) {
	for _, seed := range propertySeeds {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 42)) // #nosec G404 -- a reproducible test schedule
			m := &model{e: newEnv(t, open, fixture), rng: rng}
			for step := range propertySteps {
				op := m.step()
				if err := m.check(); err != nil {
					t.Fatalf("step %d (%s): %v", step, op, err)
				}
			}
		})
	}
}

// model drives the store and remembers what a node would: the leases it holds.
type model struct {
	e      *env
	rng    *rand.Rand
	runs   []string
	held   []store.Lease
	fixed  bool
	nextID int
}

func (m *model) step() string {
	e, r := m.e, m.rng
	switch k := r.IntN(100); {
	case k < 12:
		m.nextID++
		id := fmt.Sprint("r", m.nextID)
		e.fire(id)
		m.runs = append(m.runs, id)
		return "fire"
	case k < 30:
		m.held = append(m.held, e.lease(1+r.IntN(4))...)
		return "lease"
	case k < 55:
		if l, ok := m.takeLease(); ok {
			m.expect(e.s.Done(e.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Outputs: m.outputs()}))
		}
		return "done"
	case k < 65:
		if l, ok := m.takeLease(); ok {
			m.expect(e.s.Fail(e.ctx, store.FailRequest{LeaseRef: l.Ref(), Attempt: l.Attempt, Class: "x",
				Retryable: r.IntN(2) == 0}))
		}
		return "fail"
	case k < 70:
		if len(m.held) > 0 {
			refs := make([]store.LeaseRef, len(m.held))
			for i, l := range m.held {
				refs[i] = l.Ref()
			}
			if _, err := e.s.Renew(e.ctx, "node1", refs, time.Minute); err != nil {
				e.t.Fatalf("Renew: %v", err)
			}
		}
		return "renew"
	case k < 78:
		e.clock.Advance(time.Duration(r.IntN(90)) * time.Second)
		if _, err := e.s.ExpireLeases(e.ctx); err != nil {
			e.t.Fatalf("ExpireLeases: %v", err)
		}
		return "expire"
	case k < 82:
		if run, ok := m.pickRun(); ok {
			if err := e.s.Cancel(e.ctx, "listings", run); err != nil && !errors.Is(err, store.ErrConflict) {
				e.t.Fatalf("Cancel: %v", err)
			}
		}
		return "cancel"
	case k < 90:
		if run, ok := m.pickRun(); ok {
			_, err := e.s.ReplayDeadLetters(e.ctx, "listings", run, nil, m.fixed && r.IntN(2) == 0)
			if err != nil && !errors.Is(err, store.ErrConflict) {
				e.t.Fatalf("Replay: %v", err)
			}
		}
		return "replay"
	case k < 95:
		dls, _ := e.s.DeadLetters(e.ctx, store.DeadLetterFilter{State: store.DeadLetterOpen})
		if len(dls) > 0 {
			if _, err := e.s.DiscardDeadLetters(e.ctx, "listings", []string{dls[r.IntN(len(dls))].ID}); err != nil {
				e.t.Fatalf("Discard: %v", err)
			}
		}
		return "discard"
	case k < 97:
		if !m.fixed {
			e.apply(fixtureFixed)
			m.fixed = true
		}
		return "apply"
	default:
		// A message from an owner that lost its lease.
		if l, ok := m.takeLease(); ok {
			ref := l.Ref()
			ref.FencingToken += 100
			_, err := e.s.Done(e.ctx, store.DoneRequest{LeaseRef: ref, Attempt: l.Attempt})
			if !errors.Is(err, store.ErrStaleLease) {
				e.t.Fatalf("stale Done = %v", err)
			}
		}
		return "stale"
	}
}

// expect accepts a Done or Fail outcome: a held lease may have expired or been re-leased meanwhile.
func (m *model) expect(_ store.DeliveryState, err error) {
	if err != nil && !errors.Is(err, store.ErrStaleLease) {
		m.e.t.Fatalf("unit: %v", err)
	}
}

func (m *model) takeLease() (store.Lease, bool) {
	if len(m.held) == 0 {
		return store.Lease{}, false
	}
	i := m.rng.IntN(len(m.held))
	l := m.held[i]
	m.held = append(m.held[:i], m.held[i+1:]...)
	return l, true
}

func (m *model) pickRun() (string, bool) {
	if len(m.runs) == 0 {
		return "", false
	}
	return m.runs[m.rng.IntN(len(m.runs))], true
}

// outputs returns zero to three outputs, sometimes breaking the key rule, sometimes unroutable or with a score
// routes cannot compare.
func (m *model) outputs() []store.Output {
	r := m.rng
	n := r.IntN(4)
	outs := make([]store.Output, n)
	for i := range outs {
		key := fmt.Sprint("k", r.IntN(3))
		if r.IntN(10) == 0 {
			key = ""
		}
		score := fmt.Sprint(r.IntN(120) - 10)
		if r.IntN(20) == 0 {
			score = `"high"`
		}
		outs[i] = out(key, `{"score":`+score+`}`)
	}
	return outs
}

// consumers of the fixture's streams (fixtureFixed has the same).
var consumers = map[string][]string{
	"trigger.daily":  {"crawl"},
	"listings.raw":   {"score"},
	"listings.audit": {"audit"},
	"listings.good":  {"report"},
	"listings.rest":  {"report"},
	"crawl.dlq":      {"alert"},
	"score.dlq":      {"alert"},
}

// check verifies the invariants against the store's tables.
func (m *model) check() error {
	e := m.e
	evs := e.events()
	for i, ev := range evs {
		if ev.Seq != int64(i+1) {
			return fmt.Errorf("event %d has seq %d", i, ev.Seq)
		}
	}
	ds := e.deliveries("")
	byKey := map[string]store.Delivery{}
	live := map[string]int{}
	for _, d := range ds {
		switch d.State {
		case store.DeliveryLeased:
			if d.LeaseOwner == "" || d.LeaseExpiresAt.IsZero() {
				return fmt.Errorf("leased delivery %s without owner or expiry", d.ID)
			}
		case store.DeliveryPending, store.DeliveryDone, store.DeliveryDead, store.DeliveryCancelled:
			if d.LeaseOwner != "" {
				return fmt.Errorf("%s delivery %s keeps owner %s", d.State, d.ID, d.LeaseOwner)
			}
		default:
			return fmt.Errorf("delivery %s in state %q", d.ID, d.State)
		}
		if !d.State.Terminal() {
			live[d.RunID]++
		}
		byKey[d.Step+"\x00"+d.ItemID] = d
	}
	items := e.items("")
	seen := map[string]bool{}
	for _, it := range items {
		if seen[it.ID] {
			return fmt.Errorf("item %s twice", it.ID)
		}
		seen[it.ID] = true
		for _, step := range consumers[it.Stream] {
			d, ok := byKey[step+"\x00"+it.ID]
			if !ok || d.ID != ids.Delivery(step, it.ID, it.Generation) {
				return fmt.Errorf("item %s on %s has no delivery for %s", it.ID, it.Stream, step)
			}
		}
	}
	dls := e.deadLetters("")
	errs := map[string]int{}
	deadLetterOf := map[string]store.DeadLetter{}
	for _, dl := range dls {
		if dl.State == store.DeadLetterOpen || dl.State == store.DeadLetterDiscarded {
			errs[dl.RunID]++
		}
		if dl.Kind == store.DeadLetterDead {
			deadLetterOf[dl.OriginDelivery] = dl
		}
	}
	for _, d := range ds {
		if d.State == store.DeliveryDead {
			if dl, ok := deadLetterOf[d.ID]; !ok || dl.State == store.DeadLetterReplayed {
				return fmt.Errorf("dead delivery %s without a current dead letter", d.ID)
			}
		}
	}
	runs, err := e.s.Runs(e.ctx, store.RunFilter{})
	if err != nil {
		return err
	}
	for _, r := range runs {
		settled := !r.SettledAt.IsZero()
		if settled != (live[r.ID] == 0) {
			return fmt.Errorf("run %s settled=%v with %d live deliveries", r.ID, settled, live[r.ID])
		}
		var want store.RunState
		switch {
		case r.State == store.RunCancelled:
			continue
		case !settled:
			want = store.RunRunning
		case errs[r.ID] > 0:
			want = store.RunCompletedWithErrors
		default:
			want = store.RunSucceeded
		}
		if r.State != want {
			return fmt.Errorf("run %s is %s, want %s (%d dead letters)", r.ID, r.State, want, errs[r.ID])
		}
	}
	return nil
}
