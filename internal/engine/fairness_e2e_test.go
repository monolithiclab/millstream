//go:build e2e

package engine

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/monolithiclab/millstream/internal/store"
)

// Many nodes pulling batches and reporting concurrently, over pipelines with random limits and step concurrency:
// no pipeline ever holds more than its limit in flight, no step more than its concurrency, the pipelines without a
// limit are served within one delivery of each other while they all have a backlog, and every limited pipeline is
// served (spec sections 4.10 and 5.9).
func TestRoundRobinProperty(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	v := newEnv(t, Options{LeaseTTL: time.Hour})

	const backlog, nodes = 30, 6
	limits := map[string]int{}      // pipeline: limits.concurrency, absent without one
	concurrency := map[string]int{} // pipeline/step: concurrency
	var unlimited []string
	for i := range 6 {
		name := fmt.Sprintf("p%d", i)
		steps := ""
		for _, step := range []string{"a", "b"} {
			c := 1 + rng.IntN(4)
			if i < 3 {
				c = 1000 // the pipelines without a limit are never capped by a step either
			}
			concurrency[name+"/"+step] = c
			steps += fmt.Sprintf("  %s: { action: ./actions/a, in: trigger.adhoc, concurrency: %d }\n", step, c)
		}
		if i < 3 {
			unlimited = append(unlimited, name)
		} else {
			limits[name] = 1 + rng.IntN(3)
			steps += fmt.Sprintf("limits: { concurrency: %d }\n", limits[name])
		}
		v.apply(newPackage(t, manualPipeline(name, steps), "a"))
		for k := range backlog {
			v.fire(name, "adhoc", fmt.Sprintf("r%03d", k))
		}
	}

	var (
		mu       sync.Mutex
		held     = map[string][]store.Lease{} // node: leases not yet reported
		inFlight = map[string]int{}           // pipeline and pipeline/step: leased, not yet reported
		served   = map[string]int{}
		stop     bool
	)
	check := func(l store.Lease) error {
		inFlight[l.Pipeline]++
		inFlight[l.Pipeline+"/"+l.Step]++
		served[l.Pipeline]++
		if lim, ok := limits[l.Pipeline]; ok && inFlight[l.Pipeline] > lim {
			return fmt.Errorf("%s holds %d in flight, over its limit %d", l.Pipeline, inFlight[l.Pipeline], lim)
		}
		if c := concurrency[l.Pipeline+"/"+l.Step]; inFlight[l.Pipeline+"/"+l.Step] > c {
			return fmt.Errorf("%s/%s holds %d in flight, over its concurrency %d", l.Pipeline, l.Step,
				inFlight[l.Pipeline+"/"+l.Step], c)
		}
		// Each unlimited pipeline has two steps of backlog deliveries: stop well before any could run dry.
		stop = stop || served[l.Pipeline] >= backlog
		return nil
	}

	var wg sync.WaitGroup
	errs := make(chan error, nodes)
	for n := range nodes {
		nodeID := fmt.Sprintf("node-%d", n)
		r := rand.New(rand.NewPCG(seed, uint64(n)+1))
		wg.Go(func() {
			for {
				mu.Lock()
				done := stop
				var report []store.Lease
				if hs := held[nodeID]; len(hs) > 0 && r.IntN(2) == 0 {
					k := r.IntN(len(hs))
					l := hs[k]
					report = append(report, l)
					held[nodeID] = append(hs[:k], hs[k+1:]...)
					// Forgotten before the engine hears of it, so a pull leasing the freed slot never counts twice.
					inFlight[l.Pipeline]--
					inFlight[l.Pipeline+"/"+l.Step]--
				}
				mu.Unlock()
				if done {
					return
				}
				for _, l := range report {
					if err := v.e.Done(v.ctx, store.DoneRequest{LeaseRef: l.Ref(), Attempt: l.Attempt}); err != nil {
						errs <- err
						return
					}
				}
				as, err := v.e.Pull(v.ctx, nodeID, 1+r.IntN(3))
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				for _, a := range as {
					held[nodeID] = append(held[nodeID], a.Lease)
					if err := check(a.Lease); err != nil {
						stop = true
						mu.Unlock()
						errs <- err
						return
					}
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	// Every lease is recorded once the nodes stopped: they are a prefix of the engine's order, in which the pipelines
	// that always have a backlog take strict turns. (While the nodes run, a node may record its lease before another
	// that the engine leased first.)
	lo, hi := served[unlimited[0]], served[unlimited[0]]
	for _, name := range unlimited {
		lo, hi = min(lo, served[name]), max(hi, served[name])
	}
	if hi-lo > 1 {
		t.Errorf("the pipelines without a limit were served unevenly: %v", served)
	}
	for name := range limits {
		if served[name] == 0 {
			t.Errorf("%s was never served: %v", name, served)
		}
	}
}
