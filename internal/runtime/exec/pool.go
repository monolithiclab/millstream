package exec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/monolithiclab/millstream/internal/pipeline"
)

// DefaultMaxIdle is how many idle processes a Pool keeps by default.
const DefaultMaxIdle = 8

// PoolOptions configure a Pool.
type PoolOptions struct {
	Options
	// MaxIdle caps the idle processes kept for reuse; the least recently used is closed first. Runs with other
	// vars or revisions key other processes, so without a cap a long-running node would accumulate them.
	MaxIdle int
}

// Pool runs deliveries on action processes. A long-lived process is reused only for the same action digest,
// config and vars (spec section 6.5), and serves one delivery at a time: concurrent deliveries of one key run on
// as many processes. A process that fails a delivery in the runtime is dropped, so the next delivery spawns a
// fresh one. A perItem action gets a fresh process for every delivery, closed after it.
type Pool struct {
	opts PoolOptions

	mu     sync.Mutex
	idle   []idleProcess // least recently used first
	closed bool
}

type idleProcess struct {
	key string
	p   *Process
}

// ErrPoolClosed is returned by Deliver after Close.
var ErrPoolClosed = errors.New("process pool closed")

// NewPool returns an empty pool.
func NewPool(opts PoolOptions) *Pool {
	opts.Options = opts.withDefaults()
	if opts.MaxIdle <= 0 {
		opts.MaxIdle = DefaultMaxIdle
	}
	return &Pool{opts: opts}
}

// Deliver runs d on a process for spec: an idle one with the same key, else a new one. Its errors are those of
// Start and Process.Deliver.
func (pl *Pool) Deliver(ctx context.Context, spec Spec, d Delivery) (Result, error) {
	if spec.Lifecycle == pipeline.LifecyclePerItem {
		p, err := Start(ctx, spec, pl.opts.Options)
		if err != nil {
			return Result{}, err
		}
		res, err := p.Deliver(ctx, d)
		pl.closeLogged(p)
		return res, err
	}
	key, err := poolKey(spec)
	if err != nil {
		return Result{}, err
	}
	p, err := pl.acquire(ctx, key, spec)
	if err != nil {
		return Result{}, err
	}
	res, err := p.Deliver(ctx, d)
	if healthy := err == nil || Permanent(err) && p.Alive(); !healthy {
		pl.closeLogged(p)
		return res, err
	}
	pl.release(key, p) // a permanent error left the process healthy: it serves the next delivery
	return res, err
}

func (pl *Pool) acquire(ctx context.Context, key string, spec Spec) (*Process, error) {
	var stale []*Process
	defer func() {
		for _, p := range stale {
			pl.closeLogged(p)
		}
	}()
	pl.mu.Lock()
	if pl.closed {
		pl.mu.Unlock()
		return nil, ErrPoolClosed
	}
	for i := len(pl.idle) - 1; i >= 0; i-- {
		ip := pl.idle[i]
		if ip.key != key {
			continue
		}
		pl.idle = append(pl.idle[:i], pl.idle[i+1:]...)
		if ip.p.Alive() {
			pl.mu.Unlock()
			return ip.p, nil
		}
		stale = append(stale, ip.p) // exited while idle: replaced below
	}
	pl.mu.Unlock()
	return Start(ctx, spec, pl.opts.Options)
}

func (pl *Pool) release(key string, p *Process) {
	var evicted []*Process
	pl.mu.Lock()
	if pl.closed {
		evicted = append(evicted, p)
	} else {
		pl.idle = append(pl.idle, idleProcess{key: key, p: p})
		for len(pl.idle) > pl.opts.MaxIdle {
			evicted = append(evicted, pl.idle[0].p)
			pl.idle = pl.idle[1:]
		}
	}
	pl.mu.Unlock()
	for _, e := range evicted {
		pl.closeLogged(e)
	}
}

// Close closes the idle processes; processes serving a delivery are closed when it ends.
func (pl *Pool) Close() error {
	pl.mu.Lock()
	idle := pl.idle
	pl.idle, pl.closed = nil, true
	pl.mu.Unlock()
	var errs []error
	for _, ip := range idle {
		errs = append(errs, ip.p.Close())
	}
	return errors.Join(errs...)
}

func (pl *Pool) closeLogged(p *Process) {
	if err := p.Close(); err != nil {
		pl.opts.Logger.Warn("close action process", "error", err.Error())
	}
}

// poolKey is the action digest with hashes of the config and vars: encoding/json sorts map keys, so equal maps
// encode alike.
func poolKey(spec Spec) (string, error) {
	enc, err := json.Marshal([]any{spec.Config, spec.Vars})
	if err != nil {
		return "", fmt.Errorf("action %s: encode config and vars: %w", spec.Action, err)
	}
	sum := sha256.Sum256(enc)
	return spec.Digest + "/" + hex.EncodeToString(sum[:]), nil
}
