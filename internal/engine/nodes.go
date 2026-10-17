package engine

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/monolithiclab/millstream/internal/node"
	"github.com/monolithiclab/millstream/internal/pipeline"
	"github.com/monolithiclab/millstream/internal/store"
)

// Nodes (spec section 5.10). The engine keeps a registry of the nodes it serves in its term: its embedded node, in
// memory only, and the remote nodes that registered with it, each recorded in the store too (store.RegisterNode), so
// that a revocation outlives the engine and every engine lists them. A remote node registers again with the engine
// that takes a deployment over. Placement: a registered node leases only the deliveries of the steps whose
// placement it matches (node.Placement: the runtime the step's action needs and the step's labels); a delivery no
// registered node matches is unschedulable (Unschedulable). A node registered with AnyPlacement (run's embedded node,
// a laptop's only node) takes every delivery, and so does a puller that never registered (tests driving Pull), which
// is recorded so the first time it pulls.

// Errors of the node registry.
var (
	// ErrUnregistered means the remote node is not registered with this engine (in this term): it registers.
	ErrUnregistered = errors.New("node not registered with this engine")
	// ErrNotHeld means the node does not hold the lease its request names: it may fetch nothing for it (spec
	// section 15.7).
	ErrNotHeld = errors.New("the node holds no such lease")
)

// NodeInfo is what a node registers with.
type NodeInfo struct {
	ID       string
	Version  string
	Labels   map[string]string
	Runtimes []string
	Slots    int
	// Embedded marks the engine's own node, kept in memory only.
	Embedded bool
	// AnyPlacement makes the node take every delivery whatever its placement and runtime.
	AnyPlacement bool
}

// Node health, from the age of its last heartbeat.
const (
	HealthHealthy = "healthy" // heard from within three heartbeat intervals
	HealthLate    = "late"    // heard from within the lease duration: its leases still hold
	HealthLost    = "lost"    // silent for longer than a lease: its leases expire
)

// nodeEntry is a registered node. The caller holds e.mu.
type nodeEntry struct {
	info  NodeInfo
	state store.NodeState
	heard time.Time
}

// AddNode registers the engine's embedded node, in memory only.
func (e *Engine) AddNode(info NodeInfo) {
	info.Embedded = true
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nodes[info.ID] = &nodeEntry{info: info, state: store.NodeActive, heard: e.opts.Now()}
}

// RegisterNode registers a remote node with the engine and records it in the store: active, unless its id was
// revoked (store.ErrRevoked).
func (e *Engine) RegisterNode(ctx context.Context, info NodeInfo) error {
	if err := e.st.RegisterNode(ctx, store.Node{ID: info.ID, Version: info.Version, Labels: info.Labels,
		Runtimes: info.Runtimes, Slots: info.Slots}); err != nil {
		return fmt.Errorf("register node %s: %w", info.ID, err)
	}
	e.mu.Lock()
	e.nodes[info.ID] = &nodeEntry{info: info, state: store.NodeActive, heard: e.opts.Now()}
	e.mu.Unlock()
	e.log.Info("node registered", "nodeId", info.ID, "version", info.Version, "runtimes", info.Runtimes,
		"labels", info.Labels, "slots", info.Slots)
	return nil
}

// SetNodeState drains or revokes a node (spec sections 5.10 and 15.7), or with expireLeases ends the leases it holds
// at once: a revocation always does, and so does a node leaving. Draining makes the engine lease the node nothing
// more; it finishes what it holds.
func (e *Engine) SetNodeState(ctx context.Context, id string, state store.NodeState, expireLeases bool) error {
	expireLeases = expireLeases || state == store.NodeRevoked
	ended, err := e.st.SetNodeState(ctx, id, state, expireLeases)
	if err != nil {
		return fmt.Errorf("node %s: %w", id, err)
	}
	for _, ref := range ended {
		f, _ := e.release(ref)
		e.flightLog(ref, f).Warn("lease ended with its node", "nodeId", id, "nodeState", string(state))
	}
	e.mu.Lock()
	if n := e.nodes[id]; n != nil && n.state != store.NodeRevoked {
		n.state = state
	}
	e.mu.Unlock()
	e.log.Info("node state set", "nodeId", id, "state", string(state), "leasesEnded", len(ended))
	return nil
}

// CheckNode reports whether a remote node may talk to the engine: ErrUnregistered when it is not registered in this
// term, store.ErrRevoked once revoked.
func (e *Engine) CheckNode(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := e.nodes[id]
	switch {
	case n == nil || n.info.Embedded:
		return ErrUnregistered
	case n.state == store.NodeRevoked:
		return store.ErrRevoked
	}
	return nil
}

// NodeHeartbeat is a remote node's heartbeat: it renews the leases it names and returns those lost (Heartbeat). The
// store refuses a revoked node's renewal, so that a revocation another process recorded (millstream nodes revoke
// --direct) is refused from the next heartbeat on.
func (e *Engine) NodeHeartbeat(ctx context.Context, id string, refs []store.LeaseRef) ([]store.LeaseRef, error) {
	if err := e.CheckNode(id); err != nil {
		return nil, err
	}
	lost, err := e.Heartbeat(ctx, id, refs)
	e.mu.Lock()
	defer e.mu.Unlock()
	if n := e.nodes[id]; n != nil {
		switch {
		case errors.Is(err, store.ErrRevoked):
			n.state = store.NodeRevoked
		case err == nil:
			n.heard = e.opts.Now()
		}
	}
	return lost, err
}

// Health is a remote node's health at now after its last heartbeat heard, under leases of leaseTTL: healthy within
// three heartbeat intervals (a node heartbeats every node.DefaultHeartbeatInterval, or a third of the lease when
// shorter), late within a lease, lost beyond, its leases expired; lost too when never heard from (an id revoked
// before it registered).
func Health(heard, now time.Time, leaseTTL time.Duration) string {
	age := now.Sub(heard)
	switch beat := min(node.DefaultHeartbeatInterval, leaseTTL/3); {
	case heard.IsZero():
		return HealthLost
	case age <= 3*beat:
		return HealthHealthy
	case age <= leaseTTL:
		return HealthLate
	}
	return HealthLost
}

// remote reports whether owner is a remote node, which builds its deliveries' process specs itself.
func (e *Engine) remote(owner string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := e.nodes[owner]
	return n != nil && !n.info.Embedded
}

// leasable reports whether owner may lease now, and the placement filter of its leases: nil for a node that takes
// every delivery. The caller holds e.mu.
func (e *Engine) leasable(owner string) (ok bool, n *nodeEntry) {
	n = e.nodes[owner]
	switch {
	case n == nil: // a puller that never registered (tests drive Pull so) takes every delivery, as run's node does
		e.nodes[owner] = &nodeEntry{info: NodeInfo{ID: owner, Embedded: true, AnyPlacement: true},
			state: store.NodeActive}
		return true, nil
	case n.state != store.NodeActive:
		return false, nil
	case n.info.AnyPlacement:
		return true, nil
	}
	return true, n
}

// stepPlacement is where a step's deliveries may go: the runtime its action needs (none for a builtin, and none for
// an action that does not resolve, which fails at dispatch), and the step's labels, else its pipeline's.
func stepPlacement(p *pipeline.Pipeline, step pipeline.Step, ra *resolvedAction) node.Placement {
	pl := node.Placement{Labels: p.Placement.Labels}
	if step.Placement != nil {
		pl.Labels = step.Placement.Labels
	}
	if ra.err == nil {
		pl.Runtime = node.RuntimeOf(ra.a)
	}
	return pl
}

// UnschedulableStep is a step whose pending deliveries no node can take (spec section 5.10): none of the nodes
// registered and active, and heard from within a lease, offers its runtime and carries its labels.
type UnschedulableStep struct {
	Pipeline  string
	Step      string
	Pending   int64
	Placement node.Placement
}

// Unschedulable returns the steps with pending deliveries no registered node can take, by pipeline and step.
func (e *Engine) Unschedulable(ctx context.Context) ([]UnschedulableStep, error) {
	src, err := e.limitSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("unschedulable: %w", err)
	}
	var out []UnschedulableStep
	for _, rev := range src.active {
		stats, err := e.st.StepStats(ctx, rev.p.Name, e.opts.Now())
		if err != nil {
			return nil, fmt.Errorf("unschedulable: %w", err)
		}
		for _, s := range stats {
			ref := store.StepRef{Pipeline: rev.p.Name, Step: s.Step}
			pl, ok := src.placement[ref]
			if !ok || s.Pending == 0 || e.schedulable(pl) {
				continue
			}
			out = append(out, UnschedulableStep{Pipeline: ref.Pipeline, Step: ref.Step, Pending: s.Pending,
				Placement: pl})
		}
	}
	slices.SortFunc(out, func(a, b UnschedulableStep) int {
		return cmp.Or(cmp.Compare(a.Pipeline, b.Pipeline), cmp.Compare(a.Step, b.Step))
	})
	return out, nil
}

// schedulable reports whether a registered node, active and not lost, can take deliveries placed by pl.
func (e *Engine) schedulable(pl node.Placement) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.opts.Now()
	for _, n := range e.nodes {
		if n.state != store.NodeActive || (!n.info.Embedded && Health(n.heard, now, e.opts.LeaseTTL) == HealthLost) {
			continue
		}
		if n.info.AnyPlacement || pl.Matches(n.info.Runtimes, n.info.Labels) {
			return true
		}
	}
	return false
}

// heldBy returns the delivery in flight under ref's lease when owner holds it.
func (e *Engine) heldBy(owner string, ref store.LeaseRef) (flight, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	f, ok := e.inflight[ref.Key()]
	if !ok || f.token != ref.FencingToken || f.owner != owner {
		return flight{}, fmt.Errorf("delivery %s of %s: %w", ref.DeliveryID, ref.Pipeline, ErrNotHeld)
	}
	return f, nil
}

// HeldElsewhere reports whether the lease ref names is held, under its fencing token, by another node than owner:
// a report from owner must be refused as stale. A lease the engine does not know is left to the store's fencing.
func (e *Engine) HeldElsewhere(owner string, ref store.LeaseRef) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	f, ok := e.inflight[ref.Key()]
	return ok && f.token == ref.FencingToken && f.owner != owner
}

// HeldPackage returns the bundle of the package of the run of a delivery owner holds, when it is the one under hash
// (spec section 15.7: a node fetches a package only for a delivery it holds).
func (e *Engine) HeldPackage(ctx context.Context, owner string, ref store.LeaseRef, hash string) ([]byte, error) {
	f, err := e.heldBy(owner, ref)
	if err != nil {
		return nil, err
	}
	run, err := e.st.Run(ctx, ref.Pipeline, f.runID)
	if err != nil {
		return nil, err
	}
	rev, err := e.st.Revision(ctx, run.Revision)
	if err != nil {
		return nil, err
	}
	if rev.PackageHash != hash {
		return nil, fmt.Errorf("package %s is not the package of delivery %s: %w", hash, ref.DeliveryID, ErrNotHeld)
	}
	return e.st.Package(ctx, hash)
}

// HeldInput returns the input artifact sha of a delivery owner holds, its file made present in the delivery's
// pipeline (an artifact of a copy on an imported stream is copied from the producer's on first use, spec section
// 9.2).
func (e *Engine) HeldInput(ctx context.Context, owner string, ref store.LeaseRef, sha string) (store.ArtifactRef,
	error,
) {
	f, err := e.heldBy(owner, ref)
	if err != nil {
		return store.ArtifactRef{}, err
	}
	items, err := e.st.Items(ctx, store.ItemFilter{Pipeline: ref.Pipeline, IDs: []string{f.itemID}})
	if err != nil {
		return store.ArtifactRef{}, err
	}
	for _, it := range items {
		for _, a := range it.Artifacts {
			if a.SHA256 == sha {
				if e.opts.Artifacts == nil {
					return store.ArtifactRef{}, errNoArtifactStore
				}
				return a, EnsureArtifact(ctx, e.opts.Artifacts, ref.Pipeline, it.ParentPipeline, a)
			}
		}
	}
	return store.ArtifactRef{}, fmt.Errorf("artifact %s is not an input of delivery %s: %w", sha, ref.DeliveryID,
		ErrNotHeld)
}

// HoldsLease reports whether owner holds the lease ref names: an output artifact it puts goes to that delivery's
// pipeline.
func (e *Engine) HoldsLease(owner string, ref store.LeaseRef) error {
	_, err := e.heldBy(owner, ref)
	return err
}
