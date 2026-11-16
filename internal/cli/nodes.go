package cli

import (
	"cmp"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/monolithiclab/millstream/internal/engine"
	"github.com/monolithiclab/millstream/internal/store"
)

// NodesCmd is "millstream nodes": the remote nodes, their draining and their revocation (spec sections 5.10 and
// 15.7).
type NodesCmd struct {
	List   NodesListCmd   `cmd:"" default:"withargs" help:"List the remote nodes: state, health, version, slots, leases held, labels and runtimes."`
	Drain  NodesDrainCmd  `cmd:"" help:"Drain the node running from the state directory: it stops pulling, finishes or releases its deliveries within its drain timeout, and exits."`
	Revoke NodesRevokeCmd `cmd:"" help:"Revoke a node id: refused from then on, its leases ended at once."`
}

// NodesListCmd is "millstream nodes [list]".
type NodesListCmd struct {
	LeaseTTL time.Duration `name:"lease-ttl" default:"60s" help:"The engines' lease duration, which health is measured against."`
}

// Run prints the remote nodes the store records, read-only, with the leases each holds.
func (c NodesListCmd) Run(rt *Runtime) error {
	return withStore(rt, func(st store.Store) error {
		nodes, err := st.Nodes(rt.Ctx)
		if err != nil {
			return err
		}
		leased, err := st.Deliveries(rt.Ctx, store.DeliveryFilter{State: store.DeliveryLeased})
		if err != nil {
			return err
		}
		held := map[string]int{}
		for _, d := range leased {
			held[d.LeaseOwner]++
		}
		w := tabwriter.NewWriter(rt.IO.Out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tSTATE\tHEALTH\tVERSION\tSLOTS\tLEASES\tLABELS\tRUNTIMES\tLAST HEARTBEAT")
		now := time.Now()
		for _, n := range nodes {
			heard := "-"
			if !n.LastHeartbeat.IsZero() {
				heard = n.LastHeartbeat.Format(time.RFC3339)
			}
			labels := make([]string, 0, len(n.Labels))
			for _, k := range slices.Sorted(maps.Keys(n.Labels)) {
				labels = append(labels, k+"="+n.Labels[k])
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\n", n.ID, n.State, engine.Health(n.LastHeartbeat, now, c.LeaseTTL), cmp.Or(n.Version, "-"), n.Slots,
				held[n.ID], cmp.Or(strings.Join(labels, ","), "-"), cmp.Or(strings.Join(n.Runtimes, ","), "-"), heard)
		}
		return w.Flush()
	})
}

// NodesRevokeCmd is "millstream nodes revoke <id>".
type NodesRevokeCmd struct {
	Node string `arg:"" help:"Node id."`
}

// Run revokes the node: the engine refuses it from then on, and every lease it holds ends at once, its attempt
// consumed, so fencing refuses what it sends afterwards.
func (c NodesRevokeCmd) Run(rt *Runtime) error {
	err := changeState(rt, func(cl *apiClient) error {
		_, err := cl.post(rt.Ctx, "/nodes/"+url.PathEscape(c.Node)+"/revoke", nil, nil)
		return err
	}, func(st store.Store) error {
		_, err := st.SetNodeState(rt.Ctx, c.Node, store.NodeRevoked, true)
		return err
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(rt.IO.Out, "%s revoked\n", c.Node)
	return err
}
