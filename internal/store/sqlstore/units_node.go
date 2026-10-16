package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/monolithiclab/millstream/internal/store"
)

// RegisterNode records a remote node joining (spec section 5.10): a new row, or the same id again from a new
// process, active again unless revoked.
func (s *Store) RegisterNode(ctx context.Context, n store.Node) error {
	if n.ID == "" || n.Slots < 0 {
		return fmt.Errorf("register node: an id and slots: %w", store.ErrInvalid)
	}
	labels, err := json.Marshal(nonNil(n.Labels))
	if err != nil {
		return fmt.Errorf("register node %s: %w", n.ID, err)
	}
	if n.Runtimes == nil {
		n.Runtimes = []string{}
	}
	runtimes, err := json.Marshal(n.Runtimes)
	if err != nil {
		return fmt.Errorf("register node %s: %w", n.ID, err)
	}
	return s.submit(ctx, unitRegisterNode, func(w *wtx) error {
		var state store.NodeState
		switch err := w.queryRow(`SELECT state FROM nodes WHERE id = ?`, n.ID).Scan(&state); {
		case errors.Is(err, sql.ErrNoRows):
			_, err := w.exec(`INSERT INTO nodes (id, version, labels, runtimes, slots, state, registered_at,
				last_heartbeat) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, n.ID, n.Version, string(labels), string(runtimes),
				n.Slots, store.NodeActive, w.ts(), w.ts())
			return err
		case err != nil:
			return fmt.Errorf("register node %s: %w", n.ID, err)
		case state == store.NodeRevoked:
			return fmt.Errorf("register node %s: %w", n.ID, store.ErrRevoked)
		}
		_, err := w.exec(`UPDATE nodes SET version = ?, labels = ?, runtimes = ?, slots = ?, state = ?,
			registered_at = ?, last_heartbeat = ? WHERE id = ?`, n.Version, string(labels), string(runtimes), n.Slots,
			store.NodeActive, w.ts(), w.ts(), n.ID)
		return err
	})
}

// SetNodeState drains or revokes a node, ending the leases it holds when asked to (spec sections 5.10 and 15.7).
// A revoked node stays revoked.
func (s *Store) SetNodeState(ctx context.Context, id string, state store.NodeState, expireLeases bool) (
	[]store.LeaseRef, error,
) {
	if id == "" || (state != store.NodeDraining && state != store.NodeRevoked) {
		return nil, fmt.Errorf("set node state: an id, and draining or revoked: %w", store.ErrInvalid)
	}
	var ended []store.LeaseRef
	err := s.submit(ctx, unitSetNodeState, func(w *wtx) error {
		ended = nil // the unit fills the result only when it applies
		var current store.NodeState
		switch err := w.queryRow(`SELECT state FROM nodes WHERE id = ?`, id).Scan(&current); {
		case errors.Is(err, sql.ErrNoRows) && state == store.NodeRevoked:
			if _, err := w.exec(`INSERT INTO nodes (id, state) VALUES (?, ?)`, id, state); err != nil {
				return err
			}
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("node %s: %w", id, store.ErrNotFound)
		case err != nil:
			return fmt.Errorf("node %s: %w", id, err)
		case current != store.NodeRevoked:
			if _, err := w.exec(`UPDATE nodes SET state = ? WHERE id = ?`, state, id); err != nil {
				return err
			}
		}
		if !expireLeases {
			return nil
		}
		reason := fmt.Sprintf("node %s left before the lease it held reported", id)
		if state == store.NodeRevoked {
			reason = fmt.Sprintf("node %s was revoked while it held the lease", id)
		}
		var err error
		ended, err = w.expireWhere(`lease_owner = ?`, []any{id}, reason)
		return err
	})
	if err != nil {
		return nil, err
	}
	return ended, nil
}

// Nodes returns the remote nodes by id.
func (s *Store) Nodes(ctx context.Context) ([]store.Node, error) {
	out, err := scan(ctx, s.read, `SELECT id, version, labels, runtimes, slots, state, registered_at, last_heartbeat
		FROM nodes ORDER BY id`, nil, func(rows *sql.Rows) (store.Node, error) {
		var n store.Node
		var labels, runtimes string
		var registered, heard TimeCol
		if err := rows.Scan(&n.ID, &n.Version, &labels, &runtimes, &n.Slots, &n.State, &registered,
			&heard); err != nil {
			return n, fmt.Errorf("scan node: %w", err)
		}
		if err := json.Unmarshal([]byte(labels), &n.Labels); err != nil {
			return n, fmt.Errorf("node %s labels: %w", n.ID, err)
		}
		if err := json.Unmarshal([]byte(runtimes), &n.Runtimes); err != nil {
			return n, fmt.Errorf("node %s runtimes: %w", n.ID, err)
		}
		n.RegisteredAt, n.LastHeartbeat = registered.Time, heard.Time
		return n, nil
	})
	if err != nil {
		return nil, fmt.Errorf("nodes: %w", err)
	}
	return out, nil
}
