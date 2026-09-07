package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// GetStateGroupsForEvents maps event ids to their state groups.
//
// The state group recorded against an event is the state AFTER it, which is why
// the sender asks about an event's PREV events: the state after those is the
// state before this one. That is the whole reason Synapse resolves destinations
// from prev_events rather than from the event itself
// (federation/sender/__init__.py:661) -- it is what makes the last member on a
// server still receive their own ban.
//
// Events with no row are simply absent: an outlier has no state group, and
// there is nothing wrong with that.
func (s *Store) GetStateGroupsForEvents(ctx context.Context, eventIDs []string) (map[string]int64, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	const q = `SELECT event_id, state_group FROM event_to_state_groups WHERE event_id = ANY($1)`

	rows, err := s.pool.Query(ctx, q, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: state groups for events: %w", err)
	}
	defer rows.Close()

	out := make(map[string]int64, len(eventIDs))
	for rows.Next() {
		var id string
		var group int64
		if err := rows.Scan(&id, &group); err != nil {
			return nil, fmt.Errorf("store: state groups for events: %w", err)
		}
		out[id] = group
	}
	return out, rows.Err()
}

// joinedHostsAtGroupQuery walks a state group's delta chain and returns the
// servers with a joined member.
//
// State groups form a chain: a group holds either full state or a delta against
// a previous group, linked by state_group_edges. Resolving means collecting the
// whole chain and letting the highest-numbered group win for each state key,
// which DISTINCT ON with a descending sort does in one pass. Ported from
// Synapse's _get_state_groups_from_groups_txn.
//
// Filtered to m.room.member inside the walk rather than after it. The unfiltered
// form resolves the room's entire state -- power levels, topic, every alias --
// when all we want is who is joined.
//
// The membership is not in state_groups_state, so room_memberships is joined for
// it. The regex is the same one Synapse uses to pull a domain out of a state key
// (roommember.py:1241).
const joinedHostsAtGroupQuery = `
	WITH RECURSIVE sgs(state_group) AS (
		VALUES($1::bigint)
	  UNION ALL
		SELECT prev_state_group FROM state_group_edges e, sgs s
		WHERE s.state_group = e.state_group
	), members AS (
		SELECT DISTINCT ON (state_key) state_key, event_id
		FROM state_groups_state
		INNER JOIN sgs USING (state_group)
		WHERE type = 'm.room.member'
		ORDER BY state_key, state_group DESC
	)
	SELECT DISTINCT substring(m.state_key FROM '@[^:]*:(.*)$')
	FROM members m
	JOIN room_memberships rm ON rm.event_id = m.event_id
	WHERE rm.membership = 'join'`

// JoinedHostsAtStateGroup returns the servers with a joined member at a state
// group.
//
// state_groups_state is the largest table in a Synapse database by a wide
// margin, and the planner will choose a sequential scan over it without help.
// Synapse disables seqscan for this transaction and so must we: the query is
// otherwise pathological on a large room. SET LOCAL scopes the change to the
// transaction, which also keeps it safe behind a transaction-mode pooler.
func (s *Store) JoinedHostsAtStateGroup(ctx context.Context, group int64) ([]string, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return nil, fmt.Errorf("store: disable seqscan: %w", err)
	}

	rows, err := tx.Query(ctx, joinedHostsAtGroupQuery, group)
	if err != nil {
		return nil, fmt.Errorf("store: joined hosts at state group %d: %w", group, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var host *string
		if err := rows.Scan(&host); err != nil {
			return nil, fmt.Errorf("store: joined hosts at state group %d: %w", group, err)
		}
		// NULL for a malformed state key. Synapse's set comprehension would
		// keep it as None; dropping it is the only sensible reading, since
		// None is not a server we can send to.
		if host != nil && *host != "" {
			out = append(out, *host)
		}
	}
	return out, rows.Err()
}
