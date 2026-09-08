package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/dbtrace"
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
// otherwise pathological on a large room.
//
// Sent as ONE batch rather than as an explicit transaction. pgx ends a batch
// with a single Sync, so the statements run in one implicit transaction --
// which is what SET LOCAL needs to be scoped, and what keeps it from leaking
// onto a pooled connection where it would silently change the plan of every
// other query. Verified against a real PostgreSQL rather than assumed: inside
// the batch enable_seqscan reads "off", and on the same connection immediately
// afterwards it reads "on".
//
// The point is round trips. BEGIN, SET, query, ROLLBACK is four of them for one
// answer, on the per-event path, and through a transaction pooler they are four
// network hops rather than four socket writes.
func (s *Store) JoinedHostsAtStateGroup(ctx context.Context, group int64) ([]string, error) {
	ctx = dbtrace.WithQueryName(ctx, "joined_hosts_at_state_group")
	b := &pgx.Batch{}
	b.Queue(`SET LOCAL enable_seqscan = off`)
	b.Queue(joinedHostsAtGroupQuery, group)

	br := s.pool.SendBatch(ctx, b)
	defer func() { _ = br.Close() }()

	if _, err := br.Exec(); err != nil {
		return nil, fmt.Errorf("store: disable seqscan: %w", err)
	}
	rows, err := br.Query()
	if err != nil {
		return nil, fmt.Errorf("store: joined hosts at state group %d: %w", group, err)
	}

	var out []string
	for rows.Next() {
		var host *string
		if err := rows.Scan(&host); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: joined hosts at state group %d: %w", group, err)
		}
		// NULL for a malformed state key. Synapse's set comprehension would
		// keep it as None; dropping it is the only sensible reading, since
		// there is no server to send to.
		if host != nil && *host != "" {
			out = append(out, *host)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: joined hosts at state group %d: %w", group, err)
	}
	return out, nil
}
