package store

import (
	"context"
	"fmt"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/dbtrace"

	"github.com/jackc/pgx/v5"
)

// NewEvent is one row of the federation sender's pickup query.
type NewEvent struct {
	StreamOrdering int64
	EventID        string
	// ReceivedTS is nullable in the schema; zero means unknown, which Synapse
	// treats as "cannot measure lag" rather than "arrived at the epoch".
	ReceivedTS int64
}

// GetAllNewEventIDsStream is Synapse's get_all_new_event_ids_stream
// (storage/databases/main/stream.py:2107), the query the sender's pickup loop
// runs.
//
// It returns the rows and the token to resume from. The token is uptoTS unless
// the limit was hit, in which case it is the last row's stream_ordering --
// advancing to uptoTS after a truncated batch would skip every event the limit
// cut off, silently and permanently.
func (s *Store) GetAllNewEventIDsStream(ctx context.Context, from, upto int64, limit int) ([]NewEvent, int64, error) {
	ctx = dbtrace.WithQueryName(ctx, "new_event_ids")
	const q = `
		SELECT e.stream_ordering, e.event_id, COALESCE(e.received_ts, 0)
		FROM events AS e
		WHERE $1 < e.stream_ordering AND e.stream_ordering <= $2
		ORDER BY e.stream_ordering ASC
		LIMIT $3`

	rows, err := s.pool.Query(ctx, q, from, upto, limit)
	if err != nil {
		return nil, from, fmt.Errorf("store: new event ids: %w", err)
	}
	defer rows.Close()

	out := make([]NewEvent, 0, limit)
	for rows.Next() {
		var e NewEvent
		if err := rows.Scan(&e.StreamOrdering, &e.EventID, &e.ReceivedTS); err != nil {
			return nil, from, fmt.Errorf("store: new event ids: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, from, fmt.Errorf("store: new event ids: %w", err)
	}

	next := upto
	if len(out) == limit && limit > 0 {
		next = out[len(out)-1].StreamOrdering
	}
	return out, next, nil
}

// MaxStreamOrdering is the newest event in the database.
//
// Used to seed the pickup loop's upper bound at startup. A real sender learns
// this from the replication stream's POSITION, which we never ask for
// (docs/shadow-safety.md), so we read it instead.
func (s *Store) MaxStreamOrdering(ctx context.Context) (int64, error) {
	ctx = dbtrace.WithQueryName(ctx, "max_stream_ordering")
	var max int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(MAX(stream_ordering), 0) FROM events`).Scan(&max)
	if err != nil {
		return 0, fmt.Errorf("store: max stream_ordering: %w", err)
	}
	return max, nil
}

// Event is what the sender needs to decide where a PDU goes and to put it on
// the wire.
type Event struct {
	EventID        string
	RoomID         string
	Type           string
	StateKey       string
	Sender         string
	StreamOrdering int64
	// Outlier and RejectionReason are read because Synapse's checks depend on
	// them: an out-of-band membership is always an outlier, and a rejected
	// event is never sent.
	Outlier         bool
	RejectionReason string
	// JSON is the event body as stored, which is what gets serialised into a
	// transaction.
	JSON []byte
	// ReceivedTS is when Synapse persisted the event, in milliseconds. It is
	// the baseline for the processing-lag metric, which is the number directly
	// comparable with Synapse's own. Zero when unknown, which the metric reads
	// as "cannot measure" rather than "arrived at the epoch".
	ReceivedTS int64
	// InternalMetadata carries the three flags the eligibility filter needs --
	// out_of_band_membership, proactively_send and send_on_behalf_of -- none of
	// which are in the event body.
	InternalMetadata []byte
}

// GetEvents loads events by id.
//
// Returned in the order the ids were given, with missing ids simply absent:
// Synapse's own loop iterates the id list and skips what the cache-or-db lookup
// did not return, so an event purged between the pickup query and this one is
// not an error.
func (s *Store) GetEvents(ctx context.Context, ids []string) ([]Event, error) {
	ctx = dbtrace.WithQueryName(ctx, "events_by_id")
	if len(ids) == 0 {
		return nil, nil
	}
	const q = `
		SELECT e.event_id, e.room_id, e.type, COALESCE(e.state_key, ''),
		       COALESCE(e.sender, ''), e.stream_ordering, e.outlier,
		       COALESCE(e.rejection_reason, ''), COALESCE(e.received_ts, 0),
		       ej.json, ej.internal_metadata
		FROM events AS e
		JOIN event_json AS ej USING (event_id)
		WHERE e.event_id = ANY($1)`

	rows, err := s.pool.Query(ctx, q, ids)
	if err != nil {
		return nil, fmt.Errorf("store: get events: %w", err)
	}
	defer rows.Close()

	byID := make(map[string]Event, len(ids))
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.EventID, &e.RoomID, &e.Type, &e.StateKey, &e.Sender,
			&e.StreamOrdering, &e.Outlier, &e.RejectionReason, &e.ReceivedTS,
			&e.JSON, &e.InternalMetadata); err != nil {
			return nil, fmt.Errorf("store: get events: %w", err)
		}
		byID[e.EventID] = e
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: get events: %w", err)
	}

	out := make([]Event, 0, len(byID))
	for _, id := range ids {
		if e, ok := byID[id]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

// CurrentJoinedHosts returns the servers with a joined member in a room.
//
// This is Synapse's get_current_hosts_in_room SQL verbatim
// (roommember.py:1240), including the regex that pulls the domain out of the
// state key.
//
// It is the CURRENT state, whereas the real sender resolves the state BEFORE
// the event (federation/sender/__init__.py:661). The difference is real and is
// measured rather than papered over -- see internal/destinations -- but it only
// shows up for events that change room membership, which is a small fraction of
// traffic and exactly the fraction the divergence counter exists to size.
func (s *Store) CurrentJoinedHosts(ctx context.Context, roomID string) ([]string, error) {
	ctx = dbtrace.WithQueryName(ctx, "current_joined_hosts")
	const q = `
		SELECT DISTINCT substring(state_key FROM '@[^:]*:(.*)$')
		FROM current_state_events
		WHERE type = 'm.room.member' AND membership = 'join' AND room_id = $1`

	rows, err := s.pool.Query(ctx, q, roomID)
	if err != nil {
		return nil, fmt.Errorf("store: current joined hosts: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var host *string
		if err := rows.Scan(&host); err != nil {
			return nil, fmt.Errorf("store: current joined hosts: %w", err)
		}
		// The regex yields NULL for a malformed state key. Synapse's set
		// comprehension would keep that as None; dropping it is the only
		// sensible reading, since None is not a server we can send to.
		if host != nil && *host != "" {
			out = append(out, *host)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: current joined hosts: %w", err)
	}
	return out, nil
}

// GetFederationOutPos reads a row of federation_stream_position.
//
// Read-only, and specifically the row of the instance we shadow: it is where
// the real sender has got to, which is the right place for the shadow to start
// so the two are comparable from the first event. We never write this table --
// Synapse rewrites it at startup using MIN(stream_id) across instances and
// deletes rows for unknown ones, so our row would either vanish or drag both
// real senders backwards (docs/shadow-safety.md).
func (s *Store) GetFederationOutPos(ctx context.Context, typ, instanceName string) (int64, error) {
	const q = `SELECT stream_id FROM federation_stream_position WHERE type = $1 AND instance_name = $2`
	var pos int64
	err := s.pool.QueryRow(ctx, q, typ, instanceName).Scan(&pos)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: federation out pos: %w", err)
	}
	return pos, nil
}
