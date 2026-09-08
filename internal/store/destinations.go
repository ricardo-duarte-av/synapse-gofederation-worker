package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/dbtrace"
)

// RetryTimings is a destination's backoff state.
type RetryTimings struct {
	// FailureTS, RetryLastTS and RetryInterval are milliseconds, as stored.
	FailureTS     int64
	RetryLastTS   int64
	RetryInterval int64
}

// DueAt reports whether the destination may be tried at nowMS.
//
// Synapse's get_retry_limiter treats a destination with no retry_last_ts as
// never having failed, and only refuses while retry_last_ts + retry_interval is
// still in the future (util/retryutils.py:61).
func (r RetryTimings) DueAt(nowMS int64) bool {
	if r.RetryLastTS == 0 {
		return true
	}
	return r.RetryLastTS+r.RetryInterval <= nowMS
}

// GetDestinationRetryTimings reads the backoff state for several destinations.
//
// Batched rather than one query per destination because a single event in a
// large room fans out to a thousand of them, and a thousand round trips per
// event is the cost this worker exists to avoid. Destinations with no row are
// simply absent from the result and are due immediately.
func (s *Store) GetDestinationRetryTimings(ctx context.Context, destinations []string) (map[string]RetryTimings, error) {
	ctx = dbtrace.WithQueryName(ctx, "destination_retry_timings")
	if len(destinations) == 0 {
		return nil, nil
	}
	const q = `
		SELECT destination, COALESCE(failure_ts, 0), COALESCE(retry_last_ts, 0),
		       COALESCE(retry_interval, 0)
		FROM destinations
		WHERE destination = ANY($1)`

	rows, err := s.pool.Query(ctx, q, destinations)
	if err != nil {
		return nil, fmt.Errorf("store: retry timings: %w", err)
	}
	defer rows.Close()

	out := make(map[string]RetryTimings, len(destinations))
	for rows.Next() {
		var d string
		var t RetryTimings
		if err := rows.Scan(&d, &t.FailureTS, &t.RetryLastTS, &t.RetryInterval); err != nil {
			return nil, fmt.Errorf("store: retry timings: %w", err)
		}
		out[d] = t
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: retry timings: %w", err)
	}
	return out, nil
}

// FilterDestinationsByRetryLimiter is Synapse's filter_destinations_by_retry_limiter
// (util/retryutils.py:133): keep destinations that are due, or that will become
// due within dueWithin.
//
// The dueWithin slack is not a rounding convenience. The sender passes
// CATCHUP_RETRY_INTERVAL (one hour) so that a destination about to come back is
// included in the batch rather than deferred to the next poll, which is what
// keeps a recovering server from waiting an extra full interval.
func FilterDestinationsByRetryLimiter(
	destinations []string, timings map[string]RetryTimings, now time.Time, dueWithin time.Duration,
) []string {
	cutoff := now.UnixMilli() + dueWithin.Milliseconds()
	out := make([]string, 0, len(destinations))
	for _, d := range destinations {
		t, ok := timings[d]
		if !ok || t.DueAt(cutoff) {
			out = append(out, d)
		}
	}
	return out
}

// GetDestinationLastSuccessfulStreamOrdering reads a destination's catch-up
// cursor, or 0 with ok=false when there is no row.
//
// The distinction matters: a destination with no value has never had a
// successful transaction recorded, and Synapse leaves catch-up immediately
// rather than replaying history for it (per_destination_queue.py:485).
func (s *Store) GetDestinationLastSuccessfulStreamOrdering(ctx context.Context, destination string) (int64, bool, error) {
	const q = `SELECT last_successful_stream_ordering FROM destinations WHERE destination = $1`
	var v *int64
	err := s.pool.QueryRow(ctx, q, destination).Scan(&v)
	if err == pgx.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("store: last successful stream ordering: %w", err)
	}
	if v == nil {
		return 0, false, nil
	}
	return *v, true, nil
}

// GetCatchUpRoomEventIDs is Synapse's get_catch_up_room_event_ids
// (storage/databases/main/transactions.py:408): up to `limit` event ids owed to
// a destination, one per room, oldest first.
func (s *Store) GetCatchUpRoomEventIDs(ctx context.Context, destination string, lastSuccessful int64, limit int) ([]string, error) {
	ctx = dbtrace.WithQueryName(ctx, "catchup_room_event_ids")
	const q = `
		SELECT event_id FROM destination_rooms
		JOIN events USING (stream_ordering)
		WHERE destination = $1 AND stream_ordering > $2
		ORDER BY stream_ordering
		LIMIT $3`

	rows, err := s.pool.Query(ctx, q, destination, lastSuccessful, limit)
	if err != nil {
		return nil, fmt.Errorf("store: catch-up event ids: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: catch-up event ids: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// GetCatchUpOutstandingDestinations is Synapse's
// get_catch_up_outstanding_destinations (transactions.py:490): destinations
// that are owed something and are due to be tried.
//
// Paginated by destination name rather than offset, because the set changes
// while it is being walked -- a destination that catches up mid-scan would
// shift every later page and silently skip one.
//
// Both halves matter. A destination can be owed PDUs (destination_rooms ahead
// of its last success) or to-device messages (rows sitting in the outbox) or
// both, and a sender that only looked at one would leave the other stranded
// until unrelated traffic happened to wake it.
func (s *Store) GetCatchUpOutstandingDestinations(ctx context.Context, after string, nowMS int64, limit int) ([]string, error) {
	ctx = dbtrace.WithQueryName(ctx, "catchup_outstanding_destinations")
	const q = `
		WITH pdu_destinations AS (
			SELECT DISTINCT destination FROM destination_rooms
			LEFT JOIN destinations USING (destination)
			WHERE destination > $1
			  AND destination_rooms.stream_ordering >
			      COALESCE(destinations.last_successful_stream_ordering, 0)
			  AND (destinations.retry_last_ts IS NULL
			       OR destinations.retry_last_ts + destinations.retry_interval < $2)
			ORDER BY destination LIMIT $3
		), to_device_destinations AS (
			SELECT DISTINCT destination FROM device_federation_outbox
			LEFT JOIN destinations USING (destination)
			WHERE destination > $1
			  AND (destinations.retry_last_ts IS NULL
			       OR destinations.retry_last_ts + destinations.retry_interval < $2)
			ORDER BY destination LIMIT $3
		)
		SELECT destination FROM pdu_destinations
		UNION SELECT destination FROM to_device_destinations
		ORDER BY destination LIMIT $3`

	rows, err := s.pool.Query(ctx, q, after, nowMS, limit)
	if err != nil {
		return nil, fmt.Errorf("store: catch-up destinations: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, fmt.Errorf("store: catch-up destinations: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
