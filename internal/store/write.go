// Writer is the other half of this package, and the dangerous half.
//
// Everything in store.go is a SELECT under a role that cannot write. This file
// is what a worker uses when it is a homeserver's REAL federation sender rather
// than a shadow of one, and at that point the read-only guarantee no longer
// applies -- it cannot, because a sender's bookkeeping is a set of deletions.
//
// It lives on its own pool under its own role and is constructed only in
// primary mode, so a shadow deployment has no writable handle to Synapse's
// tables anywhere in the process. See docs/shadow-safety.md for why that
// separation is worth the duplication.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/dbtrace"
)

// Writer performs a federation sender's writes against Synapse's database.
type Writer struct {
	pool *pgxpool.Pool
}

// OpenWriter connects a writable pool.
func OpenWriter(ctx context.Context, cfg Config) (*Writer, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: parse write dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = cfg.MaxConns
	}
	// Same transaction-pooler reasoning as internal/store.Open: one round trip,
	// so a pooler has no seam to switch the backend connection on.
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

	// Every query on this pool is measured, including ones added later by
	// somebody who never reads this file. See internal/dbtrace.
	if t := dbtrace.New(cfg.OnQuery); t != nil {
		pcfg.ConnConfig.Tracer = t
	}
	if cfg.ConnectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("store: connect writer: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping writer: %w", err)
	}
	return &Writer{pool: pool}, nil
}

// Close releases the pool.
func (w *Writer) Close() {
	if w != nil && w.pool != nil {
		w.pool.Close()
	}
}

// CanWrite verifies the role is actually able to write.
//
// Checked at startup, because the failure it catches is a primary sender
// configured with the read-only role: it would start, consume the stream, send
// transactions, and then fail every deletion -- delivering correctly while its
// bookkeeping silently went nowhere and the outbox grew without bound.
func (w *Writer) CanWrite(ctx context.Context) error {
	ctx = dbtrace.WithQueryName(ctx, "check_can_write")
	var readOnly string
	if err := w.pool.QueryRow(ctx, `SHOW default_transaction_read_only`).Scan(&readOnly); err != nil {
		return fmt.Errorf("store: checking write access: %w", err)
	}
	if readOnly == "on" {
		return fmt.Errorf("store: the write role has default_transaction_read_only set; " +
			"a primary sender cannot do its bookkeeping with it")
	}
	return nil
}

// DeleteDeviceMsgsForRemote is Synapse's delete_device_msgs_for_remote
// (deviceinbox.py:669), run after a transaction carrying to-device messages was
// accepted.
//
// This deletion IS the cursor for to-device messages. A sender that reads them
// and never deletes will re-read the same rows forever, and the outbox grows
// without bound.
func (w *Writer) DeleteDeviceMsgsForRemote(ctx context.Context, destination string, upToStreamID int64) error {
	ctx = dbtrace.WithQueryName(ctx, "delete_device_msgs")
	const q = `DELETE FROM device_federation_outbox WHERE destination = $1 AND stream_id <= $2`
	if _, err := w.pool.Exec(ctx, q, destination, upToStreamID); err != nil {
		return fmt.Errorf("store: delete to-device messages for %s: %w", destination, err)
	}
	return nil
}

// MarkAsSentDevicesByRemote is Synapse's mark_as_sent_devices_by_remote
// (devices.py:922).
//
// One statement deletes the pokes and returns them, and the highest stream id
// per user is then recorded in device_lists_outbound_last_success. Both halves
// matter and they must be one transaction: the last_success rows are where the
// NEXT batch's prev_id comes from, so losing them after the delete breaks the
// chain a receiver uses to notice a gap -- and a broken chain is not detectable
// from either end.
func (w *Writer) MarkAsSentDevicesByRemote(ctx context.Context, destination string, upToStreamID int64) error {
	ctx = dbtrace.WithQueryName(ctx, "mark_devices_sent")
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const del = `
		DELETE FROM device_lists_outbound_pokes
		WHERE destination = $1 AND stream_id <= $2
		RETURNING user_id, stream_id`

	rows, err := tx.Query(ctx, del, destination, upToStreamID)
	if err != nil {
		return fmt.Errorf("store: mark devices sent for %s: %w", destination, err)
	}
	// Aggregated here rather than with a second GROUP BY query, as Synapse
	// does and for the same reason: this runs often, and touching the same
	// rows twice costs more than the arithmetic.
	maxByUser := map[string]int64{}
	for rows.Next() {
		var user string
		var streamID int64
		if err := rows.Scan(&user, &streamID); err != nil {
			rows.Close()
			return fmt.Errorf("store: mark devices sent for %s: %w", destination, err)
		}
		if streamID > maxByUser[user] {
			maxByUser[user] = streamID
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: mark devices sent for %s: %w", destination, err)
	}

	const upsert = `
		INSERT INTO device_lists_outbound_last_success (destination, user_id, stream_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (destination, user_id) DO UPDATE
		SET stream_id = EXCLUDED.stream_id
		WHERE device_lists_outbound_last_success.stream_id < EXCLUDED.stream_id`

	for user, streamID := range maxByUser {
		if _, err := tx.Exec(ctx, upsert, destination, user, streamID); err != nil {
			return fmt.Errorf("store: recording last device success for %s: %w", destination, err)
		}
	}
	return tx.Commit(ctx)
}

// StoreDestinationRoomsEntries is Synapse's store_destination_rooms_entries
// (transactions.py:305): the record of which destinations are owed which room,
// written BEFORE the retry filter so a server that is down still has the event
// recorded as owed to it.
//
// The destinations rows are inserted first only to satisfy the foreign key;
// their retry columns are left alone.
func (w *Writer) StoreDestinationRoomsEntries(ctx context.Context, destinations []string, roomID string, streamOrdering int64) error {
	ctx = dbtrace.WithQueryName(ctx, "store_destination_rooms")
	if len(destinations) == 0 {
		return nil
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const ensure = `
		INSERT INTO destinations (destination) SELECT unnest($1::text[])
		ON CONFLICT (destination) DO NOTHING`
	if _, err := tx.Exec(ctx, ensure, destinations); err != nil {
		return fmt.Errorf("store: ensuring destinations: %w", err)
	}

	const upsert = `
		INSERT INTO destination_rooms (destination, room_id, stream_ordering)
		SELECT unnest($1::text[]), $2, $3
		ON CONFLICT (destination, room_id) DO UPDATE
		SET stream_ordering = EXCLUDED.stream_ordering
		WHERE destination_rooms.stream_ordering < EXCLUDED.stream_ordering`
	if _, err := tx.Exec(ctx, upsert, destinations, roomID, streamOrdering); err != nil {
		return fmt.Errorf("store: recording destination rooms: %w", err)
	}
	return tx.Commit(ctx)
}

// SetDestinationLastSuccessfulStreamOrdering advances a destination's catch-up
// cursor (transactions.py:359).
func (w *Writer) SetDestinationLastSuccessfulStreamOrdering(ctx context.Context, destination string, streamOrdering int64) error {
	ctx = dbtrace.WithQueryName(ctx, "set_last_successful")
	const q = `
		INSERT INTO destinations (destination, last_successful_stream_ordering)
		VALUES ($1, $2)
		ON CONFLICT (destination) DO UPDATE
		SET last_successful_stream_ordering = EXCLUDED.last_successful_stream_ordering
		WHERE destinations.last_successful_stream_ordering IS NULL
		   OR destinations.last_successful_stream_ordering < EXCLUDED.last_successful_stream_ordering`
	if _, err := w.pool.Exec(ctx, q, destination, streamOrdering); err != nil {
		return fmt.Errorf("store: last successful ordering for %s: %w", destination, err)
	}
	return nil
}

// UpdateFederationOutPos writes our position in a stream
// (stream.py:2148), keyed by instance name as Synapse does.
func (w *Writer) UpdateFederationOutPos(ctx context.Context, typ, instanceName string, streamID int64) error {
	ctx = dbtrace.WithQueryName(ctx, "update_federation_out_pos")
	const q = `
		INSERT INTO federation_stream_position (type, instance_name, stream_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (type, instance_name) DO UPDATE
		SET stream_id = EXCLUDED.stream_id
		WHERE federation_stream_position.stream_id < EXCLUDED.stream_id`
	if _, err := w.pool.Exec(ctx, q, typ, instanceName, streamID); err != nil {
		return fmt.Errorf("store: federation out pos: %w", err)
	}
	return nil
}

// SetDestinationRetryTimings and ClearDestinationRetryTimings write Synapse's
// own `destinations` retry columns, exactly as a Python sender does.
//
// These were removed once and had to come back, and the reason is worth
// keeping. Synapse caches these timings (transactions.py:169) and invalidates
// the cache only from its own write path, so writing them from out here leaves
// every Synapse process serving what it cached. That is a real cost -- but the
// cost of NOT writing them turned out to be larger and was measured: Synapse's
// inbound path resets a destination's backoff when that server contacts us, and
// it decides whether to bother by reading THIS column
// (transport/server/_base.py:143). With the column left at zero the reset never
// fires, so a server that came back and talked to us could not clear its
// backoff -- 62 of 140 backing-off destinations were stuck that way, against a
// cap of a year.
//
// The general rule this taught, and the reason to prefer writing: Synapse has
// readers and triggers that are not enumerated anywhere, so a primary sender
// should fill its tables the way a Python sender would and keep its own copy
// only where it needs a different answer. internal/state.SetRetryTimings holds
// the copy this worker's own sending decisions are made from, so a stale cache
// on Synapse's side cannot affect what WE send.
//
// The staleness also has a cure built into the same trigger: when the server
// contacts us, the inbound path calls Synapse's own setter, which invalidates
// the cache properly everywhere.

// SetDestinationRetryTimings is Synapse's set_destination_retry_timings
// (transactions.py:265).
//
// The WHERE clause is the whole point and is copied deliberately. Several
// senders and several requests race on the same destination, so the upsert only
// takes effect when the new value is a reset (interval or last_ts zero), when
// there was no interval before, or when it is a LONGER backoff or a LATER
// attempt than what is already recorded.
//
// Without it, two concurrent failures can leave the shorter backoff winning and
// a dead server gets hammered; and a success racing a failure can clear a
// backoff that should have stood.
func (w *Writer) SetDestinationRetryTimings(ctx context.Context, destination string, failureTS, retryLastTS, retryInterval int64) error {
	ctx = dbtrace.WithQueryName(ctx, "set_destination_retry_timings")
	const q = `
		INSERT INTO destinations (destination, failure_ts, retry_last_ts, retry_interval)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (destination) DO UPDATE SET
			failure_ts = EXCLUDED.failure_ts,
			retry_last_ts = EXCLUDED.retry_last_ts,
			retry_interval = EXCLUDED.retry_interval
		WHERE EXCLUDED.retry_interval = 0
		   OR EXCLUDED.retry_last_ts = 0
		   OR destinations.retry_interval IS NULL
		   OR destinations.retry_interval < EXCLUDED.retry_interval
		   OR destinations.retry_last_ts < EXCLUDED.retry_last_ts`

	var failure any
	if failureTS != 0 {
		failure = failureTS
	}
	if _, err := w.pool.Exec(ctx, q, destination, failure, retryLastTS, retryInterval); err != nil {
		return fmt.Errorf("store: retry timings for %s: %w", destination, err)
	}
	return nil
}

// ClearDestinationRetryTimings records a destination as healthy again.
//
// Synapse's success path sets failure_ts NULL and both retry columns to zero
// (retryutils.py:226). Separate from the failure path because the reset must go
// through even when a concurrent failure has just written a longer backoff --
// which is exactly what the WHERE clause above lets through on
// `EXCLUDED.retry_interval = 0`.
func (w *Writer) ClearDestinationRetryTimings(ctx context.Context, destination string) error {
	return w.SetDestinationRetryTimings(ctx, destination, 0, 0, 0)
}

// Stat reports the connection pool's saturation.
//
// The pool is the shared resource this worker contends on with itself: a
// thousand destination goroutines resolving rooms all queue for the same
// max_conns connections, so "slow query" and "waited for a connection" look
// identical from a duration alone and are fixed differently.
func (w *Writer) Stat() *pgxpool.Stat {
	if w == nil || w.pool == nil {
		return nil
	}
	return w.pool.Stat()
}
