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

// Synapse's `destinations` retry columns are deliberately NOT written here.
//
// A Python sender writes them through set_destination_retry_timings
// (transactions.py:265), which also invalidates a cache
// (get_destination_retry_timings, transactions.py:169) and streams that
// invalidation to every other Synapse process. A write from out here does the
// first half and cannot do the second, so every process goes on serving what it
// cached -- and a backoff we CLEARED would keep Synapse from talking to a server
// that is back up, for as long as the cached interval says.
//
// So this worker keeps its own backoff in its own schema; see
// internal/state.SetRetryTimings, which also explains why publishing the
// invalidation on the caches stream would be a worse trade than a stale read.
// The writer keeps last_successful_stream_ordering and destination_rooms below,
// which are read straight from the database and cached nowhere.

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
