// Package state stores this worker's own stream cursors.
//
// It is the only thing the worker writes, anywhere, and it exists because none
// of the mechanisms a real federation sender uses to remember its place are
// available to a shadow:
//
//   - federation_stream_position is keyed (type, instance_name), and Synapse
//     rewrites it at every startup from MIN(stream_id) across the configured
//     senders, deleting rows for instances it does not recognise. A row of ours
//     would either be deleted or drag both real senders backwards.
//   - device_federation_outbox and device_lists_outbound_pokes are consumed by
//     DELETE once a transaction succeeds. We must not consume them, so without
//     our own high-water marks we would re-read the same device pokes on every
//     pass, forever.
//
// The table lives in its own schema under its own role, which can reach nothing
// else, and nothing in Synapse reads it. See deploy/state-role.sql and
// docs/shadow-safety.md.
package state

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/dbtrace"
)

// Cursor names. The per-destination forms are built by the To* helpers below.
const (
	// CursorEvents is the PDU pickup position, our stand-in for
	// federation_stream_position(type='events').
	CursorEvents = "events"
)

// Store persists cursors.
type Store struct {
	pool  *pgxpool.Pool
	table string
	// routesTable mirrors Synapse's destination_rooms, for the comparison.
	routesTable string
	// retryTable holds our own per-destination backoff. Empty disables it.
	retryTable string
	// instance is the sender we impersonate, so two shadows of two different
	// senders can share a database without overwriting each other.
	instance string
}

// Config describes how to reach the cursor table.
type Config struct {
	DSN   string
	Table string
	// RoutesTable holds our copy of Synapse's destination_rooms. Empty
	// disables route recording, which only makes sense for a worker that is
	// not being compared against anything.
	RoutesTable string
	// RetryTable holds our own per-destination backoff, kept out of Synapse's
	// `destinations` table because Synapse caches that one and only its own
	// writes invalidate the cache. See SetRetryTimings. Empty keeps the backoff
	// in memory for this process's lifetime only.
	RetryTable     string
	InstanceName   string
	MaxConns       int32
	ConnectTimeout time.Duration
	// OnQuery is told about every query this pool runs, labelled by
	// dbtrace.WithQueryName. Optional; nil disables tracing.
	OnQuery dbtrace.Observer
}

// Open connects and verifies the table is reachable and writable.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Table == "" {
		return nil, fmt.Errorf("state: no table configured")
	}
	if cfg.InstanceName == "" {
		return nil, fmt.Errorf("state: no instance name configured")
	}

	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("state: parse dsn: %w", err)
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
		return nil, fmt.Errorf("state: connect: %w", err)
	}
	s := &Store{pool: pool, table: cfg.Table, routesTable: cfg.RoutesTable,
		retryTable: cfg.RetryTable, instance: cfg.InstanceName}

	// Read one row rather than merely pinging. The failure this catches is the
	// state role being pointed at a database where the schema was never
	// created, which otherwise surfaces as the worker restarting from zero on
	// every deploy while looking healthy.
	if _, _, err := s.Get(ctx, CursorEvents); err != nil {
		pool.Close()
		return nil, fmt.Errorf("state: %s is not readable: %w", cfg.Table, err)
	}
	return s, nil
}

// Close releases the pool.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Get reads a cursor. ok is false when it has never been set, which is
// different from zero: a cursor at zero has been written and means "start from
// the beginning", while an unset one means "we have never run".
func (s *Store) Get(ctx context.Context, name string) (pos int64, ok bool, err error) {
	ctx = dbtrace.WithQueryName(ctx, "state_position_get")
	q := fmt.Sprintf(
		`SELECT position FROM %s WHERE instance_name = $1 AND name = $2`, s.table)
	err = s.pool.QueryRow(ctx, q, s.instance, name).Scan(&pos)
	if err == pgx.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("state: get %q: %w", name, err)
	}
	return pos, true, nil
}

// GetAll reads every cursor for this instance, for startup logging and for
// seeding several streams in one round trip.
func (s *Store) GetAll(ctx context.Context) (map[string]int64, error) {
	ctx = dbtrace.WithQueryName(ctx, "state_positions_all")
	q := fmt.Sprintf(`SELECT name, position FROM %s WHERE instance_name = $1`, s.table)
	rows, err := s.pool.Query(ctx, q, s.instance)
	if err != nil {
		return nil, fmt.Errorf("state: get all: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var name string
		var pos int64
		if err := rows.Scan(&name, &pos); err != nil {
			return nil, fmt.Errorf("state: get all: %w", err)
		}
		out[name] = pos
	}
	return out, rows.Err()
}

// Set advances a cursor.
//
// It never moves one backwards. A cursor going backwards would make the worker
// re-send everything between the two positions, and the ways it could happen --
// two workers sharing an instance name, a delayed write landing after a newer
// one -- are all cases where the older value is simply wrong. Refusing the
// regression in SQL means it cannot happen even under a race.
func (s *Store) Set(ctx context.Context, name string, pos int64) error {
	ctx = dbtrace.WithQueryName(ctx, "set_stream_position")
	// Aliased as sp so the ON CONFLICT clause can refer to the target row.
	// A schema-qualified name is not usable there, and the alias is the
	// portable way to say it.
	q := fmt.Sprintf(`
		INSERT INTO %s AS sp (instance_name, name, position, updated_ts)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (instance_name, name) DO UPDATE
		SET position = EXCLUDED.position, updated_ts = EXCLUDED.updated_ts
		WHERE sp.position < EXCLUDED.position`, s.table)

	_, err := s.pool.Exec(ctx, q, s.instance, name, pos, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("state: set %q: %w", name, err)
	}
	return nil
}

// ToDeviceCursor and DeviceListCursor name the per-destination high-water marks
// that stand in for the rows we may not delete.
func ToDeviceCursor(destination string) string { return "to_device:" + destination }

// DeviceListCursor is the per-destination device list poke cursor.
func DeviceListCursor(destination string) string { return "device_lists:" + destination }

// RoutedRoom is one routing decision in the shape Synapse records it.
type RoutedRoom struct {
	Destination    string
	RoomID         string
	StreamOrdering int64
}

// RecordRoutes writes our routing decisions in the same shape as Synapse's
// destination_rooms table.
//
// This is what makes the shadow checkable. Synapse's _send_pdu upserts
// (destination, room_id) -> stream_ordering for the full sharded destination
// set, BEFORE the retry filter, so destination_rooms is its own durable record
// of every routing decision it made -- independent of whether delivery
// succeeded, and written by Synapse rather than reconstructed by us. Keeping
// ours in the identical shape turns "did we agree?" into a SQL join.
//
// Like Synapse's, this is a high-water mark rather than a log: one row per
// (destination, room), holding the newest stream ordering routed there.
func (s *Store) RecordRoutes(ctx context.Context, routes []RoutedRoom) error {
	ctx = dbtrace.WithQueryName(ctx, "record_routes")
	if len(routes) == 0 {
		return nil
	}
	q := fmt.Sprintf(`
		INSERT INTO %s AS dr (instance_name, destination, room_id, stream_ordering, updated_ts)
		SELECT $1, d, r, o, $5
		FROM unnest($2::text[], $3::text[], $4::bigint[]) AS t(d, r, o)
		ON CONFLICT (instance_name, destination, room_id) DO UPDATE
		SET stream_ordering = EXCLUDED.stream_ordering, updated_ts = EXCLUDED.updated_ts
		WHERE dr.stream_ordering < EXCLUDED.stream_ordering`, s.routesTable)

	dests := make([]string, len(routes))
	rooms := make([]string, len(routes))
	orders := make([]int64, len(routes))
	for i, r := range routes {
		dests[i], rooms[i], orders[i] = r.Destination, r.RoomID, r.StreamOrdering
	}

	_, err := s.pool.Exec(ctx, q, s.instance, dests, rooms, orders, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("state: record routes: %w", err)
	}
	return nil
}

// RetryTimings is one destination's backoff.
type RetryTimings struct {
	// FailureTS is when the destination started failing, or zero.
	FailureTS int64
	// RetryLastTS is the last attempt, and RetryInterval how long to wait from
	// it. Both zero means the destination is not backing off.
	RetryLastTS   int64
	RetryInterval int64
}

// GetRetryTimings reads the persisted backoff for some destinations.
//
// Ours rather than Synapse's `destinations` table, and the reason is not
// tidiness -- see SetRetryTimings.
func (s *Store) GetRetryTimings(ctx context.Context, destinations []string) (map[string]RetryTimings, error) {
	ctx = dbtrace.WithQueryName(ctx, "state_retry_timings_get")
	if s.retryTable == "" || len(destinations) == 0 {
		return nil, nil
	}
	q := fmt.Sprintf(`
		SELECT destination, failure_ts, retry_last_ts, retry_interval
		FROM %s WHERE instance_name = $1 AND destination = ANY($2)`, s.retryTable)

	rows, err := s.pool.Query(ctx, q, s.instance, destinations)
	if err != nil {
		return nil, fmt.Errorf("state: get retry timings: %w", err)
	}
	defer rows.Close()

	out := make(map[string]RetryTimings)
	for rows.Next() {
		var d string
		var t RetryTimings
		if err := rows.Scan(&d, &t.FailureTS, &t.RetryLastTS, &t.RetryInterval); err != nil {
			return nil, fmt.Errorf("state: get retry timings: %w", err)
		}
		out[d] = t
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: get retry timings: %w", err)
	}
	return out, nil
}

// SetRetryTimings writes one destination's backoff. Zero timings clear it.
//
// This is deliberately NOT Synapse's `destinations` table, which is where a
// Python sender keeps the same thing. Synapse caches those timings
// (get_destination_retry_timings is @cached, transactions.py:169) and
// invalidates the cache only from its own write path, by streaming the
// invalidation to every other process. A write from outside Synapse cannot do
// that, so every process would go on serving the value it cached before we
// wrote -- and the harmful direction is the one that matters most: we clear a
// backoff when a destination recovers, Synapse keeps refusing to talk to a
// server that is now up, for as long as the cached interval says, which with
// Synapse's defaults is up to seven days.
//
// The obvious fix -- publish the invalidation on the caches stream -- is worse
// than the problem. It would make this worker a writer of that stream, and
// MultiWriterIdGenerator computes the stream's persisted-upto position as the
// minimum across the OTHER writers' positions (id_generators.py:787). A writer
// that publishes once and then goes quiet -- which is exactly the shape of a
// backoff, rare and bursty -- pins that position in every Synapse process,
// freezing cache-invalidation cleanup and anything waiting on the stream. Our
// worker being idle or stopped would then degrade the homeserver. That is the
// perturbation the subscribe-only rule exists to prevent (see
// internal/replication), and it is a worse failure than a stale read.
//
// So the backoff lives here, in the schema we own, where nothing else has
// cached it. Synapse still keeps its own for its own outbound requests, written
// and invalidated by Synapse, exactly as it does when no sender is running.
func (s *Store) SetRetryTimings(ctx context.Context, destination string, t RetryTimings) error {
	ctx = dbtrace.WithQueryName(ctx, "state_retry_timings_set")
	if s.retryTable == "" {
		return nil
	}
	q := fmt.Sprintf(`
		INSERT INTO %s (instance_name, destination, failure_ts, retry_last_ts,
		                retry_interval, updated_ts)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (instance_name, destination) DO UPDATE
		SET failure_ts = EXCLUDED.failure_ts,
		    retry_last_ts = EXCLUDED.retry_last_ts,
		    retry_interval = EXCLUDED.retry_interval,
		    updated_ts = EXCLUDED.updated_ts`, s.retryTable)

	_, err := s.pool.Exec(ctx, q, s.instance, destination,
		t.FailureTS, t.RetryLastTS, t.RetryInterval, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("state: set retry timings for %s: %w", destination, err)
	}
	return nil
}

// Stat reports the connection pool's saturation.
//
// The pool is the shared resource this worker contends on with itself: a
// thousand destination goroutines resolving rooms all queue for the same
// max_conns connections, so "slow query" and "waited for a connection" look
// identical from a duration alone and are fixed differently.
func (s *Store) Stat() *pgxpool.Stat {
	if s == nil || s.pool == nil {
		return nil
	}
	return s.pool.Stat()
}
