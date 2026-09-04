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
	// instance is the sender we impersonate, so two shadows of two different
	// senders can share a database without overwriting each other.
	instance string
}

// Config describes how to reach the cursor table.
type Config struct {
	DSN            string
	Table          string
	InstanceName   string
	MaxConns       int32
	ConnectTimeout time.Duration
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
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	if cfg.ConnectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("state: connect: %w", err)
	}
	s := &Store{pool: pool, table: cfg.Table, instance: cfg.InstanceName}

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
