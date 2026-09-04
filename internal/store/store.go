// Package store provides read-only access to Synapse's PostgreSQL database.
//
// Every query here is a SELECT, and the role it connects as has only SELECT
// granted with default_transaction_read_only set (deploy/readonly-role.sql), so
// a bug cannot write even if it tries.
//
// That is not merely defensive. A real federation sender CONSUMES what it
// reads: it deletes rows from device_federation_outbox and
// device_lists_outbound_pokes once a transaction succeeds, and advances
// destinations.last_successful_stream_ordering. This worker does none of that,
// and the asymmetry has to be handled deliberately rather than discovered --
// see internal/state for where our own cursors live, and docs/shadow-safety.md
// for why writing any of Synapse's tables would break the live homeserver.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store holds a pool of read-only connections to Synapse's database.
type Store struct {
	pool *pgxpool.Pool
}

// Config describes how to reach the database.
type Config struct {
	// DSN is a libpq connection string. For a unix socket, set host to the
	// directory containing .s.PGSQL.5432, e.g.
	// "host=/var/sockets user=gofed_ro dbname=synapse-db".
	DSN string
	// MaxConns bounds the pool. Zero uses pgx's default.
	MaxConns int32
	// ConnectTimeout bounds the initial connection.
	ConnectTimeout time.Duration
}

// Open connects and verifies the database is reachable.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = cfg.MaxConns
	}

	// Synapse's database may sit behind pgcat in transaction pooling mode,
	// where server-side prepared statements cannot be reused across
	// transactions. Describing statements on each exec keeps us compatible with
	// both a direct connection and a transaction-mode pooler.
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec

	if cfg.ConnectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Pool exposes the underlying pool for tests and metrics.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// IsReadOnly reports whether the connected role is restricted to reads.
//
// Checked at startup and reported rather than assumed: this worker runs against
// a production Synapse database whose tables two live federation senders depend
// on, and the guarantee is worth confirming out loud at every start rather than
// inferring from the deployment being correct.
func (s *Store) IsReadOnly(ctx context.Context) (bool, error) {
	var setting string
	err := s.pool.QueryRow(ctx, `SHOW default_transaction_read_only`).Scan(&setting)
	if err != nil {
		return false, fmt.Errorf("store: check read-only: %w", err)
	}
	return setting == "on", nil
}

// CurrentRole returns the role the pool is connected as, for startup logging.
func (s *Store) CurrentRole(ctx context.Context) (string, error) {
	var role string
	if err := s.pool.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		return "", fmt.Errorf("store: current_user: %w", err)
	}
	return role, nil
}
