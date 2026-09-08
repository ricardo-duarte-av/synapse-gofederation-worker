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

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/dbtrace"
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
	// OnQuery is told about every query this pool runs, labelled by
	// dbtrace.WithQueryName. Optional; nil disables tracing.
	OnQuery dbtrace.Observer
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

	// Synapse's database may sit behind pgcat or PgBouncer in TRANSACTION
	// pooling mode, where consecutive round trips can land on different backend
	// connections.
	//
	// QueryExecModeExec is the mode that survives that: it sends parse, bind
	// and execute as one round trip, so a pooler has no seam to switch on. The
	// obvious-looking DescribeExec does NOT -- pgx's own documentation says it
	// "may cause problems with connection poolers that switch the underlying
	// connection between round trips" (pgx conn.go:633), because it describes
	// the unnamed prepared statement on one round trip and executes it on the
	// next. Through pgcat that fails with "unnamed prepared statement does not
	// exist" (SQLSTATE 26000), which names the symptom and not the cause.
	//
	// The cost is that parameters and results use the text format and parameter
	// types are inferred from the Go values, so an argument of an ambiguous type
	// is rejected rather than guessed. Everything here passes strings, integers
	// and typed arrays, which are none of those. It is also one round trip
	// rather than two, so it is faster than what it replaces.
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
	ctx = dbtrace.WithQueryName(ctx, "check_read_only")
	var setting string
	err := s.pool.QueryRow(ctx, `SHOW default_transaction_read_only`).Scan(&setting)
	if err != nil {
		return false, fmt.Errorf("store: check read-only: %w", err)
	}
	return setting == "on", nil
}

// CurrentRole returns the role the pool is connected as, for startup logging.
func (s *Store) CurrentRole(ctx context.Context) (string, error) {
	ctx = dbtrace.WithQueryName(ctx, "current_role")
	var role string
	if err := s.pool.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		return "", fmt.Errorf("store: current_user: %w", err)
	}
	return role, nil
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
