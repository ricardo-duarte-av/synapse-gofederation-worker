package state

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-gofederation-worker/internal/dbtrace"
)

// The worker's tables, in the shape deploy/state-tables.sql gives them. Each
// takes the configured name; the comments on what the columns mean live there.
const (
	positionsDDL = `CREATE TABLE IF NOT EXISTS %s (
		instance_name TEXT NOT NULL,
		name          TEXT NOT NULL,
		position      BIGINT NOT NULL,
		updated_ts    BIGINT NOT NULL,
		PRIMARY KEY (instance_name, name)
	)`
	routesDDL = `CREATE TABLE IF NOT EXISTS %s (
		instance_name   TEXT NOT NULL,
		destination     TEXT NOT NULL,
		room_id         TEXT NOT NULL,
		stream_ordering BIGINT NOT NULL,
		updated_ts      BIGINT NOT NULL,
		PRIMARY KEY (instance_name, destination, room_id)
	)`
	// Named after the table, which for the default name is exactly the index
	// state-tables.sql creates, so running the file afterwards is a no-op.
	routesIndexDDL = `CREATE INDEX IF NOT EXISTS %s_ordering
		ON %s (instance_name, stream_ordering)`
	retryDDL = `CREATE TABLE IF NOT EXISTS %s (
		instance_name  TEXT NOT NULL,
		destination    TEXT NOT NULL,
		failure_ts     BIGINT NOT NULL,
		retry_last_ts  BIGINT NOT NULL,
		retry_interval BIGINT NOT NULL,
		updated_ts     BIGINT NOT NULL,
		PRIMARY KEY (instance_name, destination)
	)`
)

// ensureTables creates whichever of the configured tables do not exist, and
// returns the names it created.
//
// It runs no DDL at all when they are all present. That matters for a narrow
// role such as gofed_state, which owns its tables but has no CREATE on the
// database: CREATE SCHEMA checks that privilege before IF NOT EXISTS, so even
// a no-op would fail for it.
//
// It refuses to create anything in a database without Synapse's tables. The
// failure this guards against is a state.dsn pointed at the wrong database,
// where tables appearing quietly would turn into a worker that starts from
// zero and looks healthy -- the reason this used to be a manual step at all.
func ensureTables(ctx context.Context, pool *pgxpool.Pool, cfg Config) ([]string, error) {
	ctx = dbtrace.WithQueryName(ctx, "state_ensure_tables")

	type table struct {
		name string
		ddl  []string
	}
	var missing []table
	for _, t := range []table{
		{cfg.Table, []string{positionsDDL}},
		{cfg.RoutesTable, []string{routesDDL, routesIndexDDL}},
		{cfg.RetryTable, []string{retryDDL}},
	} {
		if t.name == "" {
			continue
		}
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, t.name).
			Scan(&exists); err != nil {
			return nil, fmt.Errorf("state: look for %s: %w", t.name, err)
		}
		if !exists {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		return nil, nil
	}

	var synapse bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('destination_rooms') IS NOT NULL
		AND to_regclass('federation_stream_position') IS NOT NULL`).Scan(&synapse); err != nil {
		return nil, fmt.Errorf("state: look for Synapse's tables: %w", err)
	}
	if !synapse {
		return nil, fmt.Errorf("state: %s does not exist, and state.dsn reaches a database "+
			"without Synapse's tables, so it is not created there; check state.dsn, or "+
			"create it by hand with deploy/state-tables.sql", missing[0].name)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("state: create tables: %w", err)
	}
	defer tx.Rollback(ctx)

	// Two workers starting together on one database would otherwise race on
	// IF NOT EXISTS, which is not atomic against a concurrent CREATE.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtext('gofederation-worker state tables'))`); err != nil {
		return nil, fmt.Errorf("state: create tables: %w", err)
	}

	var created []string
	for _, t := range missing {
		if schema, _, ok := strings.Cut(t.name, "."); ok {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT to_regnamespace($1) IS NOT NULL`, schema).
				Scan(&exists); err != nil {
				return nil, fmt.Errorf("state: look for schema %s: %w", schema, err)
			}
			if !exists {
				if _, err := tx.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
					return nil, fmt.Errorf("state: create schema %s: %w", schema, err)
				}
			}
		}
		for _, ddl := range t.ddl {
			q := fmt.Sprintf(ddl, t.name)
			if strings.Count(ddl, "%s") == 2 {
				q = fmt.Sprintf(ddl, strings.ReplaceAll(t.name, ".", "_"), t.name)
			}
			if _, err := tx.Exec(ctx, q); err != nil {
				return nil, fmt.Errorf("state: create %s: %w", t.name, err)
			}
		}
		created = append(created, t.name)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("state: create tables: %w", err)
	}
	return created, nil
}
