-- The worker's own tables, without a role of its own.
--
-- Use this where the worker connects to PostgreSQL as SYNAPSE'S role -- which
-- is what happens when gofederation-worker.yaml leaves `database.dsn` and
-- `state.dsn` unset and the connection is built from homeserver.yaml. That
-- role already owns the database, so there is nothing to grant: only the schema
-- and the two tables are missing.
--
-- Where the worker has roles of its own -- a read-only one for Synapse's
-- tables and a separate writer for these -- use state-role.sql instead, which
-- creates the same two tables AND the gofed_state role that owns them. Running
-- both is harmless; every statement here is IF NOT EXISTS.
--
-- Run as the database owner, against the database homeserver.yaml names:
--   psql -h matrix-postgres -U testingsynapse -d testingsynapse -f state-tables.sql
--
-- The worker does NOT create these itself, and that is deliberate. Creating a
-- table means holding CREATE on the database at runtime, every run, to do
-- something that happens exactly once -- and this worker's whole shape is that
-- the smallest possible set of grants is the guarantee, not the intention. A
-- missing table is a clear startup failure with a file to run; a worker that
-- can create tables is a worker that can create them by accident, in the wrong
-- database, under the wrong search_path, on the day somebody points it at
-- production by mistake.
--
-- To undo:  DROP SCHEMA gofederation CASCADE;

BEGIN;

CREATE SCHEMA IF NOT EXISTS gofederation;

-- Where this worker got to. It cannot use the mechanisms a real federation
-- sender uses to remember that:
--
--   * federation_stream_position is keyed (type, instance_name) and Synapse
--     rewrites it at startup from MIN(stream_id) across instances.
--   * device_federation_outbox and device_lists_outbound_pokes are consumed by
--     DELETE, so without our own high-water marks we would replay the same
--     device pokes on every pass, forever.
--
-- Matches state.table in gofederation-worker.yaml.
CREATE TABLE IF NOT EXISTS gofederation.stream_positions (
    -- The worker instance these cursors belong to, so two workers can share a
    -- database without colliding.
    instance_name TEXT NOT NULL,
    -- What the cursor counts: 'events', 'to_device', 'device_lists', or
    -- 'to_device:<destination>' and 'device_lists:<destination>' for the
    -- per-destination marks that replace the rows we may not delete.
    name          TEXT NOT NULL,
    position      BIGINT NOT NULL,
    updated_ts    BIGINT NOT NULL,
    PRIMARY KEY (instance_name, name)
);

-- Our copy of Synapse's destination_rooms, in the identical shape, written in
-- BOTH modes -- a primary sender writes Synapse's table as well, and keeping
-- ours alongside is what lets the two be compared by a SQL join rather than by
-- reading logs. Matches state.routes_table; set that empty to skip this table.
CREATE TABLE IF NOT EXISTS gofederation.destination_rooms (
    instance_name   TEXT NOT NULL,
    destination     TEXT NOT NULL,
    room_id         TEXT NOT NULL,
    stream_ordering BIGINT NOT NULL,
    updated_ts      BIGINT NOT NULL,
    PRIMARY KEY (instance_name, destination, room_id)
);

-- THIS SENDER's per-destination backoff.
--
-- Synapse keeps the same thing in its `destinations` table, and this
-- deliberately does not go there. Synapse caches those timings in every process
-- (get_destination_retry_timings, transactions.py:169) and only its own writes
-- invalidate the cache, streaming the invalidation to the other processes. A
-- write from outside Synapse does the write and not the invalidation, so every
-- process serves what it cached -- and a backoff we CLEARED when a destination
-- recovered would keep Synapse from talking to a server that is up, for as long
-- as the cached interval says. With Synapse's defaults that is up to seven days.
--
-- Publishing the invalidation on the caches stream would be worse: it makes
-- this worker a writer of that stream, whose persisted-upto position is the
-- minimum across the other writers (id_generators.py:787), so a worker that
-- publishes rarely -- which is exactly the shape of a backoff -- pins that
-- position in every Synapse process for as long as it stays quiet.
--
-- Matches state.retry_table in gofederation-worker.yaml.
CREATE TABLE IF NOT EXISTS gofederation.destination_retry (
    instance_name  TEXT NOT NULL,
    destination    TEXT NOT NULL,
    -- Milliseconds since epoch, matching Synapse's columns. Zero everywhere
    -- means the destination is not backing off.
    failure_ts     BIGINT NOT NULL,
    retry_last_ts  BIGINT NOT NULL,
    retry_interval BIGINT NOT NULL,
    updated_ts     BIGINT NOT NULL,
    PRIMARY KEY (instance_name, destination)
);

-- The comparison scans by ordering to pick a settled window.
CREATE INDEX IF NOT EXISTS gofederation_destination_rooms_ordering
    ON gofederation.destination_rooms (instance_name, stream_ordering);

COMMIT;
