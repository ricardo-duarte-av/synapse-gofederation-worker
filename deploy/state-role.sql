-- Write role for the gofederation worker's own cursors.
--
-- This is the ONLY thing this worker writes, anywhere. It exists because the
-- worker cannot use the mechanisms a real federation sender uses to remember
-- where it got to:
--
--   * federation_stream_position is keyed (type, instance_name) and Synapse
--     rewrites it at startup from MIN(stream_id) across instances, deleting
--     rows for instances not in federation_sender_instances. Our row would
--     either vanish or drag both real senders backwards.
--   * device_federation_outbox and device_lists_outbound_pokes are consumed by
--     DELETE. We must not consume them, so without our own high-water marks we
--     would replay the same device pokes on every pass, forever.
--
-- The role can touch exactly one schema, which contains exactly this table, and
-- nothing in Synapse reads it.
--
-- Run as a superuser against the Synapse database:
--   psql -h /var/sockets -U synapse -d synapse-db -f state-role.sql
--
-- To undo:  DROP SCHEMA gofederation CASCADE; DROP ROLE gofed_state;

BEGIN;

CREATE ROLE gofed_state WITH LOGIN;
GRANT CONNECT ON DATABASE "synapse-db" TO gofed_state;

CREATE SCHEMA IF NOT EXISTS gofederation AUTHORIZATION gofed_state;

CREATE TABLE IF NOT EXISTS gofederation.stream_positions (
    -- The worker instance these cursors belong to, so two shadows of two
    -- different senders can share a database without colliding.
    instance_name TEXT NOT NULL,
    -- What the cursor counts: 'events', 'to_device', 'device_lists', or
    -- 'to_device:<destination>' and 'device_lists:<destination>' for the
    -- per-destination marks that replace the rows we may not delete.
    name          TEXT NOT NULL,
    position      BIGINT NOT NULL,
    updated_ts    BIGINT NOT NULL,
    PRIMARY KEY (instance_name, name)
);

-- Our copy of Synapse's destination_rooms, in the identical shape.
--
-- This is what makes the shadow checkable rather than merely self-consistent.
-- Synapse's _send_pdu upserts (destination, room_id) -> stream_ordering for the
-- full sharded destination set BEFORE the retry filter, so destination_rooms is
-- its own durable record of every routing decision it made. Keeping ours in the
-- same shape turns "did we agree with Synapse?" into a SQL join between this
-- table and that one.
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

-- No grants on public: this role has no business reading Synapse's tables, and
-- the read-only role has no business writing these.
GRANT USAGE ON SCHEMA gofederation TO gofed_state;
GRANT SELECT, INSERT, UPDATE, DELETE ON gofederation.stream_positions TO gofed_state;
GRANT SELECT, INSERT, UPDATE, DELETE ON gofederation.destination_rooms TO gofed_state;
GRANT SELECT, INSERT, UPDATE, DELETE ON gofederation.destination_retry TO gofed_state;

-- The comparison reads Synapse's destination_rooms alongside ours. That is the
-- ONE Synapse table this role may see, and read-only: it is the oracle, not
-- something we write.
GRANT USAGE ON SCHEMA public TO gofed_state;
GRANT SELECT ON public.destination_rooms TO gofed_state;

ALTER ROLE gofed_state SET statement_timeout = '30s';

COMMIT;
