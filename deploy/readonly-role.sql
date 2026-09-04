-- Read-only PostgreSQL role for the gofederation worker.
--
-- This worker shadows a live federation sender on a production homeserver. The
-- code never writes to a Synapse table; this role makes that a guarantee the
-- database enforces rather than a property of the code being correct.
--
-- Three of the tables it reads are CONSUMED by the real senders -- Synapse
-- deletes from device_federation_outbox and device_lists_outbound_pokes once a
-- transaction succeeds, and rewrites federation_stream_position at every
-- startup. A stray write there would not corrupt a row, it would silently stop
-- to-device messages and device list updates reaching real servers, breaking
-- E2EE for real users with no error anywhere. See docs/shadow-safety.md.
--
-- Run as a superuser against the Synapse database:
--   psql -h /var/sockets -U synapse -d synapse-db -f readonly-role.sql
--
-- To undo:  DROP OWNED BY gofed_ro; DROP ROLE gofed_ro;

BEGIN;

CREATE ROLE gofed_ro WITH LOGIN;

GRANT CONNECT ON DATABASE "synapse-db" TO gofed_ro;
GRANT USAGE ON SCHEMA public TO gofed_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO gofed_ro;

-- Covers tables added by future Synapse schema migrations.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO gofed_ro;

-- Belt and braces: reject writes at the transaction level. This is what
-- Store.IsReadOnly checks for at startup.
ALTER ROLE gofed_ro SET default_transaction_read_only = on;

-- A query that outlives the transaction it was for is pure waste. The catch-up
-- and fan-out queries here are all indexed lookups; a minute means something is
-- wrong, not that the query is big.
ALTER ROLE gofed_ro SET statement_timeout = '60s';

COMMIT;

-- Verify: the first succeeds, the second must fail.
--   psql -h /var/sockets -U gofed_ro -d synapse-db -c 'select count(*) from destinations;'
--   psql -h /var/sockets -U gofed_ro -d synapse-db -c 'create table t(x int);'
