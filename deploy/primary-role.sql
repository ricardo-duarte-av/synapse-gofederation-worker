-- Write role for a PRIMARY federation sender.
--
-- Only for a homeserver where this worker IS the configured sender
-- (mode: primary). A shadow deployment must never have this role: its whole
-- safety argument is that no writable handle to Synapse's tables exists in the
-- process at all. See docs/shadow-safety.md.
--
-- The grants are narrow on purpose. A federation sender's bookkeeping touches
-- five tables and nothing else; it has no business updating events, room state
-- or account data, and a role that could would turn a bug in the sender into a
-- corrupted homeserver.
--
-- Run as a superuser against the TEST homeserver's database:
--   psql -h /var/sockets -U synapse -d testing-db -f primary-role.sql
--
-- To undo:  DROP OWNED BY gofed_rw; DROP ROLE gofed_rw;

BEGIN;

CREATE ROLE gofed_rw WITH LOGIN;

GRANT CONNECT ON DATABASE "testing-db" TO gofed_rw;
GRANT USAGE ON SCHEMA public TO gofed_rw;

-- Reading is the same surface a shadow needs.
GRANT SELECT ON ALL TABLES IN SCHEMA public TO gofed_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO gofed_rw;

-- Writing is only these five.
--
--   destinations                        retry timings and the catch-up cursor
--   destination_rooms                   what each destination is owed
--   device_federation_outbox            DELETE consumed to-device messages
--   device_lists_outbound_pokes         DELETE consumed device list pokes
--   device_lists_outbound_last_success  where the next prev_id comes from
--   federation_stream_position          our position in the events stream
GRANT INSERT, UPDATE ON destinations TO gofed_rw;
GRANT INSERT, UPDATE ON destination_rooms TO gofed_rw;
GRANT DELETE ON device_federation_outbox TO gofed_rw;
GRANT DELETE ON device_lists_outbound_pokes TO gofed_rw;
GRANT INSERT, UPDATE ON device_lists_outbound_last_success TO gofed_rw;
GRANT INSERT, UPDATE ON federation_stream_position TO gofed_rw;

-- NOT read-only, unlike every other role this project defines. A sender's
-- cursor IS the deletion of the rows it has sent, so it cannot do its job
-- inside a read-only transaction. Store.Writer checks this at startup.
ALTER ROLE gofed_rw SET statement_timeout = '60s';

COMMIT;

-- Verify: the first two succeed, the third must fail.
--   psql -h /var/sockets -U gofed_rw -d testing-db -c 'select count(*) from destinations;'
--   psql -h /var/sockets -U gofed_rw -d testing-db -c "delete from device_federation_outbox where destination='nope';"
--   psql -h /var/sockets -U gofed_rw -d testing-db -c 'delete from events;'
