# Shadow safety

This worker runs alongside two live Python federation senders on a production
homeserver. It must be impossible for it to affect them. Three invariants carry
that guarantee; everything else in the codebase is ordinary code that can be
wrong without anyone outside noticing.

If you are changing code in `internal/replication`, `internal/store` or
`internal/sink`, read this first.

## 1. Never publish to Redis

A real federation sender publishes on connect and after every transaction:

- `REPLICATE` — asks every worker on the bus to broadcast its stream positions.
  One extra publisher means every other worker does extra work on our account.
- `FEDERATION_ACK <instance> <token>` — tells the main process that we have
  consumed the federation stream up to `token`. Master clears its in-memory
  queues up to `min()` across instances (`send_queue.py:282`), so an ack from a
  worker that has not actually delivered anything could drop EDUs that a real
  sender had not yet taken.

We are **SUBSCRIBE-only**, and there is no code path that publishes. The
subscriber takes a Redis client that has no `Publish` available to it rather
than relying on discipline. `synapse-gosync-worker/internal/replication` made
the same choice for the same reason and its comments are worth reading.

The consequence we accept: we never receive the `POSITION` broadcast that a
`REPLICATE` would have triggered, so positions are seeded from the database at
startup and corrected as RDATA arrives. A seeded position is a lower bound,
which for a shadow means at worst we re-examine events we already examined.

## 2. Never write to a Synapse table

Enforced outside the process, by `deploy/readonly-role.sql`: the `gofed_ro` role
has `SELECT` and nothing else on Synapse's schema. A bug that tries to write
gets a Postgres error, not a corrupted homeserver.

The startup check asserts the role really is read-only and exposes it as a
gauge, following the precedent in gosync-worker. It warns rather than refuses,
because a deployment may legitimately point at a scratch database.

Three tables would be actively destructive to touch:

- `federation_stream_position` — see `docs/synapse-reference.md` §7. Synapse
  takes `MIN(stream_id)` across instance rows and deletes rows for instances not
  in `federation_sender_instances`. Our row would either vanish or rewind every
  real sender.
- `device_lists_outbound_pokes` — Synapse **deletes** rows once a transaction
  succeeds (`devices.py:922`). Consuming them would silently stop real device
  list updates reaching real servers, breaking E2EE for users, with no error
  anywhere.
- `device_federation_outbox` — same, for to-device messages
  (`deviceinbox.py:669`).

`destinations` and `destination_rooms` are less dramatic but still shared state
driving both real senders' backoff and catch-up.

## 3. Keep our own positions

Follows from (2). We cannot use `federation_stream_position`, and we cannot mark
device pokes as sent, so without our own bookkeeping the worker would replay the
same device pokes on every pass forever.

`internal/state` owns one table in its own schema under a second, narrowly
granted role. It is the only writable object in the entire deployment, it
contains only our own cursors, and nothing in Synapse reads it.

## What "dry-run" means precisely

With `shadow.enabled: true`, the pipeline runs to completion — destinations
resolved, shard applied, queues filled, transaction assembled, canonical JSON
produced, **signed with the real key** — and then handed to a sink that logs and
counts it instead of opening a connection.

Signing really happens, deliberately. It is cheap, it is a step that can be
wrong, and a signature computed over the wrong bytes is exactly the class of bug
that would be invisible until the day we go live. The signature is over a
`uri`/`origin`/`destination`/`content` object, not over anything that leaves the
process, so producing one has no external effect.

No HTTP client is constructed in shadow mode at all. The sink interface has two
implementations and the dry-run one has no network dependency to accidentally
use.

## Verifying inertness from outside

Do not take the code's word for it:

```sh
# No PUBLISH from us. Run while the worker is up; expect only Synapse's own.
redis-cli -s /var/sockets/keydb MONITOR | grep -i publish

# The role has no write grants on Synapse's tables.
psql -c "\dp destinations device_federation_outbox device_lists_outbound_pokes"

# No outbound federation connections from our container.
ss -tnp | grep <container-pid>
```

## The promotion gate

`shadow.enabled: false` is the only switch that makes this worker send real
traffic, and flipping it is a decision made from the difflog, not from the code
looking finished. It is gated on a sustained destination-set and PDU-inclusion
match rate against the sender we shadow, measured over weeks — which is why the
difflog counters are persisted across restarts rather than living in Prometheus
alone.
