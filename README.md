# synapse-gofederation-worker

A Synapse federation sender written in Go, which runs as a **shadow** of an
existing sender until it has earned the right to send.

## Why

`aguiarvieira.pt` runs two Python federation senders. Federation sending is the
most fan-out-heavy job in the deployment: one event in a large room becomes a
transaction to a thousand destinations, and Python's per-destination
`PerDestinationQueue` coroutines are where that cost lands. This worker makes
each destination a goroutine instead.

But a federation sender is outward-facing. A mistake sends duplicate or
malformed transactions to real homeservers, and the tables it drives are
consumed **destructively** — Synapse deletes rows from
`device_federation_outbox` and `device_lists_outbound_pokes` once a transaction
succeeds. You cannot swap one in and watch.

So this shadows. It impersonates the shard of an existing sender and runs the
entire pipeline — replication, sharding, destination resolution, per-destination
queueing, transaction assembly and signing — and then stops. Nothing is sent,
nothing is written to any Synapse table, nothing is published to Redis. The
output is metrics and a persisted comparison record. When those agree with the
real sender over weeks, one flag flips.

## Status

Shadow mode only. `shadow.enabled: false` is currently wired to a startup
refusal rather than to an HTTP sender, because failing closed is the only
acceptable direction for that flag.

Implemented: PDU routing, to-device and device-list EDUs, per-destination
queues, transaction assembly and signing, the persisted shadow record, metrics.

Not yet: the real HTTP sender (server discovery, well-known, SRV, backoff),
receipts, typing and presence EDUs, catch-up, full state resolution for forked
DAGs, and the full `m.device_list_update` body (`prev_id`, `deleted`, `keys`,
`device_display_name`).

## The three rules

Everything else is ordinary code that can be wrong without anyone outside
noticing. These three are not — see [docs/shadow-safety.md](docs/shadow-safety.md).

1. **Never publish to Redis.** A real sender publishes `REPLICATE` and
   `FEDERATION_ACK`; both perturb the live cluster. Enforced by a test that
   parses this package's AST and fails on any publish.
2. **Never write to a Synapse table.** Enforced by the database, via a role with
   only `SELECT` and `default_transaction_read_only` set.
3. **Keep our own cursors.** Because of (2), and because the device tables are
   consumed by `DELETE`. One table, its own schema, its own role.

## Verification

**How we confirm we match Synapse — not just that we are self-consistent — is
[docs/verification.md](docs/verification.md).** Transaction framing is
deliberately not compared: which PDUs share a transaction depends on what was
queued when a sender's loop ran, so two correct senders differ there and
matching it would be fitting to noise. Three things that *are* comparable are,
each against an oracle produced by Synapse itself:

| Question | Oracle | Result |
|---|---|---|
| Same events to the same servers? | Synapse's `destination_rooms` | 507/507 pairs, **100%** |
| Same PDU bytes? | Synapse's `canonicaljson` | 200 events, **byte-identical** |
| Same shard? | Synapse's `ShardedWorkerHandlingConfig` | 27,824 destinations, **identical** |
| Valid signature? | Synapse's `signedjson` | **byte-identical** |

Against the live deployment, over ~9,000 real events:

- **Shard parity is exact.** All 27,824 destinations partition identically to
  Synapse's own `ShardedWorkerHandlingConfig`. This is the check everything
  else rests on: a wrong shard function still produces a uniform, stable,
  entirely plausible split, so nothing else would reveal it.
- **Signatures match Synapse byte for byte**, cross-checked against
  `signedjson` and `canonicaljson` running inside a real federation sender
  container — including the case Go's `encoding/json` gets wrong by default,
  since it HTML-escapes `<`, `>` and `&` and canonical JSON does not.
- **It is inert.** KeyDB `MONITOR` while it ran shows one `SUBSCRIBE` and not
  one publish carrying our name.
- **Destinations are resolved exactly** for 95.6% of routing decisions, from
  the state before the event via its prev events' state group — the same thing
  Synapse computes, not an approximation of it.

```sh
go test ./...                     # unit tests, no database needed

# Live tests, each skipped unless its env vars are set:
SYNAPSE_DSN="host=/var/sockets user=gofed_ro dbname=synapse-db" go test ./internal/store/
HOMESERVER_YAML=/opt/matrix/synapse/synapse/homeserver.yaml \
  SIGNING_KEY=/opt/matrix/synapse/synapse/aguiarvieira.pt.signing.key \
  go test ./internal/synapsecfg/
SHARD_PARITY_FILE=py-shards.tsv SHARD_INSTANCES=w1,w2 go test ./internal/sharding/
```

`internal/sharding/live_test.go` documents how to generate the parity fixture
from Synapse itself.

## Running it

```sh
cp deploy/gofederation-worker.example.yaml gofederation-worker.yaml
psql -h /var/sockets -U synapse -d synapse-db -f deploy/readonly-role.sql
psql -h /var/sockets -U synapse -d synapse-db -f deploy/state-role.sql

./gofederation-worker -config gofederation-worker.yaml -check   # validate and exit
./gofederation-worker -config gofederation-worker.yaml
```

`worker_name` is this process's own identity and must not be one of Synapse's
`federation_sender_instances`; `shadow.instance` is the sender whose shard it
takes. Conflating them makes the bus suppress the shadowed sender's rows as our
own echo, and the worker processes nothing while looking perfectly healthy —
which is how that bug was found.

Everything derivable from Synapse's config is read from `homeserver.yaml` at
every start rather than copied. `federation_sender_instances` is not a value we
consume but the value that *defines* which destinations are ours, since the
shard is `sha256(destination) mod len(instances)`; a stale copy would silently
reassign every destination on the server.

## Promotion

`shadow.enabled: false` is the only switch that makes this send real traffic,
and flipping it is a decision read from the difflog rather than from the code
looking finished. The gate is a sustained match against the sender being
shadowed, which is why those counters are persisted across restarts.

The number to watch is `gofed_approximate_routes_total`, which counts routing
decisions that fell back to current room state instead of resolving the state
before the event as Synapse does. It is labelled by cause, and the two causes
mean different things: `forked dag` is work not done here — full state
resolution is room-version-specific and expensive — while
`prev event has no state group` is an outlier nobody could have resolved.

Over 14,485 production events it sits at **4.4% of routing decisions** (20 of
450), all forked DAGs. Everything else is resolved exactly, because 98% of
local events have a single prev event and therefore a single state group with
nothing to resolve.

## Documentation

- [docs/synapse-reference.md](docs/synapse-reference.md) — what is reimplemented,
  with the file:line in Synapse each rule comes from.
- [docs/shadow-safety.md](docs/shadow-safety.md) — the invariants, and how to
  verify inertness from outside.
