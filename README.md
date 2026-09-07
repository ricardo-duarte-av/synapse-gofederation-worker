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

Shadow by default, with a send allowlist for going live one destination at a
time. A real, signed transaction has been sent to a test homeserver and
accepted — the receiving Synapse verified our signature against the published
key. See [docs/going-live.md](docs/going-live.md).

Implemented: PDU routing, to-device and device-list EDUs, per-destination
queues, transaction assembly and signing, the outbound HTTP sender with Matrix
server discovery, transaction capture and comparison, the persisted shadow
record, metrics.

Not yet: our own persistent per-destination backoff (the retry filter still
reads Synapse's `destinations` table, which is another sender's bookkeeping),
catch-up, receipts/typing/presence EDUs, full state resolution for forked DAGs,
and the full `m.device_list_update` body (`prev_id`, `deleted`, `keys`,
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

## Deploying with Docker Compose

Two images are published from this repository:

| Image | Runs where | Does what |
|---|---|---|
| `ghcr.io/ricardo-duarte-av/synapse-gofederation-worker` | beside the **main** Synapse | the sender itself |
| `ghcr.io/ricardo-duarte-av/synapse-gofederation-worker/fedrecorder` | in front of the **test** homeserver | records what Synapse actually sent |

`docker-compose.yaml` in this repository defines both. They share a host but
belong to two different places, and the file says which is which; split them
into separate projects if they ever move apart.

### 1. Database roles

The worker reads Synapse's database through a role that cannot write, and keeps
its own bookkeeping under a second role that can reach nothing else:

```sh
psql -h /var/sockets -U synapse -d synapse-db -f deploy/readonly-role.sql
psql -h /var/sockets -U synapse -d synapse-db -f deploy/state-role.sql
```

`require_read_only: true` then makes a role with write access to Synapse's
tables a startup failure rather than a warning. Three of the tables this reads
are consumed by the real senders — see [docs/shadow-safety.md](docs/shadow-safety.md).

### 2. Configuration

```sh
cp deploy/gofederation-worker.example.yaml gofederation-worker.yaml
```

The paths in it are the ones inside the container, as mounted by the compose
file. Two fields decide whether it works at all:

- **`worker_name`** is this process's own identity and must **not** appear in
  `federation_sender_instances`. Startup refuses that, because the replication
  bus suppresses a worker's own echo by instance name — a shadow named after the
  sender it shadows discards that sender's rows and processes nothing while
  looking perfectly healthy.
- **`shadow.instance`** is the sender whose *shard* it takes. Different thing.

### 3. Bring it up

```sh
docker compose up -d gofederation-worker
docker compose logs -f gofederation-worker
```

The startup line answers the only question that matters:

```
INF starting ... shadow=true sink="dry-run (shadow; nothing is sent)"
INF SHADOW MODE: transactions will be built and signed but never sent, and no Synapse table will be written
```

`-check` validates the config and every connection without starting the worker,
which is the right thing to run after an edit:

```sh
docker compose run --rm gofederation-worker -check
```

### 4. The recording proxy

`fedrecorder` goes in front of the test homeserver. **Route only the send
endpoint to it:**

```
/_matrix/federation/v1/send/   ->  fedrecorder:8449
everything else                ->  testing-synapse:8008
```

It is then in the path only for what it records — client traffic, long-polling
`/sync` and media never touch it, so its uptime and timeouts are irrelevant to
everything but the comparison. Routing everything through it also works; it
forwards untouched and records nothing else.

**The proxy must not rewrite the path.** The `X-Matrix` signature covers the
request URI, so a stripped or rewritten prefix turns every transaction into a
401 that looks exactly like a signing bug and is not.

```sh
docker compose up -d fedrecorder
```

### 5. Compare

Both halves land in `./captures`, side by side. `fedcompare` is a local CLI
rather than an image — it reads two files and prints a report, so there is
nothing for a container to add:

```sh
go build ./cmd/fedcompare
./fedcompare \
  -synapse captures/synapse.jsonl \
  -worker  captures/worker.jsonl \
  -destination testing.aguiarvieira.pt
```

Exit status is 0 when the PDUs agree and 1 when they do not, so it drops
straight into a cron job or a CI step.

See [docs/test-destination.md](docs/test-destination.md) for what the report
will say that is *not* a fault: transaction counts differ because framing is
not comparable between two senders, and PDUs are matched on a content hash
because room version 12 carries no `event_id`.

### Volumes that must persist

`./difflog` and `./captures` are bind mounts on purpose. The difflog is the
promotion record and is measured in weeks; the captures are half of the
byte-level comparison and are read long after they are written. A volume that
reset on redeploy would make the promotion question unanswerable.

### Building the images yourself

```sh
docker build -t gofederation-worker .
docker build -f Dockerfile.fedrecorder -t fedrecorder .
```

CI builds both on every push (`.github/workflows/docker.yml`), gated on
`go vet`, `go test -race` and `gofmt`, and smoke-tests each image by running
`-version` and asserting that an unusable configuration exits non-zero.

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

## The test destination

The captures and the recording proxy for comparing against a homeserver we
control are built; the server itself is deployment work. See
[docs/test-destination.md](docs/test-destination.md) — including why the
subdomain has to be chosen so it hashes into the right shard, and why the first
room should be private.

- `cmd/fedrecorder` sits in front of the test homeserver and records what
  Synapse actually sent.
- `shadow.capture_destinations` records what this worker would have sent.
- `cmd/fedcompare` diffs the two.

## Documentation

- [docs/synapse-reference.md](docs/synapse-reference.md) — what is reimplemented,
  with the file:line in Synapse each rule comes from.
- [docs/shadow-safety.md](docs/shadow-safety.md) — the invariants, and how to
  verify inertness from outside.
- [docs/verification.md](docs/verification.md) — how we confirm we match
  Synapse, and what is still unverified.
- [docs/test-destination.md](docs/test-destination.md) — the capture rig.
- [docs/going-live.md](docs/going-live.md) — the send allowlist, and the order
  in which to widen it.
