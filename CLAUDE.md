# Working on synapse-gofederation-worker

A Synapse federation sender in Go. It reads Synapse's database and replication
bus and puts transactions on the wire, in place of Synapse's Python federation
sender workers.

Read `README.md` for what it does. This file is for things that are not
derivable from the code and are expensive to rediscover.

## The deployment this runs on

- **`aguiarvieira.pt` — PRIMARY mode, the only federation sender.**
  `federation_sender_instances` names this worker and nothing else, so if it is
  down the homeserver federates with nobody. There is no Python sender to fall
  back to and no shadow to compare against.
- **The `testing.aguiarvieira.pt` worker has been decommissioned.**
- **Presence is OFF** on the homeserver. Anything about presence in the code or
  the docs is therefore untested in production here; do not assume a presence
  change is exercised by simply deploying it.
- `homeserver.yaml` sets `destination_max_retry_interval: 365d` with a `x5`
  multiplier from a 1-minute base, so a destination reaches a **one-year**
  backoff after about eight consecutive failures. Mistakes in backoff handling
  are close to permanent here.
- The database is reached through **pgcat in transaction pooling mode** when the
  DSN comes from `homeserver.yaml`. See the pgx note below.

## Where things are

| | |
|---|---|
| Synapse source | `/home/daedric/synapse` — a real checkout |
| PostgreSQL socket | `/var/sockets`, database `synapse-db`, role `synapse` |
| Prometheus | `https://prometheus.aguiarvieira.pt` (credentials are not in this repo; ask) |
| Deployment | `/opt/matrix/synapse`, mounted read-only at `/etc/synapse` in the container |

**Read the Synapse checkout.** Nearly every rule in this worker cites a
`file:line` in Synapse, and those citations are the reason the reimplementation
is trustworthy. Verify them rather than trusting the comment — several have been
wrong, and one cost a feature (see "typing" below).

Two traps when reading it: check **which instance** actually runs a code path
before concluding what it does, and run greps from the repo you mean to search.
Both have produced confidently wrong answers here.

## Running the tests

```sh
go test -race ./...                     # everything that needs no database
```

Live tests skip without their environment variable. Against the real database:

```sh
SYNAPSE_DSN="host=/var/sockets user=synapse dbname=synapse-db" go test ./internal/store/
STATE_DSN="host=/var/sockets user=synapse dbname=synapse-db"   go test ./internal/state/
```

Others: `WRITE_DSN`, `HOMESERVER_YAML`, `SIGNING_KEY`, `SHARD_INSTANCES`,
`SHADOW_INSTANCE`, `WORKER_NAME`, `FEDERATION_TEST_DESTINATION`.

Live tests write rows keyed by a `test-` instance name. Clean up after:

```sql
DELETE FROM gofederation.destination_retry  WHERE instance_name LIKE 'test-%';
DELETE FROM gofederation.stream_positions   WHERE instance_name LIKE 'test-%';
DELETE FROM gofederation.destination_rooms  WHERE instance_name LIKE 'test-%';
```

**There is no PostgreSQL server in the dev environment, only client tools.** SQL
in `deploy/` cannot be executed there; run it against the real database or say
plainly that it is unverified.

## Invariants with tests behind them

Breaking one of these should fail a test, not a deployment.

- `TestEveryQuerySiteIsNamed` — every query in `internal/store` and
  `internal/state` must call `dbtrace.WithQueryName`. Unnamed queries are
  counted as `other`, which is where a hot query hides while every panel looks
  healthy. It happened.
- `TestHTTPSinkHasNoAllowlistOfItsOwn` — the send allowlist lives in
  `sink.Router` and nowhere else, so a bug in the HTTP sink cannot reach a
  destination the router did not choose.
- `TestShadowModeRefusesAWriteDSN` — a shadow must have no writable handle to
  Synapse's tables anywhere in the process.
- `destinations.Metadata.ProactivelySend` **defaults to true**. Almost every
  event has `{}` metadata, so defaulting it to false would drop essentially all
  federation traffic while every individual check still looked correct.

## Things that cost a deploy to learn

- **Typing does not arrive on the `federation` stream.** `build_and_send_edu` is
  only ever called on an instance that is already a federation sender
  (`handlers/typing.py:89`), so nothing reaches that stream in practice. A
  sender must consume the **`typing`** stream and build the EDUs itself. Typing
  was entirely absent for weeks before this was found.
- **A typing row is the room's whole typing set**, so start and stop exist only
  in the diff against the previous token. Order is therefore the meaning, and
  the diff must run synchronously on the replication goroutine.
- **Retry timings live in `gofederation.destination_retry`, not Synapse's
  `destinations`.** Synapse caches that table (`transactions.py:169`) and only
  its own writes invalidate the cache, so a backoff written or cleared from out
  here is served stale indefinitely. Publishing the invalidation on the `caches`
  stream would be worse: an occasional writer pins the stream's persisted-upto
  position in every Synapse process. See `docs/shadow-safety.md`.
- **pgx must use `QueryExecModeExec`.** `DescribeExec` needs two round trips and
  breaks behind a transaction pooler with "unnamed prepared statement does not
  exist" (SQLSTATE 26000). pgx documents this at `conn.go:633`.
- **Quantiles here are usually lies.** This worker routes on the order of one
  event a minute, so `histogram_quantile` over a short rate window interpolates
  inside a bucket from two or three samples. A stage whose p95 across its whole
  history was 24ms was reported as **4.20 s**. Always read a quantile next to
  its sample count.
- **Catch-up must take the same one-transaction-at-a-time claim as the live
  loop.** It runs on its own goroutine and originally called `send` directly, so
  it went out ALONGSIDE the transmission loop: PDUs on the wire out of order,
  and the remote answering 429 "still processing another transaction from this
  origin". Those 429s were then recorded as delivery failures, so this worker
  grew a live server's backoff because it was talking over itself --
  `nexy7574.co.uk` reached 231 minutes that way. `queue.ErrBusy` is not a
  failure and must never be reported as one.
- **Presence per destination is too sparse here to batch.** Merging and holding
  are both implemented and both inert on this homeserver: a `presence_federation`
  row is `(destination, user)`, so a destination sees roughly one state every
  few minutes and nothing ever accumulates. `docs/edu-batching.md` has the
  numbers.

## How to be useful here

- **Measure before building.** Two features were built and shipped inert because
  the one number that decided them — how often a single destination receives
  presence — was never checked. It was one query.
- **Assert on behaviour, not on helpers.** `FilterDue` had a passing test and
  was never called from anywhere; the path it was written for ran unfiltered for
  weeks while looking covered.
- **Prefer structural coverage to discipline.** Hooking the connection pool
  measures queries nobody remembered to instrument; an AST test keeps them
  named. Both exist because the by-hand version failed first.
- Most bugs found here have been **silent**: a feature absent, a resource
  growing, a filter never called. Almost none announced themselves in a log.
  When something looks fine, check whether it is running at all.

## Open

- `docs/edu-batching.md` — transaction-level EDU batching, designed and
  deliberately not finished. Presence merging and holding are done but dormant.
- The Python senders' Prometheus history from before the cutover is the only
  apples-to-apples baseline that exists, and retention is about two weeks.
  Turning presence off has already made that comparison largely meaningless,
  since presence was 99.6% of what those senders sent.
