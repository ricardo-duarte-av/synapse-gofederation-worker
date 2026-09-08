# Dashboard and scraping

`gofederation-worker.json` is a Grafana dashboard for this worker. It shows
this worker's own numbers only -- there are no panels comparing it against
Synapse's senders.

That was a deliberate removal rather than a simplification. The comparison was
built for a shadow running beside a Python sender on the SAME homeserver, and
it stops meaning anything anywhere else: a homeserver whose only sender is this
worker exports no sender metrics at all, so a comparison panel there can only
draw a line from a different homeserver. It did exactly that once -- a test
homeserver's events position of 42 beside production's 14,081,652, which reads
as a catastrophic lag when the two numbers simply count different servers'
events. `git log -- grafana/` has the panels if a side-by-side is ever wanted
back for a shadow deployment.

Import it through **Dashboards → New → Import → Upload JSON file**. It asks for
a Prometheus datasource on import; everything else is driven by the template
variables at the top.

## Prometheus

The worker serves `/metrics` on `metrics.addr` (`:9202` by default — 9099 is
Synapse main, 9101 the Python workers, 9200 gopro, 9201 gosync).

```yaml
scrape_configs:
  # This worker.
  - job_name: gofederation-worker
    scrape_interval: 15s
    static_configs:
      - targets:
          - av-gofederation-worker-1:9202
        labels:
          # Only needed if you run more than one; the dashboard's Worker
          # variable is populated from the instance label.
          deployment: aguiarvieira.pt
      # A worker acting as the TEST homeserver's own sender.
      - targets:
          - testing-gofederation-worker-1:9202
        labels:
          deployment: testing.aguiarvieira.pt

```

Container names work as targets when Prometheus shares a Docker network with
them, which is how the rest of this deployment is wired.

## Template variables

| Variable | What it selects |
|---|---|
| `Datasource` | which Prometheus |
| `Job` | the scrape job, if you run several |
| `Worker` | which worker instance, when more than one is running |


The dashboard is for a worker that is SENDING. Panels that only meant something
while shadowing -- the persisted shadow record, its age, its restart count --
are gone; the Mode tile stays, because "is this putting traffic on the
internet?" is the one question worth answering at a glance either way.

Stream positions are one homeserver's stream orderings and are never comparable
across deployments: a new test server sits in the tens while a server with years
of history sits in the millions.

## What each row answers

**Status** — is it running, and is it sending? `gofed_shadow_mode` is the single
most important number on the dashboard: 1 means every transaction is built,
signed and then logged, 0 means it is on the wire.

**Throughput** — how much it is doing. The gap between "examined" and "routed"
is expected to be large: most events on a homeserver are other servers' and are
correctly skipped, and `gofed_events_skipped_total` breaks down why.

**Latency** — where the time goes, and the panel that says where to optimise.
The lag histogram is seconds from an event being persisted to routing
finishing, which is what a user waits for.

One thing worth knowing before reading it: the lag panels are only meaningful
while **following** the stream. During a replay — a restart with a backlog, or a
cursor rewound by hand — every event is hours old by construction and the
histogram says nothing about speed. `gofed_stage_duration_seconds` is
unaffected, so read that instead.

The stage panel has already earned its place: on first measurement the
goroutine fan-out this design bets on cost roughly **5 µs** per event while
destination resolution cost roughly **6 ms** — three orders of magnitude more.
The bottleneck is the database, not the concurrency model, which is worth
knowing before optimising the wrong half.

**Queues and delivery** — whether delivery keeps up with routing. The queue
depths are sampled rather than tracked by counter, deliberately: a leak shows
up even when the counters that should have caught it are themselves the buggy
part. `Events stream position` is this worker's own progress through the
events stream, not a comparison.

**Routing accuracy** — `gofed_approximate_routes_total` counts routing decisions
that fell back to current room state rather than resolving the state before the
event. That is the one known difference from Synapse's algorithm rather than a
bug we hope is absent, so it is worth a panel of its own.

**Go runtime** — what the concurrency model costs. These come from the standard
Go and process collectors, which `promhttp.Handler()` registers by default, so
there is nothing to enable.

The goroutine panel is the design bet in numbers. One goroutine per destination
with work is the whole premise, so goroutines should track "destinations with
queued work" plus a small constant, and a count that climbs while queued work
does not is a leak. It can legitimately sit ABOVE
`queue.max_concurrent_destinations`: `Wake` spawns before taking a concurrency
slot, so the excess is goroutines parked waiting for a slot rather than sends in
flight, which is why all three lines share the panel.

Two readings that look like problems and are not:

- **Resident memory lagging a drop in real usage.** The gap between heap in-use
  and heap allocated is memory Go has freed but not yet returned to the OS.
  After a large queue is dropped, resident stays high for minutes.
- **File descriptors in the thousands.** One per open connection, so a sender
  talking to thousands of servers is expected to hold thousands. The limit is
  plotted beside it because exhausting it does not look like a resource problem
  from the outside — it looks like every destination failing at once, which
  reads as a network outage.

**Database** — every query this worker makes, measured at the connection POOL
rather than at chosen call sites. That distinction is the point: the query
nobody thought to instrument is the one that turns out to be slow, so pgx's
QueryTracer hooks the pool and a query with no name is counted as `other`
rather than dropped. An uninstrumented query appears as a rising `other` line.

`joined_hosts_at_state_group` is the one to watch. It walks the state group
edges and was measured directly against this database at 10.8ms, 108ms, 375ms
and **6.3 seconds** for the deepest chain, which is why it has a cache in front
of it.

The pool panels answer a question a duration cannot: "slow query" and "waited
for a free connection" look identical end to end and are fixed differently.
Acquired connections pinned at `max` with a rising wait time is the second one,
and means raising `database.max_conns` will do more than optimising any query.

**Bookkeeping** — the writes to Synapse's tables that a primary sender's
position IS. A non-zero failure rate means delivery is working while the bookkeeping is not —
rows re-read forever, the outbox growing without bound, `prev_id` chaining
silently lagging. Nothing else reports it.

## A note on empty panels

Several series only appear once something has happened, because Prometheus does
not export a labelled metric until its first observation:

| Series | Appears when |
|---|---|
| `gofed_send_duration_seconds` | the worker sends for real; never in shadow mode |
| `gofed_write_operations_total` | `mode: primary` |
| `gofed_approximate_routes_total` | a routing decision falls back to current state |
| `gofed_stage_duration_seconds` | the first event is routed |

An empty panel here is usually the right answer rather than a broken query.
