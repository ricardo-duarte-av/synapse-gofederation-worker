# Dashboard and scraping

`gofederation-worker.json` is a Grafana dashboard for this worker, with panels
that put it side by side with Synapse's own federation senders where the two
export comparable numbers.

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

  # Synapse's own senders, for the comparison panels. Without this job the
  # dashboard still works, but every "vs synapse" series is empty.
  - job_name: synapse
    metrics_path: /_synapse/metrics
    scrape_interval: 15s
    static_configs:
      - targets:
          - av-federation-sender-worker-1:9101
          - av-federation-sender-worker-2:9101
          - synapse-matrix-synapse-1:9099
```

Container names work as targets when Prometheus shares a Docker network with
them, which is how the rest of this deployment is wired.

## Template variables

| Variable | What it selects |
|---|---|
| `Datasource` | which Prometheus |
| `Job` | the scrape job, if you run several |
| `Worker` | which worker instance, when more than one is running |
| `Synapse server_name` | the homeserver the selected worker serves |

`Synapse server_name` is used **only** by the five panels that compare with
Synapse. It is populated from the selected worker's own `deployment` label, not
from the homeservers Synapse exports, and that distinction has already produced
one wrong reading worth describing.

Those are not the same set. A homeserver whose only federation sender is this
worker exports no `synapse_..._federation_sender` metrics at all, so it never
appears in Synapse's list -- and picking from that list can then only ever name
a **different** homeserver. The comparison panels would plot two unrelated
servers against each other, with nothing on the chart to say so: on this
deployment, a test homeserver's events position of 42 next to production's
14,081,652, read as a catastrophic lag when the two numbers simply count
different servers' events.

So an **empty** Synapse series is the correct output on a homeserver this worker
is the only sender for. That is primary mode working, not a scrape failure.
Stream positions in particular are one homeserver's stream orderings and are
never comparable across deployments: a new test server sits in the tens while a
server with years of history sits in the millions.

## What each row answers

**Status** — is it running, and is it sending? `gofed_shadow_mode` is the single
most important number on the dashboard: 1 means every transaction is built,
signed and then logged, 0 means it is on the wire.

**Throughput** — how much it is doing, against Synapse. Two honest caveats are
written into the panel descriptions: while shadowing this worker covers ONE
shard and Synapse's line covers every sender, so compare shapes rather than
heights; and the gap between "examined" and "routed" is expected to be large,
because most events on a homeserver are other servers' and are correctly
skipped.

**Latency** — the headline comparison, and the panel that says where to
optimise. Both lag histograms are in seconds and measure the same thing: from
the event being persisted to routing finishing.

Two things worth knowing before reading it:

- Synapse also exports `synapse_event_processing_lag`, a **millisecond gauge**.
  It is not the same measurement. The dashboard uses
  `synapse_event_processing_lag_by_event`, which is a seconds histogram and is
  the real counterpart.
- The lag panels are only meaningful while **following** the stream. During a
  replay — a restart with a backlog, or a cursor rewound by hand — every event
  is hours old by construction and the histogram says nothing about speed.
  `gofed_stage_duration_seconds` is unaffected, so read that instead.

The stage panel has already earned its place: on first measurement the
goroutine fan-out this design bets on cost roughly **5 µs** per event while
destination resolution cost roughly **6 ms** — three orders of magnitude more.
The bottleneck is the database, not the concurrency model, which is worth
knowing before optimising the wrong half.

**Queues and delivery** — whether delivery keeps up with routing. The queue
depths are sampled rather than tracked by counter, deliberately: a leak shows
up even when the counters that should have caught it are themselves the buggy
part.

**Shadow record** — what the promotion decision is read from. These counters are
restored from disk at startup and are cumulative across deploys, because "has
this agreed with the real sender for a month?" cannot be answered from numbers
that reset. `gofed_approximate_routes_total` is the one to watch: it counts
routing decisions that fell back to current room state rather than resolving
the state before the event, which is the one known difference from Synapse's
algorithm rather than a bug we hope is absent.

**Primary mode** — bookkeeping written to Synapse's tables. Always zero while
shadowing, and being able to see that is the point: a shadow that started
writing would be changing the real senders' behaviour. In primary mode a
non-zero failure rate means delivery is working while the bookkeeping is not —
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
