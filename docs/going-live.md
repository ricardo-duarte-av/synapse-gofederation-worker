# Going live

`shadow.enabled: false` is the switch that makes this worker put real
federation traffic on the internet. This is how it is arranged so that flipping
it is a small, reversible step rather than a leap.

## What sending alongside Synapse actually means

The allowlist does **not** stop Synapse sending. Destinations are divided
between the senders in `federation_sender_instances` by
`sha256(destination) mod len(instances)`, and nothing this worker does changes
that: `testing.aguiarvieira.pt` hashes to `av-federation-sender-worker-1`, so
worker-1 keeps delivering to it whatever we do. Turning on our own sending to
that destination means it is served by **two** senders at once.

Most of that duplication is harmless, and it is worth knowing exactly which
parts, because one of them is not.

| | Deduplicated by | Effect of a duplicate |
|---|---|---|
| PDUs | `event_id`, on the receiving server | wasted work, nothing else |
| `m.direct_to_device` | `(origin, message_id)` — `deviceinbox.py:883` | none; the second insert returns early |
| `m.device_list_update` | stream ids in the body | at worst a redundant device resync |
| **the transaction itself** | **`(origin, transaction_id)`** | **the second transaction is DISCARDED** |

That last row is the hazard. `federation_server.py:400` looks up
`(origin, transaction_id)` and, on a repeat, returns the **cached response**
without reading the body. Both senders share an origin, both seed their counter
from milliseconds-since-epoch and increment by one, so their id ranges are two
intervals that drift into each other. A collision does not produce an error --
it silently drops every event in the losing transaction.

So this worker namespaces its transaction ids: `gofed-1788792203712` rather
than `1788792203712`. `transaction_id` is an opaque string in the spec and
Synapse's route accepts anything without a slash
(`transport/server/federation.py:80`), so the prefix removes the possibility
rather than reducing it -- and it makes the receiving server's logs say which
sender produced a transaction.

It is on by default, because the dangerous configuration is the one this worker
starts life in. `queue.transaction_id_prefix: ""` turns it off, and is only
safe once no other sender delivers to any destination this worker handles.

### There is no way to tell Synapse to skip one destination

Worth stating plainly, because it is the obvious thing to reach for and it does
not exist.

- **`federation_domain_whitelist`** is a whitelist, not a denylist
  (`config/federation.py:97`). Setting it restricts the homeserver to ONLY the
  listed domains; there is no way to subtract one.
- **No denylist exists.** The whitelist is the only destination filter the
  sender applies.
- **The module callback is an observer, not a decision hook.**
  `notify_on_event_delivered_over_federation` fires at
  `transaction_manager.py:226`, *after* a successful send. Modules are told what
  was delivered; they cannot stop a delivery.
- **Patching Synapse** would work -- the deployment already has a patch
  mechanism in `patches/patches.sh`, currently all commented out -- but
  carrying a local patch on a production homeserver to accommodate a test is a
  worse trade than accepting deduplicated duplicates on a server we own.

So while this worker is not a member of `federation_sender_instances`, any
destination it sends to is served by two senders. That is not a configuration
mistake to be fixed; it is the shape of the problem.

### Why duplicate delivery is worth accepting for the test destination

Device EDU **content** cannot be verified while shadowing at all: a real sender
DELETES the `device_federation_outbox` row once its transaction succeeds, so
the evidence is destroyed by the act being shadowed
(see [verification.md](verification.md)). Sending for real to one destination we
control is the only way to exercise that path -- we read the row ourselves, and
the receiving server tells us whether the key share was right by decrypting or
failing to.

The cost is a test homeserver receiving some events twice, deduplicated on
arrival. That is a reasonable trade for a server whose purpose is to be
experimented on. It would not be reasonable for a stranger's.

## One destination at a time

The allowlist is the mechanism:

```yaml
shadow:
  enabled: false
  send_only_to: [testing.aguiarvieira.pt]
```

Everything not on the list stays dry-run, exactly as in shadow mode. So the
first live configuration sends to one homeserver we control and to nothing
else, and widening it is a deliberate edit rather than a side effect.

## Why the allowlist is not a flag inside the sender

`internal/sink.HTTP` has no allowlist and no idea which destinations it may
talk to. `Router` holds the list and simply never hands it an unapproved one;
everything else goes to a sink with no network dependency at all.

The alternative — a boolean inside the HTTP sender saying whether to really
send — puts the decision next to the code that opens sockets, where a wrong
answer means real traffic to real servers. Here a bug in the sender cannot
reach a destination the router did not choose, because the sender is never
called for it. `TestHTTPSinkHasNoAllowlistOfItsOwn` reads the AST and fails if
anything resembling an allowlist appears in that file, so there is exactly one
place the question is answered. It has already caught one leftover.

## The default is deny

- An empty `send_only_to` sends to **nobody**. It is never read as "everyone".
- Sending to everything needs `send_to_all: true`, a separately named option.
  Nobody should reach production federation traffic by deleting a line.
- `shadow.enabled: false` with neither set is **refused at startup**. Leaving
  shadow mode is the one change here with consequences outside the process, so
  an ambiguous configuration is not resolved, it is rejected.
- Setting both is refused too: they contradict each other, so neither is
  assumed.

## Startup says which it is

Whatever the configuration, one line answers "is this putting traffic on the
internet?":

```
INF starting ... shadow=false sink="REALLY SENDING to testing.aguiarvieira.pt; every other destination is dry-run"
WRN LIVE: sending real federation traffic to the allowlisted destinations only; every other destination is still dry-run
```

and in shadow mode:

```
INF starting ... shadow=true sink="dry-run (shadow; nothing is sent)"
INF SHADOW MODE: transactions will be built and signed but never sent, and no Synapse table will be written
```

## What the sender does

Server discovery — `.well-known`, then `_matrix-fed._tcp` SRV, then the legacy
`_matrix._tcp` SRV, then port 8448, with the right `Host` and TLS name at each
step — is mautrix's `ServerResolvingTransport`, not ours. It is fiddly,
security-relevant, and has nothing to do with what this worker is for.

Redirects are refused: the signature names the destination, so following one
would send a signed transaction somewhere it was not addressed to.

Retries mirror Synapse's `/send` behaviour and read its tuning from
`homeserver.yaml` rather than assuming the defaults, which this deployment
overrides substantially (`client_timeout: 10s`, `max_long_retries: 2`,
`max_long_retry_delay: 10s` against Synapse's 60s/10/60s). The delay is
`4^attempt` capped, with 0.8–1.4 jitter — the jitter is not decoration, since
without it every queue that failed together retries together and a recovering
server is met by the whole backlog at once. A 5xx or 429 is retried; anything
else is not, because the remote has made a decision it will make again.

## Verified

A real, signed, **empty** transaction was sent to `testing.aguiarvieira.pt` and
accepted:

```
ACCEPTED by testing.aguiarvieira.pt: status 200, 0 per-PDU errors
```

and from the receiving server's own log:

```
Received txn 1788788300774 from aguiarvieira.pt. (PDUs: 0, EDUs: 0)
200 "PUT /_matrix/federation/v1/send/1788788300774" "synapse-gofederation-worker"
```

It authenticated us as `aguiarvieira.pt`, which is the thing no fixture can
establish: a **real Synapse verified our signature against the published key**.
Comparing against `signedjson` proves our arithmetic; this proves the whole
path — discovery, TLS, the `X-Matrix` header and the bytes underneath it.

The transaction is empty on purpose. It carries no PDUs and no EDUs, so it
changes nothing on the receiving server and cannot reach the wrong room or the
wrong people; the only thing under test is whether the envelope is accepted.
`TestLiveSendEmptyTransaction` reruns it on demand — only ever against a server
you control.

## Order of operations

1. **Shadow with capture** against the test destination, and compare with
   `fedcompare` until the PDUs agree. See [test-destination.md](test-destination.md).
2. **Live to the test destination only**, with `send_only_to`. Compare again —
   now the capture is of traffic that really went somewhere.
3. **Widen deliberately**, a few destinations at a time, watching
   `gofed_transactions_total` and the receiving servers' behaviour.
4. **`send_to_all`**, and only then add the worker to
   `federation_sender_instances` — which reshards all three senders and is a
   Synapse restart, planned separately.

## Not yet done

- **Our own backoff state.** The retry filter reads Synapse's `destinations`
  table, which is right while shadowing but is another sender's bookkeeping.
  With one test destination the in-memory per-request retry is enough; before
  step 3 this worker needs its own persistent per-destination backoff, in
  `internal/state` where the rest of its bookkeeping lives.
- **Catch-up.** A destination that was down while we were sending gets nothing
  replayed. Fine for a test destination that is up; not fine at step 3.
- **EDU bodies.** `m.device_list_update` is still missing `prev_id`, `deleted`,
  `keys` and `device_display_name`.
