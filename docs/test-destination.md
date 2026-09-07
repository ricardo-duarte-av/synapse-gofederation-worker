# The test destination

A real homeserver we control, joined to a room with `aguiarvieira.pt`, with a
recording proxy in front of it. It gives us the one thing no amount of reading
Synapse's source can: **the exact bytes Synapse actually put on the wire**.

## Why this is worth building

Three things are currently unverifiable without it.

- **EDU content.** `destination_rooms` says which servers got something and
  when; it says nothing about what an `m.direct_to_device` or
  `m.device_list_update` body contained. That is also where our biggest known
  gap is — the device-list body is deliberately incomplete.
- **The request itself.** We have checked our signing against `signedjson` on
  fixtures. We have never checked a real request: the `X-Matrix` header over
  the real body, the URI, discovery, TLS.
- **Acceptance.** Nothing so far proves a remote would take what we produce.

## Use a real Synapse, not a stub

A stub receiver accepts anything, so it confirms nothing on the day we send for
real. A real Synapse validates our signature and PDU format and rejects us
loudly when we are wrong — which is the entire point of a canary. It also
removes the risk of debugging our own fake server's `make_join` instead of the
worker.

## Choose the name carefully

The destination has to land in the shard we shadow, or the worker produces
nothing for it and the exercise silently yields an empty file. The shard is
`sha256(name)` little-endian mod the number of senders:

```sh
docker exec av-federation-sender-worker-1 python3 -c '
from hashlib import sha256
inst = ["av-federation-sender-worker-1", "av-federation-sender-worker-2"]
for n in ["testing.aguiarvieira.pt", "test.aguiarvieira.pt"]:
    h = sha256(n.encode("utf8")).digest()
    print(n, inst[int.from_bytes(h, byteorder="little") % len(inst)])'
```

On this deployment `testing.aguiarvieira.pt` lands on **worker-1** and
`test.aguiarvieira.pt` on worker-2. The name is not cosmetic.

## Keep it to a private room

Joining a *public* room makes every server in it federate to the test
destination: other people's servers accumulating backoff state for our test rig,
and a capture polluted with traffic that is not ours.

It also buys nothing. `aguiarvieira.pt` only sends events its own users
originated (the `is_mine` check in `federation/sender/__init__.py:556`), so a
busy shared room adds no signal for this comparison.

Start with a private room shared only with `aguiarvieira.pt`. Then every
captured payload is ours by construction. If realistic fan-out and forked-DAG
state are wanted later, join one further room deliberately, as a second phase.

## Shape

```
sender  ──TLS──>  nginx  ──HTTP──>  fedrecorder  ──HTTP──>  test Synapse
                                         │
                                         └──> captures/synapse.jsonl
```

`fedrecorder` is transparent: it records `PUT /_matrix/federation/v1/send/…` and
forwards everything else untouched. It forwards the exact bytes it received —
re-encoding would invalidate the signature computed over them and the homeserver
would reject a transaction that was in fact correct — and a recording failure
never fails a request, because a test homeserver broken by its own
instrumentation is worse than no instrumentation.

```sh
fedrecorder \
  -listen :8449 \
  -upstream http://testing-synapse:8008 \
  -out /data/captures/synapse.jsonl \
  -origin aguiarvieira.pt
```

`-origin` filters on the origin in the **body**, which is what was signed,
rather than the one in the `Authorization` header, which is not.

## Worker side

```yaml
shadow:
  capture_destinations: [testing.aguiarvieira.pt]
  capture_file: /data/captures/worker.jsonl
```

Meant for one low-volume destination we control. Pointing it at a real busy
server writes every transaction to that server to disk — a lot of disk, and a
lot of other people's message content.

## Comparing

```sh
fedcompare \
  -synapse captures/synapse.jsonl \
  -worker  captures/worker.jsonl \
  -destination testing.aguiarvieira.pt
```

Exit status is 0 when the PDUs agree and 1 when they do not. EDUs are reported
but excluded from the verdict while the device-list body is knowingly
incomplete: a red light that is permanently on stops being read.

Two things the report will say that are not faults:

- **Transaction counts differ.** Framing is not comparable — which PDUs share a
  transaction depends on what was queued when a loop ran. Verified on real data:
  114 PDUs sent by us as 73 transactions and reframed as 114 single-PDU ones
  compare as full agreement.
- **PDUs matched on a content hash.** This deployment's default room version is
  12, and room version 3 onward derives the event id from the content rather
  than carrying it, so almost no PDU here has an `event_id` field. Computing the
  real id needs the room version, which is not in the PDU. A changed event
  therefore shows as one MISSING and one EXTRA rather than as a byte difference;
  the report says so, and each line carries room, type, sender and timestamp so
  the event is still findable.

## Going live, one destination at a time

When the captures agree, this is also the first place to send for real. That
still needs a send allowlist in the worker — not yet built — so that
`shadow.enabled: false` means "send to this one destination" rather than "send
to all 13,929 at once".
