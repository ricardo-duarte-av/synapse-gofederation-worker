# Batching EDUs per destination

Not implemented. This is the design and the evidence for it, written down during
the soak so the numbers that motivated it do not have to be rediscovered.

## The observation

Every transaction this worker sends carries exactly one unit. Measured over an
hour of steady state on `aguiarvieira.pt`:

| | |
|---|---|
| transactions/s | 3.358 |
| EDUs/s | 3.250 |
| PDUs/s | 0.121 |
| **EDUs per transaction** | **0.968** |
| EDU-only transactions | 3.237/s — **96% of all transactions** |
| mean bytes per transaction | 679 |

The limits are not the cause: `queue.max_edus_per_transaction` is 100 and
`takeLocked` takes everything pending. The cause is *when* the queue is woken.
`Wake` is called on every enqueue, the transmission loop starts immediately,
drains the single unit that is there and sends it. By the time the next EDU
arrives the queue is empty again, so nothing ever accumulates to batch.

Batching happens today only by accident — when a unit is enqueued while a send
to that destination is already in flight, since one transaction at a time per
destination is enforced.

## Why it matters, and to whom

Not to us. It costs this worker very little to send four transactions instead of
one. It costs the **receiving** server a TLS write, an HTTP request, a canonical
JSON encoding, an **ed25519 signature verification**, and a dedup lookup on
`(origin, transaction_id)` — almost all of it fixed cost, the same whether the
transaction carries one EDU or fifty.

At 679 bytes for a transaction carrying one EDU, most of what we are sending is
envelope and signature. We are, at roughly 3.2 EDU-only transactions per second
against 10,000 destinations, imposing more request load on the rest of the
federation than a stock Synapse would for the same information.

That is the argument: this is a change made for other people's servers.

## What Synapse does, and how this differs

Synapse does not wake destinations directly for presence, typing, or
non-immediate device messages. They go through `_DestinationWakeupQueue`
(`federation/sender/__init__.py:317`), whose docstring states a different
purpose:

> Staggers waking up of per destination queues to ensure that we don't attempt
> to start TLS connections with many hosts all at once, leading to pinned CPU.

The delay is `min(max_delay_s, 30 / len(queue))`, where `max_delay_s` is
`1 / federation_rr_transactions_per_room_per_second` — **20ms** by default. The
whole queue drains within 30 seconds, so with 5,000 destinations queued each
waits about 6ms.

So Synapse's mechanism smooths connection churn and batches only incidentally.
What is proposed here is more aggressive by three orders of magnitude, and is
therefore a deliberate divergence rather than closing a gap. It should be
configurable, and its default should be defensible on its own terms.

## The proposal

Hold EDU-only transactions per destination until either:

- pending EDUs reach a threshold (default around 50), or
- time since that destination's last transmission exceeds a deadline
  (default around 30s).

Both configurable, because the right trade differs by deployment.

## What the design has to get right

**A pending PDU flushes immediately.** An event is what a user is waiting for; a
receipt is not. PDUs must never be held, and any EDUs queued at that moment
should ride along in the same transaction — which is free, and is where a good
part of the batching would come from anyway.

**Typing cannot wait 30 seconds.** A remote server expires a typing indicator
after Synapse's `FEDERATION_TIMEOUT` of one minute, and this worker re-announces
every 40s (`FederationPingInterval`). A 30s hold on top of that means an
indicator can appear after the user has stopped typing, or lag by up to 70s.
Either typing keeps a much shorter deadline than receipts and presence, or it is
excluded from batching. Read receipts and presence tolerate 30s comfortably.

**Presence merging is DONE** (`EnqueuePresence`), and was the larger half. This
worker was emitting 1.00 presence states per `m.presence` EDU though one EDU
carries 50, so each user cost a transaction. Merging puts N states in one EDU;
batching, still to do, would put N EDUs in one transaction. The two compound.

The encoding is deferred to transaction-build time, which matters more once
transactions are also held: `last_active_ago` is a duration from now, so a state
rendered on arrival and sent 30 seconds later would claim the user was active 30
seconds more recently than they were.

**The deadline needs a timer, not a poll.** Holding EDUs means a destination with
queued work and no new arrivals must still wake when the deadline passes.
Today's loop only runs when something wakes it.

**It interacts with the long-outage drop.** EDUs held for a destination that then
enters a long backoff are dropped, which is correct, but the held window makes
the drop slightly more likely. Worth watching `gofed_edus_dropped_total` after.

## How to tell whether it worked

- `gofed_transactions_total` rate should fall sharply — the goal.
- `gofed_transaction_edus_total` rate must NOT fall. Same information, fewer
  envelopes; a drop here means EDUs are being lost, not batched.
- EDUs per transaction should rise from ~1 toward the threshold.
- `gofed_transaction_bytes_total` should fall somewhat, as envelope and
  signature overhead is amortised.
- Typing latency, if typing is included, is the thing most likely to regress in
  a way no counter shows.
