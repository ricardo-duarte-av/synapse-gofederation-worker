# How we confirm we match Synapse

Everything the worker does to itself — its tests, its metrics, its shadow record
— proves only that it is self-consistent. This document is about the separate
question: **is what we would send the same as what Synapse actually sent?**

## What is not comparable, and why chasing it is a mistake

Transaction framing is not comparable between two senders. Which PDUs share a
transaction depends on what happened to be queued at the instant a sender's loop
ran, so two *correct* implementations legitimately produce different groupings,
different transaction ids and different `origin_server_ts`. A byte-for-byte diff
of whole transactions would report constant failures that mean nothing, and
tuning our batching to match Synapse's would be fitting to noise.

So "identical to Synapse" is the wrong target for the transaction. The right
targets are three questions that *are* answerable, each with its own oracle.

## 1. Did we route the same events to the same servers?

The most important question, and the one most likely to have a wrong answer.

**Oracle: Synapse's own `destination_rooms` table.** `_send_pdu` upserts
`(destination, room_id) -> stream_ordering` for the full sharded destination set
*before* the retry filter (`federation/sender/__init__.py:828`). It is Synapse's
own durable record of every routing decision it made — written by Synapse, not
reconstructed by us, and independent of whether delivery succeeded.

We keep our decisions in the identical shape
(`gofederation.destination_rooms`), written at the same point in the pipeline,
so the comparison is a join. `internal/compare` does it; `TestLiveRoutingParity`
runs it.

Three things make this comparison honest, and all three were learned by getting
them wrong first:

- **A floor.** Synapse's table is a high-water mark accumulated over the
  homeserver's entire lifetime — here it spans stream orderings 3,605 to the
  present across 40,562 rows — while ours begins the moment this worker first
  ran. Without a floor, every pair Synapse routed before we existed reads as
  "missing". On the first run that was 19,314 of 19,822 pairs.
- **An unsettled class.** These are high-water marks, not logs. A pair whose
  mark has moved past the horizon cannot be compared for equality: the two marks
  describe different instants and the row holds no way to recover the value *at*
  the horizon. The first version compared them anyway and reported a pair as
  "extra" that Synapse had in fact routed, 361 orderings later than our copy.
- **A settle window.** Both senders work the same stream at their own pace, so
  near the tip one is always slightly ahead. Comparing there reports clock skew
  as disagreement.

Disagreements are classified rather than counted together, because they mean
very different things. `missing` is the dangerous direction — in production, an
event a server never receives. `extra` is a server receiving something it should
not have: wrong, but less damaging.

**Result so far:** 507 settled pairs, 507 agreed, **100%**, over a window of
21,815 stream orderings.

### Two ways to get a meaningless answer

Both were hit for real before being fixed, and both produced confident output.

**Comparing periods only one side observed.** The recorder's file is cumulative
from the day it was deployed; the worker's covers only when the worker ran.
Diffing the whole of each reports everything outside that period as MISSING.
`fedcompare` therefore windows to the intersection by default.

**Windowing on the wrong clock.** A worker replaying history records
yesterday's events with today's timestamp, so the processing-time overlap is
*empty* even though both captures describe the same events — and an empty
comparison printed "PDUs agree". `-by-event-time` windows on the events'
own `origin_server_ts` instead, and an empty comparison is now reported as
**INCONCLUSIVE** with exit status 2, distinct from agreement. A green light
over zero records is worse than no comparison, because it gets believed.

## 2. Are our PDU bytes identical to Synapse's?

Unlike transaction framing, an individual PDU's bytes are a pure function of the
stored event with no timing in them at all, so this can be exact.

**Oracle: Synapse's own `canonicaljson`,** run inside a live federation sender
container over real events, applying exactly what `serialize_and_filter_pdus`
does. `internal/txn.TestLivePDUBytesMatchSynapse` compares our output against
that fixture; the test's own comment carries the command to regenerate it.

**Result:** 200 real production events, **byte-identical**.

Getting here required implementing `SerialisePDU` faithfully, including the part
that looks like a bug and is not: `transaction_manager.py`'s `json_data_cb`
rewrites `age_ts` to `age`, but it looks for `age_ts` at the PDU's top level
while Synapse stores it inside `unsigned`, so the condition is never true. Its
own FIXME says so, and the data agrees — of 64,297 recent events, 3,468 carry
`unsigned.age_ts` and not one carries a top-level `age_ts`. Implementing that
rewrite would have made our bytes differ from Synapse's for 5% of events.

## 3. Would a remote server accept our transaction?

Note the phrasing. For the transaction as a whole the useful question is not
"identical to Synapse's" — it cannot be — but "valid". That is verifiable
absolutely rather than comparatively.

**Oracle: Synapse's own `signedjson`.** `internal/txn.TestSignatureMatchesSynapse`
signs the same auth objects with the same key in both implementations and
requires identical output, including the case Go gets wrong by default — its
`encoding/json` HTML-escapes `<`, `>` and `&` where canonical JSON does not.

**Result:** byte-identical bodies and signatures.

## The sharding precondition

Every comparison above assumes both senders are deciding about the same set of
destinations. If the partitions differ at all, none of it means anything — and a
wrong shard function still produces a uniform, stable, entirely plausible split,
so nothing else would reveal it.

`internal/sharding.TestLiveShardParity` checks our partition against Synapse's
own `ShardedWorkerHandlingConfig` over every destination in the database.

**Result:** all **27,824** destinations, identical partition.

## What is still unverified

Stated plainly, because an unverified thing that nobody has written down is
indistinguishable from a verified one:

- **EDU content, and it cannot be fixed by shadowing.** This is a structural
  limit rather than unfinished work, found by comparing a real encrypted
  message.

  A real sender does not merely read `device_federation_outbox` and
  `device_lists_outbound_pokes` — it **deletes** the rows once the transaction
  succeeds. Those deletions are its cursor. So by the time a shadow looks, the
  row that produced Synapse's `m.direct_to_device` is gone: the capture shows
  `synapse=1 worker=0` not because we decided differently but because the
  evidence was destroyed by the act we are shadowing.

  Confirmed on a live E2EE message: Synapse sent one `m.direct_to_device` to
  the test destination, and both `device_federation_outbox` and
  `device_lists_outbound_pokes` held **zero** rows for it afterwards.

  Nothing in the shadow can close this. What can:

  - **Send for real to that destination.** Once this worker owns the delivery,
    it reads the row and the content is its own; correctness is then judged by
    whether the receiving server decrypts, not by a diff.
  - **A destination Synapse does not handle**, if one could be arranged — then
    no other sender competes for the rows.

  Until then the honest statement is that device EDU *routing and timing* are
  verified (question 1 covers them) and device EDU *content* is not.

- **`m.device_list_update` body.** Separately from the above, ours is
  deliberately incomplete: no `prev_id`, `deleted`, `keys` or
  `device_display_name`.
- **Catch-up.** Not implemented, so not compared.
- **Forked-DAG destination resolution.** 4.4% of routing decisions still fall
  back to current room state. Those pairs can still be compared by question 1 —
  and they are, which is how we would find out that the fallback is producing
  wrong destinations rather than merely approximate ones.
- **Delivery itself.** Nothing here proves a remote would accept the
  transaction in practice, only that it is well-formed and correctly signed.
  That is what the go-live canary is for: point the first real send at a
  homeserver we control and compare what it receives.

## Running the checks

```sh
# 1. Routing parity against Synapse's own record
SYNAPSE_DSN="host=/var/sockets user=gofed_ro dbname=synapse-db" \
STATE_DSN="host=/var/sockets user=gofed_state dbname=synapse-db" \
SHARD_INSTANCES=av-federation-sender-worker-1,av-federation-sender-worker-2 \
SHADOW_INSTANCE=av-federation-sender-worker-1 \
WORKER_NAME=av-gofederation-worker-1 \
  go test ./internal/compare/ -run TestLiveRoutingParity -v

# 2. PDU bytes (see the test comment for generating the fixture)
PDU_RAW=raw-events.jsonl PDU_EXPECTED=syn-pdus.jsonl \
  go test ./internal/txn/ -run TestLivePDUBytes -v

# 3. Signatures — no fixture needed, the vectors are checked in
go test ./internal/txn/ -run TestSignatureMatchesSynapse -v

# 0. The precondition (see the test comment for generating the fixture)
SHARD_PARITY_FILE=py-shards.tsv \
SHARD_INSTANCES=av-federation-sender-worker-1,av-federation-sender-worker-2 \
  go test ./internal/sharding/ -run TestLiveShardParity -v
```
