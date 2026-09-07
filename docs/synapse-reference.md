# Synapse federation sender — reference

What this worker reimplements, with the file:line in Synapse that each rule
comes from. Read against `/home/daedric/synapse` @ `3db77e80a5` (develop,
2026-08-24). Every non-obvious constant here is load-bearing: the shadow diff is
only meaningful if we reproduce these decisions exactly, so a divergence found
later should be checked against this document before it is assumed to be a bug
in our code.

## 1. Sharding

`synapse/config/_base.py:1054` `ShardedWorkerHandlingConfig`:

```python
dest_hash = sha256(key.encode("utf8")).digest()
dest_int  = int.from_bytes(dest_hash, byteorder="little")
return self.instances[dest_int % len(self.instances)]
```

- `instances` is `federation_sender_instances` **in config order**, not sorted.
- One instance short-circuits to index 0. An empty list means "someone else
  handles it" — `should_handle` returns false rather than raising.
- **Little-endian.** Get this wrong and every destination lands on the wrong
  shard, which is the one bug that would invalidate the whole shadow exercise
  while still looking plausible.

`instance_name` is `worker_name` or `"master"` (`config/workers.py:272`); the
yaml `instance_map` key `main` is rewritten to `master` (`workers.py:73,338`).

## 2. PDU pickup

`synapse/federation/sender/__init__.py:525` `_process_event_queue_loop`:

1. `last_token = get_federation_out_pos("events")`
2. `get_all_new_event_ids_stream(last_token, _last_poked_id, limit=100)` —
   `storage/databases/main/stream.py:2107`:
   ```sql
   SELECT e.stream_ordering, e.event_id, e.received_ts FROM events AS e
   WHERE ? < e.stream_ordering AND e.stream_ordering <= ?
   ORDER BY e.stream_ordering ASC LIMIT ?
   ```
   `next_token` is the upper bound unless the limit was hit, in which case it is
   the last row's `stream_ordering`.
3. Group by `room_id`; rooms concurrently, events within a room **sequentially**.
4. `update_federation_out_pos("events", next_token)`.

Per-event filter (`__init__.py:554`), skip unless all hold:

- `is_mine_id(event.sender)` **or** `send_on_behalf_of` is set
- not `internal_metadata.is_out_of_band_membership()`
- `internal_metadata.should_proactively_send()`

These three live in `event_json.internal_metadata`, not in the event body.

Destination resolution, in priority order (`__init__.py:608-670`):

1. no `prev_events` → empty set
2. `get_partial_state_servers_at_join(room_id)` if non-None
3. external Redis cache: `event_to_prev_state_group` → `get_joined_hosts`
4. `get_hosts_in_room_at_events(room_id, event.prev_event_ids())` — state
   **before** the event, so the last member on a server still receives their own
   ban.

Then the rescinded-invite case (`__init__.py:674`): a `m.room.member` `leave`
where `sender != state_key`, whose auth events contain an `invite` for that
`state_key`, adds `domain_of(state_key)` back to the destinations.

Then the shard filter, then `send_on_behalf_of` is discarded from the set.

`_send_pdu` (`__init__.py:794`) drops our own server, applies
`federation_domain_whitelist`, **writes `destination_rooms` before the retry
filter** (`transactions.py:305`) — order matters, or catch-up silently loses
events for a server that is currently down — and only then filters by retry
timings and enqueues.

## 3. Per-destination queue

`synapse/federation/sender/per_destination_queue.py`.

| Constant | Value | Where |
|---|---|---|
| PDUs per transaction | **50** | `per_destination_queue.py:838` (hardcoded slice) |
| EDUs per transaction | **100** | `api/constants.py:56` |
| reserved EDUs | 10 | `api/constants.py:59` |
| presence states per EDU | 50 | `per_destination_queue.py:79` |
| receipt EDUs per txn | 5 | `per_destination_queue.py:652` |
| catch-up retry interval | 1 h | `per_destination_queue.py:75` |

Transaction fill order (`_TransactionQueueManager.__aenter__`, line 740) — this
ordering is load-bearing because each step consumes the remaining EDU budget:

1. presence (≤50 states → one `m.presence` EDU)
2. receipts (≤5 EDUs)
3. `edu_limit = 100 - len(so far)`
4. to-device, budget `edu_limit - 10`
5. device list updates, budget `edu_limit`
6. other EDUs: `_pending_edus` then `_pending_edus_keyed`
7. PDUs: `_pending_pdus[:50]`

Nothing is dequeued or marked sent unless the send returned without raising —
`__aexit__` early-returns on `exc_type is not None` (line 851). Exactly one
in-flight transaction per destination, ever; `transmission_loop_running` is the
mutex and `_new_data_to_send` is the re-check-before-exiting flag.

## 4. Transaction wire format

`federation/units.py:104`. The body carries neither `transaction_id` nor
`destination` — the txn id is only in the URL:

```json
{"origin": "...", "origin_server_ts": 1234, "pdus": [...], "edus": [...]}
```

`edus` is omitted when empty. `transaction_manager.py:75`: txn ids start at
`int(time_msec())` and increment, shared across all destinations.

PUT `/_matrix/federation/v1/send/<txnId>`, with `long_retries=True`,
`try_trailing_slash_on_400=True` and **`backoff_on_all_error_codes=True`** —
for `/send`, unlike most federation calls, *any* non-2xx backs the whole
destination off.

Response is `{"pdus": {"<event_id>": {} | {"error": "..."}}}`; per-PDU errors
are logged, never retried.

## 5. Signing

`config/key.py:127`: `signing_key_path` file is one key per line,
`ed25519 <version> <unpadded-b64 32-byte seed>`; Synapse signs with the
**first**. Key id is `ed25519:<version>`.

`matrixfederationclient.py:905` builds the object to sign:

```python
{"method": "PUT",
 "uri": "/_matrix/federation/v1/send/<txnid>",   # path + query only, no scheme/host
 "origin": server_name,
 "destination": destination,
 "content": <the JSON body>}
```

signed with `sign_json` (canonical JSON, `signatures`/`unsigned` stripped,
Ed25519, unpadded base64), then per signature:

```
Authorization: X-Matrix origin="...",key="ed25519:...",sig="...",destination="..."
```

The body sent on the wire is `encode_canonical_json(json)` — byte-identical to
what was signed.

## 6. Redis replication

Channel is literally `server_name` (`replication/tcp/redis.py:399`); there is no
prefix setting. A plain federation sender subscribes to just that one channel.

Message = `NAME + " " + to_line()`, one line, no newlines
(`redis.py:263`). Relevant commands (`replication/tcp/commands.py`):

```
RDATA <stream> <instance> <token|"batch"> <row_json>
POSITION <stream> <instance> <prev_token> <new_token>
REPLICATE
FEDERATION_ACK <instance> <token>
REMOTE_SERVER_UP <server>
```

`RDATA` with token `"batch"` buffers; the next numeric token flushes the buffer
plus itself as one batch (`commands.py:114`). RDATA for a stream is **ignored
until a POSITION for that stream has arrived on the same connection**
(`handler.py:585`). Self-echo is suppressed by comparing `instance_name`.

Streams a sender consumes (`replication/tcp/client.py:472`):

| Stream | Effect |
|---|---|
| `events` | poke only — `notify_new_events`; content is re-read from the DB |
| `federation` | EDUs relayed from master; drives `FEDERATION_ACK` |
| `receipts` | `send_read_receipt` |
| `to_device` | `send_device_messages(hosts, immediate=True)` |
| `device_lists` | `send_device_messages(hosts, immediate=False)` |

Row shapes are positional JSON arrays:

- `events`: `[type_id, data]` where type_id is `ev` / `state` / `state-all`
- `to_device`: `[entity]` — a user id (`@…`) or a remote server name
- `device_lists`: `[user_id, is_signature, hosts_calculated]`
- `receipts`: `[room_id, receipt_type, user_id, event_id, thread_id, data]`

## 7. Tables

| Table | Columns we care about |
|---|---|
| `destinations` | `destination`, `retry_last_ts`, `retry_interval`, `failure_ts`, `last_successful_stream_ordering` |
| `destination_rooms` | `(destination, room_id)` → `stream_ordering` of newest PDU needing send |
| `device_lists_outbound_pokes` | `destination`, `stream_id`, `user_id`, `device_id`, `sent`, `ts`, `opentracing_context` |
| `device_federation_outbox` | `destination`, `stream_id`, `queued_ts`, `messages_json` |
| `federation_stream_position` | `(type, instance_name)` → `stream_id`; type is `events` or `federation` |
| `events` / `event_json` | `stream_ordering`, `event_id`, `received_ts`; `json`, `internal_metadata` |

**`federation_stream_position` is a trap for us.** On startup Synapse rewrites
it to have exactly one row per (type, configured sender instance), using
`MIN(stream_id)` per type, and deletes rows for unknown instances
(`stream.py:2162`). A row written by this worker would either be deleted or drag
every real sender backwards. We never write it — see `docs/shadow-safety.md`.

## 8. Backoff

Persistent, per destination (`util/retryutils.py:226`):

```python
if retry_interval:
    retry_interval = int(retry_interval * multiplier * random.uniform(0.8, 1.4))
    retry_interval = min(retry_interval, max_interval)
else:
    retry_interval = min_interval
```

Success with a non-zero interval clears `failure_ts`, `retry_last_ts` and
`retry_interval` and emits `REMOTE_SERVER_UP`.

This deployment's tuning (`/opt/matrix/synapse/synapse/homeserver.yaml`) is
much tighter than Synapse's defaults: `destination_min_retry_interval: 1m`
(default 10m), `destination_retry_multiplier: 5` (default 2),
`destination_max_retry_interval: 365d` (default 7d), `client_timeout: 10s`,
`max_long_retries: 2`, `max_short_retries: 2`.

## 9. Catch-up

`_catching_up` starts **true** for every newly created queue
(`per_destination_queue.py:137`), so every restart enters catch-up per
destination. While catching up, `send_pdu` **drops** the PDU and only records
`_catchup_last_skipped` — the durable record is `destination_rooms`.

`get_catch_up_room_event_ids` (`transactions.py:408`) returns ≤50 event ids, one
per room:

```sql
SELECT event_id FROM destination_rooms JOIN events USING (stream_ordering)
WHERE destination = ? AND stream_ordering > ?
ORDER BY stream_ordering LIMIT 50
```

Each is sent as its own single-room transaction with no EDUs; for a PDU that is
no longer a forward extremity, the room's current extremities newer than the
cursor are sent instead. `last_successful_stream_ordering` then advances to the
**original** PDU's stream ordering, not the extremities actually sent — it is a
cursor into `destination_rooms`, not a delivery receipt.

A fresh destination with no `last_successful_stream_ordering` row exits catch-up
immediately and never replays history.

Catch-up is gated on the backoff in four places, and all four are needed:

1. The outstanding-destinations query excludes anything not due, in SQL.
2. The sweep re-checks before dispatching, since the query's snapshot ages.
3. `CatchUp.Destination` checks on entry, so it is safe called from anywhere.
4. It re-checks between pages -- a destination can fall into backoff while its
   own backlog is being replayed, because a failed send does exactly that, and
   a long backlog would otherwise keep dialling it for as long as the pages
   last.

The sweep DISPATCHES rather than performs. `wake_destination` starts a
background process and returns (`federation/sender/__init__.py:1164`), so
Synapse's waker never waits on a destination. Doing it inline instead makes a
sweep as slow as its slowest destination -- and on a list where half the
entries no longer exist, with `client_timeout: 180s` and twenty retries behind
it, that is not a tail case.

One wart, shared with Synapse: `get_catch_up_room_event_ids` joins `events`, so
a `destination_rooms` row whose event has since been purged yields nothing.
Catch-up then finds no work, exits, and never advances the cursor -- leaving
that destination in the outstanding list to be swept again every minute. Seen
in practice on this deployment. It is wasted work rather than a correctness
problem, and behaving differently from Synapse here would be worse than
matching it.

## 10. Backoff, and why it is not optional

A homeserver's destination list is mostly a graveyard. On aguiarvieira.pt,
of 27,827 known destinations **13,416 are backing off right now** and 16,335
carry a retry interval at or past twelve hours -- servers that were shut down,
domains that expired, hosts that will never answer again.

A sender without a growing, persistent backoff attempts roughly half its
destination list continuously. That is wasted work locally and unsolicited
traffic at the other end.

Synapse gates this in two places and both are needed:

- **In front of the transmission loop.** `get_retry_limiter` raises
  `NotRetryingDestination` at the top of `_transaction_transmission_loop`
  (`per_destination_queue.py:351`). Without this, the backoff limits only how
  often a destination is *enqueued* for, not how often it is *dialled* -- a busy
  room keeps a dead server under continuous connection attempts.
- **When routing.** `filter_destinations_by_retry_limiter` drops destinations
  that are not due, with an hour of slack so a recovering server is picked up
  in the current batch rather than the next.

Growth is `interval * multiplier * uniform(0.8, 1.4)`, capped
(`retryutils.py:226`). The jitter is not decoration: without it every
destination that failed in the same incident retries in the same instant, and a
server coming back is met by the whole backlog at once. A success clears the
backoff outright rather than decaying it -- one working request is enough
evidence, and a decay would keep a recovered server throttled for as long as it
had been broken.

The per-request retry inside one attempt is separate and is Synapse's long
algorithm for `/send`: `4^(max_long_retries + 1 - retries_left)`, capped at
`max_long_retry_delay`, times the same jitter
(`matrixfederationclient.py:851`). The short algorithm applies to other
federation calls, which this worker does not make.

## 11. What a federation sender does NOT use

Worth stating, because it saves reading the config in alarm later.

`instance_map` is how workers learn to reach each other over HTTP replication,
and a worker needs an entry there only if it has a replication listener that
others call. A federation sender is not a stream writer, so nothing addresses it
that way. The one setting that would make other workers call a sender over
HTTP replication is `outbound_federation_restricted_to`
(`config/workers.py:483`, used at `matrixfederationclient.py:422`), which makes
every other worker proxy its outbound federation requests through the named
instances. It is unset on this deployment, and while it is unset a sender's
`instance_map` entry and its replication listener are inert — a duplicated or
wrong socket path there has no effect.

This worker never makes replication HTTP calls at all: it reads Redis and
PostgreSQL and nothing else. `instance_map` is parsed only so startup can say
something useful if a configured sender has no address.
