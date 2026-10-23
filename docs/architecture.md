# Architecture

## Goal

Preserve BMW CarData now, so that trips, charging analysis and a UI can be
built later against a complete archive. Everything here follows from one rule:
**the raw bytes BMW sent must survive, and the collector must never claim to
have stored something it did not.**

That rule shapes the rest. Nothing is parsed on the write path, so an unknown
descriptor is archived as faithfully as a known one. A failed commit is counted
as a failure, never as a committed event. Delivery loss is reported rather than
smoothed over, because BMW gives us no way to recover it.

## Process shape

One container, one process, one asyncio event loop:

```
                      ┌───────────────────────────────────────────┐
  BMW GCDM  ─────────▶│ AuthManager                               │
  (OAuth DCF)         │  device-code login, refresh, token upkeep │
                      └───────┬───────────────────────┬───────────┘
                              │ id_token + gcid       │ access_token
                              ▼                       ▼
  BMW broker  ──────▶ ┌──────────────┐        ┌──────────────────┐
  (MQTT QoS 0)        │ MqttReceiver │        │ RestScheduler    │ ◀── quota
                      └──────┬───────┘        └────────┬─────────┘     ledger
                             │ submit()                │ enqueue()
                             ▼                         ▼
                      ┌───────────────────────────────────────────┐
                      │ EventWriter — bounded queue, one writer   │
                      └─────────────────┬─────────────────────────┘
                                        ▼
                      ┌───────────────────────────────────────────┐
                      │ EventStore — SQLite, WAL, synchronous=FULL│
                      └───────────────────────────────────────────┘

  VehicleRegistry ── which VINs to collect (allowlist × BMW mappings)
  TelemetryServer ── /health/live, /health/ready, /metrics  (localhost)
  StandaloneLeader ─ flock, so a second start on this host is refused
```

| Module | Responsibility |
| --- | --- |
| `config.py` | Load YAML, expand `${VAR}` / `${file:…}`, validate, fail with actionable errors |
| `auth.py` | Device-code flow with PKCE, refresh-token rotation, credential file |
| `bmw_source.py` | CarData REST calls returning raw bytes plus redacted metadata |
| `vehicle_registry.py` | Resolve the allowlist against BMW's mappings; enable/disable |
| `mqtt_receiver.py` | The single streaming connection, subscriptions, reconnect policy |
| `rest_scheduler.py` | Quota-aware periodic snapshots, persisted across restarts |
| `writer.py` | Bounded ingest queue and the one task that commits |
| `store.py` | Migrations, append, read, state, incidents, backup, integrity |
| `telemetry.py` | Health and metrics endpoints, lifecycle phase |
| `export.py` | Lossless JSONL export and idempotent re-import |
| `collector.py` | Composition and lifecycle |
| `lock.py` | Host-local single-instance guard, and the seam for a future elector |

## The write path

MQTT QoS 0 means at most once: no acknowledgement travels back to BMW and
nothing can be replayed. Whatever the receiver fails to persist is gone. So the
read loop does the least possible work — inspect the topic, enqueue — and one
writer task owns every commit.

`submit()` never awaits. Blocking the read loop until SQLite caught up would
stall the MQTT connection and eventually get us disconnected, losing more than
the one event we were trying to save. The queue is bounded instead
(`storage.queue_max`, default 10 000). On overflow the newest event is dropped,
counted in `collector_queue_overflows_total`, and recorded as an incident.
Incident rows are coalesced to one per minute: sustained overflow means storage
is already the bottleneck, and writing a row per dropped event would pile load
onto exactly the thing that is struggling. Each row carries the cumulative
count, so nothing is hidden.

Commits are batched (up to 64 events, 50 ms linger) and fsynced. The writer
reports an event as committed only after SQLite's `COMMIT` returns.

REST responses take the same path but use `enqueue()`, which may await: a REST
response cost a request out of a 50-per-day allowance and the scheduler is not
in a latency-sensitive loop, so waiting for queue room beats dropping it.

## Storage

SQLite in WAL mode with `synchronous=FULL` by default — every commit fsynced.
At one BMW account's event rate this costs nothing noticeable and buys
durability across power loss. `NORMAL` is available and documented as the
faster, lossier choice.

WAL is a transaction mechanism, not the archive. `raw_events` is the journal;
the WAL file is checkpointed periodically (and `TRUNCATE` on clean shutdown) so
it does not grow without bound.

Backups use SQLite's online backup API. Copying the live `.sqlite3` file is
unsafe in WAL mode, because recent commits may still be in the `-wal` sidecar
and a plain copy can be silently short.

Every event gets a UUID at receipt. **Identical payloads are never
deduplicated** — the same state of charge reported twice is two real events,
and collapsing them would destroy exactly the timing information that trip
reconstruction needs later. That UUID is preserved through export and import,
so replaying an export into another database is idempotent without inventing
new identities.

All SQLite work goes through one connection, serialized by an `asyncio.Lock`
and executed in a worker thread, so the event loop never blocks on fsync.

## Authorization

Device-code flow with PKCE. The collector never sees the user's password. Only
the refresh token is persisted, at mode `0600`, written by rename of a
temporary file plus a directory fsync — BMW rotates the refresh token on every
exchange, and losing a rotation means a human has to log in again.

Access and id tokens live in memory only. Refresh happens 5 minutes before
expiry, with a floor on token lifetime so a skewed clock cannot make a dead
token look fresh.

The distinction that matters: `invalid_grant` / `access_denied` /
`expired_token` mean a human must act, so they raise
`ReauthorizationRequiredError`, set `bmw_auth_reauthorization_required`, and
stop the retry loop. Anything else — a 500, a timeout — is treated as transient
and retried, because mistaking a BMW outage for a revoked credential would
summon an operator for something that fixes itself.

## MQTT connection policy

BMW permits **one** active streaming connection per user (GCID), not one per
vehicle, so one TLS connection carries every VIN topic. The broker takes the
GCID as username and the OIDC id token as password.

Reconnect policy, in priority order:

1. **Authorization or permission failure** backs off hard (15 minutes by
   default). A revoked client reconnecting every five seconds would be abusive
   and would not start working any sooner. These need a human.
2. **Token expiry** is pre-empted: the connection is recycled ~2 minutes before
   the id token expires, with a freshly refreshed token. The broker drops us
   otherwise.
3. **Everything else** uses exponential backoff with full jitter, clamped to
   the configured window, so a BMW-side outage does not produce a synchronised
   reconnect storm across every collector in the world.
4. **The keepalive is held at half BMW's idle deadline.** Their broker closes
   an idle connection about 60s after the last packet whatever keepalive we
   declare, and the MQTT client only pings once a full keepalive has elapsed,
   so a 60s keepalive drops the connection every minute. Measured, not
   documented — see
   [bmw-api-observations.md](bmw-api-observations.md#1-the-broker-closes-idle-connections-at-60s-whatever-keepalive-you-ask-for).
5. **Even a clean disconnect waits** `reconnect_min_seconds`. If the broker
   accepted and immediately closed the connection, a zero delay would hammer
   it in a tight loop.

A refused subscription is treated as a real failure and recorded, because the
most common cause is a setup step the operator has not done: streaming
descriptors not selected in BMW's portal, or a VIN that is not mapped.

## REST quota

BMW documents 50 requests per day per user, reset at 00:00 UTC, possibly shared
with any other client signed in to the same account. Exceeding it earns a 403
for the rest of the day — which would also block onboarding and diagnostics.
The scheduler is therefore pessimistic:

* Requests are counted **before** they are issued. A failure still consumed
  BMW's allowance even though we got nothing useful back.
* The counter and each task's last-run time live in SQLite, so a restart
  neither re-runs a task that just ran nor forgets what today cost. Restart
  loops were the obvious way to burn a whole day's budget.
* An overdue task runs **once**, not once per missed interval. Five days down
  must not become thirty catch-up requests.
* Scheduled work stops at a reserve (5 requests, clamped so it can never
  swallow the whole budget), leaving room for interactive commands and retries.
* Retries are capped at 3, are themselves counted, and stop early rather than
  eat the reserve.
* A quota rejection (403 `CU-429`) parks all REST activity until the next UTC
  midnight and records an incident.
* When a per-VIN task cannot fit in the remaining budget it is **truncated
  rather than skipped**, and a persisted per-task offset makes the next run
  resume where it stopped — otherwise, with more VINs than budget, the first
  VINs would be collected forever and the rest never.

## Health semantics

`/health/live` says only that this process and its event loop are running. It
deliberately does **not** depend on BMW sending anything: a parked car is
silent for days, and a liveness probe that treated silence as failure would
restart the container for no reason.

`/health/ready` means *this collector can archive whatever arrives*: storage
writable, last write succeeded, authorized with BMW, and — when streaming is
enabled — a receiver that has connected at least once and is not blocked on a
permission problem. It is recomputed on **every request**, not on a timer: an
orchestrator acting on a stale "ready" is worse than a slow probe. Only the
storage write probe is cached from the periodic supervisor, so a scrape does
not cause a database write.

A currently-disconnected receiver that has connected before is `degraded`, not
unready — the collector will archive whatever arrives next, and brief
reconnects are normal.

`bmw_mqtt_last_message_timestamp_seconds` is kept deliberately separate from
`bmw_mqtt_connected` for the same reason: "connected" and "recently heard from"
are different questions.

No metric carries a VIN or GCID label. Label values end up in dashboards, alert
payloads and remote-write storage; per-vehicle detail belongs in the archive.

## Decisions

### Why not depend on zweck's library

[zweckj/bmw-cardata](https://github.com/zweckj/bmw-cardata) is MIT licensed and
covers the same API. It was evaluated seriously and not adopted:

* **It parses into typed models.** That is the right design for a consumer
  library and the wrong one here: this project's whole purpose is to keep the
  original bytes, including fields no model knows about. We would be using it
  and then discarding its main value.
* **It is `0.1.0a3`** — an alpha, last published mid-2026. Pinning an alpha's
  API into the archival path of a service meant to run unattended for years is
  a liability we would have to manage anyway.
* **The protocol surface is small and fully documented.** Device-code flow plus
  five REST endpoints plus an MQTT subscribe is a few hundred lines against
  BMW's own OpenAPI specs, which is less code than the adapter layer would be.

What we do instead: implement directly against the published specifications,
depending only on `aiohttp` and `aiomqtt` — the same stable primitives that
library uses. Its published copies of BMW's spec files were a useful
cross-check on endpoint shapes while writing `const.py`.

### Why SQLite rather than PostgreSQL

Single node, modest write rate, and the archive must be trivially backed up and
restored by one person. SQLite with `synchronous=FULL` gives durability, one
file to copy, and an online backup API. PostgreSQL would add an operational
dependency to buy replication features that are explicitly out of scope.

### Why the raw payload is a BLOB and not JSON

The payload is whatever BMW sent. Storing it as `TEXT` would require it to be
valid UTF-8, and storing it as parsed JSON would require it to be valid JSON —
neither is guaranteed, and a malformed or binary body is exactly the case where
having the original bytes matters most.

## Seams for later

Kept flexible deliberately, with nothing built on speculation:

* **`LeaderElector` protocol** (`lock.py`). `StandaloneLeader` implements it
  with `flock`. An etcd-lease or Postgres-advisory-lock implementation can be
  dropped in without touching the collector. **`StandaloneLeader` coordinates
  one host only** — two machines will each take their own local lock and then
  fight over BMW's single allowed connection.
* **`EventStore`** is the only module that knows SQL. A different backend, or
  a second write target, goes here.
* **`TokenProvider` protocol** (`bmw_source.py`) decouples REST from how tokens
  are obtained.
* **Export/import with stable UUIDs** is already enough to ship an archive to
  an aggregation node idempotently.
* **`RestTaskConfig`** adds endpoints by name; the quota ledger needs no change.

## Explicitly not built

etcd or ZooKeeper leases, fencing, Patroni, PostgreSQL, synchronous
replication, multi-host failover, MQTT fan-out to other consumers, My BMW
reverse engineering, historical trip scraping, browser login automation, maps,
a UI, derived trips, charging cost calculation. None of these are started,
stubbed or half-wired.
