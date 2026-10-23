# Observed behaviour of the live BMW CarData service

Everything here was **measured against the real service**, not read from a
specification. Where it contradicts BMW's published documents, the document is
noted so the difference is obvious.

Recorded 2026-10-23 against one account with a single PRIMARY-mapped vehicle.
BMW can change any of this without telling anyone; treat it as observation,
not contract. No account identifier, GCID, VIN or token appears below.

---

## 1. The broker closes idle connections at ~60s, whatever keepalive you ask for

**This is a data-loss bug waiting to happen, and nothing in BMW's docs hints at it.**

The streaming broker drops a connection roughly **60 seconds after the last
packet in either direction**, regardless of the keepalive value sent in
CONNECT. Meanwhile the usual MQTT client (paho, under aiomqtt) sends `PINGREQ`
only once a *full* keepalive interval has already elapsed — the spec permits a
server to wait 1.5×, so clients lean on that slack. BMW does not grant it.

With `keepalive=60` the ping fires at t+60 and races a deadline that expires at
t+60. BMW wins, every time:

```
keepalive=60
  t+  0.59s  Sending CONNECT (… k60)
  t+  0.64s  Received CONNACK (0, 0)
  t+  0.68s  SUBSCRIBED, now idling
  t+ 60.59s  DROPPED: Disconnected during message iteration     ← no PINGREQ ever sent
```

With `keepalive=30` the ping lands at t+30, resetting BMW's idle timer well
before it expires, and the connection is stable:

```
keepalive=30
  t+ 30.40s  Sending PINGREQ
  t+ 30.43s  Received PINGRESP
  t+ 60.43s  Sending PINGREQ
  t+ 60.46s  Received PINGRESP
  t+ 90.46s  Sending PINGREQ
  t+ 90.49s  Received PINGRESP
  t+100.47s  STILL CONNECTED after 100s
```

Confirmed in the deployed service: 162 s of continuous uptime at
`keepalive=30`, against a reconnect at t+60 and t+120 at `keepalive=60`.

**Why it matters more than it looks.** Streaming is QoS 0, so anything the car
reports during a reconnect gap is gone permanently. A minute-by-minute
reconnect cycle means a recurring hole in the archive — and because a parked
car is silent anyway, you would not notice the loss until you went looking for
a drive that is missing its first few seconds.

**Takeaway:** keep `bmw.mqtt.keepalive` at or below `BMW_IDLE_DEADLINE_SECONDS
/ 2` (30 s). The config loader warns above that. Lowering the keepalive is not
the intuitive fix — the intuitive reading of "dropped at exactly keepalive" is
that the keepalive is *too short* — so measure before adjusting: the absence
of any `PINGREQ` in the log is the tell.

## 1b. The id token expires after ~1h and the broker enforces it

`access_token`/`id_token` come back with `expires_in: 3599`. The broker holds
the connection open for exactly that long and then closes it — the token is
validated once at CONNECT but the session is still bound to its lifetime:

```
12:29:09  subscribed
13:29:08  Disconnected    (held 60.0 min)
14:29:13  Disconnected    (held 60.0 min)
```

So a long-lived subscription has to reconnect with a freshly refreshed token
roughly hourly. That is expected and unavoidable; what matters is doing it
*proactively*, a couple of minutes early, instead of being dropped and
reconnecting after a backoff — a drop that lands while the car is reporting
loses whatever arrives in the gap, and QoS 0 has no redelivery.

A planned recycle should also reconnect *immediately*. Reusing the
failure backoff for it costs a few seconds off-air every hour for no reason —
the credentials are known good and the disconnect was our own decision. The
backoff exists for a broker that refuses or instantly closes the connection,
which is a different situation.

**The trap:** the obvious implementation checks the clock after handling each
message, which never runs on an idle connection. A parked car is silent for
days, so the common case is precisely the one where the check never fires.
The deadline has to be driven by a timer that fires with or without traffic.

A second trap sits behind the first. `aiomqtt`'s `MessagesIterator.__anext__`
reads the queue via a task and cancels that task if the read is interrupted;
a message already dequeued at that instant is discarded. So `asyncio.wait_for`
around the message read trades an hourly disconnect for an occasional silently
dropped message, which is worse. The read has to be left armed across clock
ticks rather than cancelled and re-issued.

## 2. Vehicle mappings come back as an array, not an object

BMW's `CARDATA API` v1.0.0 spec types the 200 response of
`GET /customers/vehicles/mappings` as a single `VehicleMappingDto` object.

The live endpoint returns a **JSON array** of those objects:

```json
[{"vin": "…", "mappingType": "PRIMARY", "mappedSince": "2026-09-25T11:48:31.041Z"}]
```

Anyone generating a client from the spec alone will mis-parse this. The parser
in `vehicle_registry.py` accepts the array, a bare object, and a few wrapped
shapes (`mappings`, `vehicleMappings`, `items`, `content`) for that reason.

## 3. Streaming connection parameters (all confirmed)

| Parameter | Value |
| --- | --- |
| Host / port | `customer.streaming-cardata.bmwgroup.com:9000`, TLS |
| Username | the account's GCID |
| Password | the OIDC `id_token` (needs the `openid` scope) |
| Topic | `<gcid>/<vin>` |
| QoS | 0 — subscribe granted at 0 |
| CONNACK | `(0, 0)` on success |

A second client connecting with the **same MQTT client identifier** must, per
the MQTT specification, cause the broker to disconnect the first. This
collector identifies itself as `collector-<gcid>`, so two copies would evict
each other in a permanent loop. The host-local `flock` prevents that by
accident on one machine but coordinates nothing across machines.

Whether a *differently*-identified client on the same account is rejected
cleanly or also causes eviction is **not tested** — BMW documents one
streaming connection per user, but the failure mode is unobserved.

## 3b. A granted SUBACK does not mean data will arrive

Measured on this account: BMW grants the subscription with reason code `0` on
the exact topic `<gcid>/<vin>`, on the `+` wildcard, and on `#`:

```
  exact     codes=[0]  -> GRANTED
  wildcard  codes=[0]  -> GRANTED
  hash      codes=[0]  -> GRANTED
```

**A grant proves only that the topic is acceptable, not that anything will
ever be published to it.** With streaming descriptors unselected in the
portal, the broker accepts the subscription and then stays silent forever —
which looks exactly like a parked car. There is no API that reports whether
descriptors are configured, so this cannot be checked from code; the portal is
the only source of truth.

Two consequences worth keeping in mind when debugging silence:

* Subscribing to `<gcid>/#` is a useful probe. If nothing arrives on `#`
  either, then nothing is being published under the account at all, and the
  problem is upstream of any topic-shape question.
* `subscribe()` returns the SUBACK codes rather than raising on refusal, so
  code that ignores the return value cannot tell a working subscription from a
  rejected one. Codes 0-2 are the granted QoS; anything higher is a refusal.

## 4. Authorization

The device-code flow behaves as documented:

* `verification_uri` is `https://customer.bmwgroup.com/oneid/link`
* `user_code` is 8 characters, mixed-case alphanumeric
* `expires_in` is 300 s; `interval` is 5 s
* `slow_down` widens the poll interval by 5 s (RFC 8628)

Token lifetimes seen: `access_token` `expires_in` 3599 s; an `id_token` is
returned whenever `openid` is in the granted scopes. The `refresh_token`
rotates on **every** exchange — persist the new one or you will be
re-authorizing by hand.

With the CarData client subscribed to both the *API* and *Stream* products,
all four scopes are granted:

```
authenticate_user  openid  cardata:api:read  cardata:streaming:read
```

The granted-scope list is therefore a reliable way to check the portal
subscription state without guessing. Note the ordering is not stable — compare
as a set.

A caveat from BMW's own documentation that the code relies on: if the client
requests **no** scopes, BMW returns every scope registered for the client. So
an empty granted-scope list proves nothing, and `missing_scopes` reports
nothing in that case rather than crying wolf.

## 5. REST responses

| Endpoint | Observed |
| --- | --- |
| `/customers/vehicles/mappings` | ~94 B array, one entry per mapped vehicle |
| `/customers/vehicles/{vin}/basicData` | ~779 B object: `bodyType`, `brand`, `chargingModes`, `colourCodeRaw`, `colourDescription`, `constructionDate`, … |

Both cost exactly one request against the documented 50/day allowance. The
`x-version: v1` header is required; the call is rejected without it.

## 6. Still unverified

Honest list, so nobody mistakes this page for complete coverage:

* **No streaming message has been observed yet.** The vehicle has not reported
  since collection began, so the payload shape of a real telemetry message,
  and the VIN/timestamp extraction built on it, remain unexercised. The raw
  archive is unaffected either way — bytes are stored before anything is
  parsed.
* **Quota exhaustion.** The 403 / `exveErrorId: CU-429` path is implemented
  from the documentation and has not been triggered.
* **The wildcard topic** `<gcid>/+` is documented but untested here, since the
  account has one vehicle and an explicit allowlist.
* **Idle-deadline stability.** 60 s is what one account showed on one day. It
  could differ by region, load, or BMW's mood.
* **Refresh-token expiry.** Documented at ~14 days of disuse; not waited out.
