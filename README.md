# bmw-cardata-collector

A small, single-node service that **durably archives raw BMW CarData** for one
or more vehicles:

* every **MQTT streaming** message, stored as the exact bytes BMW sent;
* periodic **REST snapshots** (vehicle mappings, basic data, telematic
  containers, tyre diagnosis), also stored byte for byte;
* in SQLite, with WAL and `synchronous=FULL`, retained indefinitely.

The point is to **preserve the data now** so trip reconstruction, charging
analysis and a UI can be built later against a complete archive. Nothing is
interpreted on the write path: a descriptor this version has never heard of is
archived exactly as faithfully as a known one.

No Home Assistant, no cloud service, no account of ours. It runs in one
container next to your data.

> Independent project, **not affiliated with, endorsed by or supported by BMW**.
> "BMW" and "CarData" are trademarks of their respective owners, used here only
> to say what this talks to.

---

## What it does not do

Read this before deploying it.

| Limitation | Why |
| --- | --- |
| **Data can be lost.** | BMW streams at **MQTT QoS 0** — at most once, no acknowledgement, no replay. Anything sent while the collector, your network or BMW's broker is down is gone permanently. There is no backfill API to recover it. |
| **One connection per BMW account.** | BMW permits a single active streaming connection per user (GCID), *not* one per VIN. A second collector — or BavarianData, or any other client — signed in to the same account will fight this one for the connection. Run exactly one. |
| **50 REST requests per day, per account.** | Documented BMW limit, reset at 00:00 UTC, and **shared** with every other client using the same account. The collector budgets conservatively (35/day by default) and stops rather than spending your whole allowance. |
| **You must configure BMW's side yourself.** | Streaming descriptors are selected by hand in BMW's portal, and REST telematic data needs a container. The collector cannot choose these for you and does not try. |
| **Vehicle-dependent data.** | Which descriptors exist at all depends on the car. GPS, charging and tyre data are not guaranteed. |
| **No high availability.** | One node. The instance lock prevents a second start *on the same host* only; it coordinates nothing across machines. |
| **No trips, no maps, no UI.** | Deliberately out of scope for now. The archive is the product. |

The collector never commands the vehicle, never wakes it, and never changes
anything in your BMW account.

---

## Requirements

* Docker (or Python 3.12+ to run it directly)
* A BMW account with CarData access, and the one-time portal setup below

## 1. BMW portal setup (one time, by hand)

In the [BMW CarData portal](https://bmw-cardata.bmwgroup.com/):

1. **Create a CarData client.** Note the **client ID** (a UUID). There is no
   client secret: authorization uses the device-code flow.
2. **Subscribe that client to both products** — *CarData API* **and**
   *CarData Stream*. Without both, BMW withholds the `cardata:api:read` /
   `cardata:streaming:read` scopes and everything here fails with 403.
3. **Configure the data stream:** choose which descriptors are streamed. This
   is a manual selection per account; **if you skip it the broker accepts your
   connection and then refuses the subscription**, and no data arrives.
4. **For REST telematic snapshots,** create a container listing the technical
   descriptors you want (portal, or `collector containers create`). Note its
   container ID.
5. Confirm the vehicle is **mapped** to your account.

Being mapped is not the same as being collectable — see
[docs/operations.md](docs/operations.md).

## 2. Configure

```bash
mkdir -p ./data/secrets ./config
chmod 700 ./data/secrets
cp config.example.yaml ./config/config.yaml
$EDITOR ./config/config.yaml        # set your VIN(s) and container ID
```

Put the client ID in a `.env` file next to `compose.yaml` — never in the
compose file or your shell history:

```bash
printf 'BMW_CLIENT_ID=%s\n' 'your-client-id-uuid' > .env
chmod 600 .env
```

The config file reads `${BMW_CLIENT_ID}` from the environment. A file-based
secret works too: `client_id: ${file:/run/secrets/bmw_client_id}`.

## 3. Authorize (device-code flow, no password)

```bash
docker compose run --rm collector auth login
```

Open the URL it prints, sign in to BMW, enter the code. The collector stores
**only a refresh token**, at `/data/secrets/oauth.json`, mode `0600`. Your
password never passes through this software, and no token is ever printed,
logged or committed.

Restarts reuse that credential. BMW refresh tokens are valid for roughly
**14 days** and are rotated on every use, so a collector that stays down longer
than that needs `auth login` again — `/health/ready` and
`bmw_auth_reauthorization_required` both say so when it does.

## 4. Run

```bash
docker compose up -d
docker compose logs -f collector

curl -s localhost:9090/health/ready | jq      # 200 = archiving
curl -s localhost:9090/metrics | head -40
```

Management endpoints are **unauthenticated** and published to `127.0.0.1` only.
Do not expose them: even without VIN labels, the metrics describe a private
vehicle's activity.

### Running without Docker

```bash
pip install -e .
collector -c ./config/config.yaml auth login
collector -c ./config/config.yaml run
```

### Try it without a BMW account

`simulate` drives the real queue, writer, SQLite and telemetry path with
synthetic events, so you can verify a deployment before authorizing:

```bash
collector -c ./config/config.yaml simulate --count 100 --serve
```

---

## Day-to-day

```bash
collector auth status --refresh        # verify the credential still works
collector vehicles list                # what is known locally (no quota cost)
collector vehicles list --refresh      # refresh from BMW (costs 1 request)
collector vehicles disable <VIN>       # stop collecting; keeps all history
collector snapshot basic_data          # one REST task now (costs quota)
collector verify                       # integrity check + archive summary
collector incidents                    # recent write failures, auth denials…
collector backup -o /backup/snap.sqlite3
collector export --source mqtt -o /tmp/events.jsonl
collector import -i /tmp/events.jsonl  # idempotent by event UUID
```

Adding a vehicle: map it in the portal, select its streaming descriptors, add
the VIN to `bmw.vehicles.vins`, restart. The collector records when it first
saw the vehicle; **it cannot import trips from before that moment.**

Removing one: `collector vehicles disable <VIN>`. Collection stops, history
stays. Nothing in this tool deletes archived events.

Back up with `collector backup` (SQLite's online backup API), **not** by
copying the live `.sqlite3`: in WAL mode recent commits may still be in the
`-wal` sidecar, so a plain copy can be silently short. Full procedures,
including restore, are in [docs/operations.md](docs/operations.md).

## What gets stored

| Table | Contents |
| --- | --- |
| `raw_events` | Original payload bytes + receive time, source, VIN, topic, QoS, BMW's own timestamp, and redacted HTTP metadata for REST |
| `collector_state` | Scheduler cursors and the persisted REST quota ledger |
| `collector_incidents` | Write failures, auth denials, quota exhaustion, queue overflow |
| `vehicles` | Known VINs, first-observed time, mapping type, enabled flag |

Each event gets a locally unique UUID at receipt. Identical payloads are
**never** deduplicated — the same value reported twice is two real events.
That UUID is preserved across export and import, so a replay into another
database is idempotent without inventing new identities.

## Documentation

* [docs/architecture.md](docs/architecture.md) — components, data flow, design decisions
* [docs/operations.md](docs/operations.md) — deploy, monitor, back up, restore, troubleshoot
* [docs/security.md](docs/security.md) — secret handling, what is private, threat notes

## Development

```bash
pip install -e '.[dev]'
ruff check . && ruff format --check . && mypy && pytest
```

The test suite needs **no BMW credentials** and makes no network calls:
auth, REST and MQTT are exercised against fakes and a stub broker. What a real
account verifies is listed in [docs/operations.md](docs/operations.md#integration-verification-status).

## License

MIT — see [LICENSE](LICENSE). Third-party notices, including the BMW
specifications this was built against, are in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

An MIT library grants no rights over BMW's services or your data: your use of
CarData remains subject to BMW's own terms, which you should read separately.
