# Operations

## Deploy

```bash
git clone https://github.com/mikkerlo/bmw-cardata-collector.git
cd bmw-cardata-collector

mkdir -p data/secrets config
chmod 700 data/secrets
cp config.example.yaml config/config.yaml
$EDITOR config/config.yaml          # VIN(s), container id, budget

printf 'BMW_CLIENT_ID=%s\n' '<your-client-id>' > .env
chmod 600 .env

# The image runs as uid 10001; the volume must be writable by it.
sudo chown -R 10001:10001 data

docker compose run --rm collector auth login     # one time, interactive
docker compose up -d
```

To pin a deployment, replace the `:main` tag in `compose.yaml` with an
immutable reference so a redeploy cannot silently change the running code.
A digest is the strongest form:

```yaml
# Strongest: cannot change at all.
image: ghcr.io/mikkerlo/bmw-cardata-collector@sha256:<digest>
# Or a per-commit tag, which the publish workflow always produces.
image: ghcr.io/mikkerlo/bmw-cardata-collector:sha-<full-commit-sha>
```

Each publish run prints the digest and the full tag list in its job summary.
`docker buildx imagetools inspect ghcr.io/mikkerlo/bmw-cardata-collector:main`
shows the current digest and platforms without pulling the image.

### Running without Docker, under systemd

For a host where the collector should survive reboots and logouts,
[`deploy/bmw-cardata-collector.service`](../deploy/bmw-cardata-collector.service)
runs it from a virtualenv instead of a container:

```bash
python3 -m venv .venv && ./.venv/bin/pip install .
./.venv/bin/collector -c config/config.yaml auth login     # one time

sudo cp deploy/bmw-cardata-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bmw-cardata-collector

systemctl status bmw-cardata-collector
journalctl -u bmw-cardata-collector -f        # logs are already JSON
```

Adjust `User`, the paths and the virtualenv location for your host. The unit
is hardened (`ProtectSystem=strict`, `ProtectHome=read-only`, no capabilities,
`SystemCallFilter=@system-service`) with the data directory as the only
writable path, so the database and credential must live under the directory
named in `ReadWritePaths`.

Two details worth keeping if you edit it:

* `TimeoutStopSec=60` with `KillSignal=SIGTERM` — the collector drains its
  write queue and checkpoints SQLite on SIGTERM, and killing it mid-commit
  would throw away queued events.
* `StartLimitBurst=5` / `StartLimitIntervalSec=600` — a crash loop must not
  hammer BMW, which allows one streaming connection per account and
  rate-limits REST. After five starts in ten minutes systemd gives up and
  waits for a human.

To remove it: `sudo systemctl disable --now bmw-cardata-collector` and delete
`/etc/systemd/system/bmw-cardata-collector.service`.

### Prerequisites the collector cannot do for you

All five are manual steps in the [BMW CarData
portal](https://bmw-cardata.bmwgroup.com/), and the collector will not touch
your BMW account settings:

1. A CarData client exists and you have its client ID.
2. That client is subscribed to **both** *CarData API* and *CarData Stream*.
3. **Streaming descriptors are selected.** Skip this and the broker accepts
   your connection and then refuses the subscription — the single most common
   cause of "connected but no data".
4. For `telematic_snapshot`, a container exists and you have its ID.
5. The vehicle is mapped to the account.

## Monitor

```bash
curl -s localhost:9090/health/live            # process alive
curl -s localhost:9090/health/ready | jq      # 200 = archiving, 503 = not
curl -s localhost:9090/metrics
docker compose logs -f collector
```

`/health/ready` reports the phase, the last error, and per-component status.
Phases: `starting`, `opening_storage`, `awaiting_authorization`,
`discovering_vehicles`, `running`, `degraded`, `stopping`, `fatal`.

### What to alert on

| Condition | Meaning |
| --- | --- |
| `collector_ready == 0` for > 10 min | Not archiving. Read `/health/ready` for the reason. |
| `collector_storage_write_failures_total` increasing | **Events are being lost.** Disk full, permissions, or corruption. |
| `collector_queue_overflows_total` increasing | **Events are being lost.** Storage cannot keep up. |
| `bmw_auth_reauthorization_required == 1` | Run `collector auth login` — a human is required. |
| `collector_storage_writable == 0` | The volume is not writable. |
| `bmw_rest_quota_exhausted == 1` | BMW is refusing REST until 00:00 UTC. Possibly another client shares the account. |
| `bmw_mqtt_reconnects_total` rising steadily | Unstable connection; check logs for the reason. |

### What *not* to alert on

* **A stale `bmw_mqtt_last_message_timestamp_seconds`.** A parked car sends
  nothing for days. This is normal and is why liveness does not depend on it.
* **`bmw_mqtt_connected == 0` briefly.** Reconnects, including the planned
  recycle before each id-token expiry, are routine.
* **`collector_phase_info{phase="degraded"}`** on its own — it covers a brief
  reconnect as well as a real problem. Pair it with duration.

A useful first alert set is: not-ready for 10 minutes, any write failure or
queue overflow, and reauthorization required.

## Day-to-day

```bash
C="docker compose run --rm collector"

$C auth status --refresh        # verify the credential still works
$C vehicles list                # local view, no quota cost
$C vehicles list --refresh      # ask BMW (costs 1 of 50 daily requests)
$C verify                       # integrity check + archive summary
$C incidents --limit 50         # write failures, auth denials, quota events
$C snapshot basic_data          # run one REST task now (costs quota)
```

### Add a vehicle

1. Map it to your account in the BMW portal and select its streaming
   descriptors.
2. Add the VIN to `bmw.vehicles.vins` in `config/config.yaml`.
3. `docker compose up -d --force-recreate collector`
4. `$C vehicles list` — confirm it appears under both `mapped` and `active`.

The collector records when it first saw the vehicle. **It cannot import
anything from before that moment**: there is no historical backfill in BMW's
streaming API, and this MVP does not scrape My BMW.

A VIN appearing under `mapped` does not prove data will flow. A secondary
mapping, missing descriptor selection, or a vehicle that simply never reports
all look the same from here. Confirm with
`bmw_mqtt_messages_received_total` and a `vehicles list`.

### Pause or remove a vehicle

```bash
$C vehicles disable WBA...   # stops collection, keeps every stored event
$C vehicles enable  WBA...   # resumes
```

Then recreate the container so the subscription set changes. Disabling never
deletes anything, and nothing in this tool deletes archived events — there is
no automatic pruning and no destructive purge command.

## Back up

```bash
# Consistent snapshot via SQLite's online backup API.
docker compose run --rm -v /mnt/backup:/backup collector \
  backup -o /backup/collector-$(date -u +%Y%m%dT%H%M%SZ).sqlite3
```

**Do not just copy `data/collector.sqlite3`.** In WAL mode recent commits may
still live in `data/collector.sqlite3-wal`, so a plain copy can be silently
short. `collector backup` writes one self-contained file.

Back up `data/secrets/oauth.json` too — losing it means re-running
`auth login`, which is survivable but needs a human. It is a credential: store
it as one.

A nightly host cron is enough:

```cron
17 3 * * * cd /srv/bmw-cardata-collector && docker compose run --rm \
  -v /mnt/backup:/backup collector backup \
  -o /backup/collector-$(date -u +\%Y\%m\%d).sqlite3 >> /var/log/bmw-backup.log 2>&1
```

Pulling backups to another host is out of scope for this MVP; a `rsync`/
`restic` job on the backup target is the normal answer.

## Restore

```bash
docker compose down
cp /mnt/backup/collector-20261018T030000Z.sqlite3 data/collector.sqlite3
rm -f data/collector.sqlite3-wal data/collector.sqlite3-shm   # stale sidecars
sudo chown 10001:10001 data/collector.sqlite3
docker compose run --rm collector verify     # expect integrity_check: ok
docker compose up -d
```

Removing the `-wal` and `-shm` files matters: left in place next to a restored
database they belong to a different file and will be rejected or ignored.

Events received while the collector was down are **gone** — QoS 0, no replay.
Restoring recovers the archive, not the gap.

## Export and replay

```bash
$C export -o /data/events.jsonl                    # everything
$C export --source mqtt --since 2026-10-01T00:00:00+00:00 -o /data/oct.jsonl
$C export --vin WBA... --limit 1000                # to stdout
$C import -i /data/events.jsonl                    # idempotent by event UUID
```

Import keys on the original `event_uuid`, so importing the same file twice
inserts each event once. That makes an export safe to replay into a second
database.

An export is **raw vehicle telemetry, positions included if your account
streams them**. It is written mode `0600`. Treat it like the private data it
is; do not attach one to a bug report.

## Troubleshoot

### `/health/ready` returns 503

Read `last_error` — it names the reason. Common ones:

| `last_error` contains | Do this |
| --- | --- |
| `not authorized with BMW` | `auth status --refresh`, then `auth login` if needed |
| `MQTT streaming is not authorized` | Check the client is subscribed to *CarData Stream* and descriptors are selected |
| `MQTT has not connected yet` | Normal for the first few seconds; if it persists, check logs |
| `durable storage is not writable` | Volume permissions or a full disk |
| `the last storage write failed` | Check `incidents`; events are being lost |

### Connected, subscribed, but no data

Almost always step 3 of the prerequisites: no streaming descriptors selected in
the portal. Check `bmw_mqtt_subscribed_topics` is non-zero and
`collector incidents` for `mqtt_subscribe_refused`. Then confirm the car has
actually been driven or woken since you subscribed — a parked car reports
nothing.

### `reauthorization_required`

BMW refresh tokens last about 14 days and rotate on every use. A collector down
longer than that needs `auth login` again. Also check the client is still
subscribed to both products in the portal.

### 403 `CU-429` / quota exhausted

The 50/day allowance is per BMW account and shared with every client signed in
to it. Check whether something else — BavarianData, a script, the portal — is
using the same account. Lower `requests_per_day_budget` or lengthen
`interval_hours`. It clears at 00:00 UTC.

### "another collector already holds …"

The instance lock is doing its job. Find the other process
(`docker ps`, `ps aux | grep collector`). If a container was killed hard, the
kernel releases the `flock` when the process dies — a stale lock file alone
does not block a start.

Note the lock is **host-local**. Two machines will each take their own lock and
then fight over BMW's single allowed streaming connection, which shows up as
both collectors reconnecting constantly. Run exactly one.

### Queue overflows

Storage cannot keep up. Check disk I/O and free space. `synchronous: NORMAL`
trades some durability for throughput; raising `queue_max` buys headroom for
bursts but not for a sustained deficit.

## Integration verification status

What the automated suite covers, with no BMW credentials and no network:

| Verified in CI | How |
| --- | --- |
| Byte-exact storage, including non-UTF-8 and empty payloads | unit |
| Crash recovery: SIGKILL mid-write, then reopen and verify | integration, real subprocess |
| Graceful shutdown drains the queue | integration |
| Backup → restore → integrity check | unit + CLI integration |
| Export → import round-trip, idempotent replay | unit + CLI integration |
| Device-code flow, slow-down backoff, denial, expiry | unit, scripted HTTP |
| Refresh-token rotation and persistence; reauth vs transient failure | unit |
| Credential file mode `0600`, atomic replace, umask-proof | unit |
| MQTT archival, reconnect backoff bounds and jitter, token recycling | unit, stub broker |
| Auth-denial hard backoff (no reconnect storm) | unit + integration |
| Quota accounting, persistence across restart, 403 `CU-429` parking | unit |
| No catch-up storms; truncated tasks resume fairly | unit |
| Readiness and degradation transitions; endpoints answer | integration, real HTTP |
| Instance lock refuses a second start; released after a crash | unit + integration |
| No VIN or token in metrics, logs, incidents or auth status | unit + integration |
| Container runs non-root; `simulate` + `verify` inside the image | CI |

**Not verified — requires a real BMW account.** As of 2026-10-21 the author has
not run this against live CarData, so the following are implemented from BMW's
published specifications and are *unproven in production*:

* that the device-code flow completes against the real GCDM endpoint;
* that the real broker accepts `gcid` + `id_token` on port 9000 and that
  `<gcid>/<vin>` is the right topic shape;
* the exact wire shape of real streaming payloads (the archive does not care,
  but the VIN/timestamp extraction used for indexing does);
* that REST responses match the published schemas, and that BMW's error body
  really carries `exveErrorId: CU-429` on quota exhaustion;
* real-world token lifetimes and whether id-token expiry behaves as assumed;
* whether the wildcard topic works as documented;
* actual per-request quota accounting on BMW's side.

If any of these differ in practice, the raw archive is still correct — the
payload bytes are stored before anything is interpreted. What may need
adjusting is the indexing metadata and the error classification.

To validate against a real account: complete the portal prerequisites, run
`auth login`, then `auth status --refresh`, `vehicles list --refresh`,
`snapshot basic_data`, and finally `run` with
`telemetry.log_level: DEBUG` while driving the car. Check
`bmw_mqtt_messages_received_total` rises and `collector verify` counts events.
