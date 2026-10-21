# Security and privacy

## What this software holds

| Data | Where | Sensitivity |
| --- | --- | --- |
| BMW refresh token | `data/secrets/oauth.json`, mode `0600` | **Credential.** Grants read access to your CarData until revoked or expired. |
| Access / id tokens | Process memory only | Credential, short-lived, never written to disk |
| CarData client ID | Environment (`.env`) or a secret file | Semi-secret; identifies your client |
| GCID | Inside the stored credential | Account identifier |
| Raw event payloads | `data/collector.sqlite3` | **Private telemetry.** May include position, state of charge, mileage, doors, charging sessions — whatever you selected in the portal. |
| VINs | Database, config file, logs at DEBUG | Identifies a specific vehicle and, by extension, its owner |

Taken together the database is a movement history of a real person. Treat the
volume as you would a location history, because that is what it is.

## Secret handling

**The password never reaches this software.** Authorization is the OAuth
device-code flow: you sign in on BMW's own site and type a code. The collector
receives tokens, never credentials.

**Only the refresh token is persisted.** It is written at mode `0600`, into a
directory the collector sets to `0700`, by creating a temporary file and
renaming it over the target, then fsyncing the directory. The rename is not
decoration: BMW rotates the refresh token on every exchange, and a torn write
during rotation would cost you the credential and require a human to log in
again.

**Tokens are never printed, logged or returned.** `collector auth status`
reports presence, scope and expiry, never a value. The log formatter redacts
credential-shaped strings — `Authorization:` headers, bare JWTs, and the
values of ~15 sensitive key names — and does so in the **formatter**, so it
also covers records emitted by aiohttp and aiomqtt, which we do not control.
Redaction also runs over incident details before they are stored.

**Client ID:** pass it as `${BMW_CLIENT_ID}` from `.env` (mode `0600`) or as
`${file:/run/secrets/bmw_client_id}`. Not in `compose.yaml`, not in a shell
command that lands in `~/.bash_history`, not in the config file you commit.

### Rotation and revocation

Revoke access in the BMW portal by removing or re-creating the CarData client.
The collector will then fail with `reauthorization_required` and stop retrying,
rather than hammering BMW. To rotate locally, delete
`data/secrets/oauth.json` and run `auth login` again.

## Network exposure

**The management endpoints are unauthenticated**, so they must stay local. The
shipped `compose.yaml` publishes to `127.0.0.1:9090` only. Do not change that
to `0.0.0.0` or put it behind a public reverse proxy without authentication.

Even with no VIN labels, `/metrics` leaks activity patterns: message counts and
timestamps say when a vehicle was driven. `/health/ready` reports counts and
phases, and deliberately not VINs. If you must scrape remotely, use an
authenticated tunnel (Tailscale, WireGuard, SSH) rather than exposing the port.

**Outbound only, to BMW:** HTTPS to `customer.bmwgroup.com` and
`api-cardata.bmwgroup.com`, and MQTT over TLS to
`customer.streaming-cardata.bmwgroup.com:9000`, with certificates verified
against the system trust store. There is no telemetry, no phone-home, no
update check, and no remote access path into the container.

## Container hardening

The shipped compose file runs the collector:

* as **uid 10001**, never root;
* with a **read-only root filesystem**, `/data` as the only writable mount and
  a 16 MB `tmpfs` for `/tmp`;
* with `cap_drop: ALL` and `no-new-privileges`;
* with the config mounted read-only.

The image contains no build toolchain (the wheel is built in a discarded
stage), no shell utilities beyond the Debian slim base, and **no `/data`** —
production data is never baked into an image.

## What is never sent anywhere

* Collected events are **never uploaded** — not to GitHub, not to CI, not
  anywhere. There is no code in this repository that transmits event data to
  any host but BMW's.
* The test suite needs no credentials and makes **no network calls**: OAuth,
  REST and MQTT are exercised against in-process doubles. CI therefore never
  sees a real token, VIN or payload.
* CI refuses to merge a tree containing `.env`, `oauth.json`, a live
  `config.yaml`, a `.sqlite3`/`.db` file, a `.jsonl` export, or anything
  matching a real VIN pattern outside the obviously-fake fixtures. `gitleaks`
  scans the full history as well.

Every VIN in this repository — tests, docs, examples — is fabricated.

## Attack surface

| Vector | Exposure |
| --- | --- |
| Inbound network | Management port only, localhost by default, read-only endpoints, no authentication (so keep it local) |
| Host filesystem | `/data` (database + credential) and `/config` (read-only) |
| BMW responses | Stored as opaque bytes and never evaluated. The scheduler and registry parse a *copy* defensively; a malformed response is logged and skipped, never fatal |
| Config file | Can read any file the process can, via `${file:…}` — intended, and no more privileged than the process itself |
| Dependencies | Five direct, all pinned; no vendored code |

Known accepted risks, stated plainly:

* **The management endpoints have no authentication.** Mitigated by binding to
  localhost; adding auth is the obvious next step if remote scraping is needed.
* **The credential is at rest unencrypted** on a volume you control. Encrypting
  it would need a key stored somewhere the collector can reach unattended,
  which moves the problem rather than solving it. Volume-level encryption is
  the right answer.
* **A root user on the host can read everything.** Nothing here defends against
  that, and nothing claims to.

## Reporting a problem

This is a personal project with no security team and no SLA. Open an issue —
but **do not attach logs, exports, database files, tokens or real VINs.** A
redacted description of what happened is enough to start.

## Your obligations

The MIT license covers this software only. It grants nothing with respect to
BMW's services or data. Your use of BMW CarData is governed by BMW's own terms,
which you should read. If you archive data about a vehicle driven by other
people, their privacy is your responsibility, not this tool's.
