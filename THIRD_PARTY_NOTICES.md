# Third-party notices

This repository's own source code is MIT licensed — see [LICENSE](LICENSE).
It contains no code copied from another project.

## Runtime dependencies

Installed from PyPI at the versions pinned in
[`requirements.txt`](requirements.txt); none are vendored into this repository.

| Package | License | Used for |
| --- | --- | --- |
| [aiohttp](https://github.com/aio-libs/aiohttp) | Apache-2.0 | HTTPS to BMW's OAuth and CarData REST endpoints; serving the local management endpoints |
| [aiomqtt](https://github.com/empicano/aiomqtt) | BSD-3-Clause | Async MQTT client (wraps paho-mqtt) |
| [paho-mqtt](https://github.com/eclipse-paho/paho.mqtt.python) | EPL-2.0 / EDL-1.0 | MQTT protocol implementation, via aiomqtt |
| [PyYAML](https://github.com/yaml/pyyaml) | MIT | Configuration parsing |
| [prometheus-client](https://github.com/prometheus/client_python) | Apache-2.0 | Metrics exposition |

Transitive dependencies of aiohttp (`aiohappyeyeballs`, `aiosignal`, `attrs`,
`frozenlist`, `idna`, `multidict`, `propcache`, `yarl`) are also pinned in
`requirements.txt` and carry permissive licenses (Apache-2.0, MIT or
BSD-style). `pip-licenses` or `pip show` will report the exact terms for the
exact versions installed in your image.

Development-only dependencies (pytest, pytest-asyncio, ruff, mypy,
types-PyYAML) are not present in the published image.

## Specifications this was written against

The protocol handling in `src/bmw_collector/const.py`, `auth.py`,
`bmw_source.py` and `mqtt_receiver.py` was implemented from BMW's own public
documentation. No BMW specification file, schema or documentation text is
redistributed here.

* BMW CarData API documentation and integration guide (v1.6, 2026-07-30) —
  <https://bmw-cardata.bmwgroup.com/customer/public/api-documentation>
* BMW CarData API specifications (`CARDATA API` v1.0.0 and
  `Customer OAuth2.0 Login - Device Code Flow` v1.6) —
  <https://bmw-cardata.bmwgroup.com/customer/public/api-specification>

## Related projects

Neither is a dependency of this project, and no code from either is included.

* [zweckj/bmw-cardata](https://github.com/zweckj/bmw-cardata) (MIT) — an async
  Python client for the same API. It was evaluated as a dependency and the
  decision not to use it, with reasons, is recorded in
  [docs/architecture.md](docs/architecture.md#why-not-depend-on-zwecks-library).
  Its published copies of BMW's specification files were a useful
  cross-check while confirming endpoint shapes.
* [JustChr/BavarianData](https://github.com/JustChr/BavarianData) (MIT) — a
  Home Assistant integration for BMW CarData. Studied for context only;
  **no code is copied from it**, and its name and branding are not used here.
  Its published research into stream-attribute activation being portal-only is
  cited, with attribution, in
  [docs/bmw-api-observations.md](docs/bmw-api-observations.md); that document
  records the conclusion and endpoint shapes as findings, not as code.

## Trademarks

"BMW", "CarData" and related names and logos are trademarks of Bayerische
Motoren Werke AG. This project is independent and is not affiliated with,
endorsed by, or supported by BMW. No BMW logo or brand asset is included in
this repository or the published image.

Your use of BMW CarData is governed by BMW's own terms of service. The MIT
license on this software grants no rights to BMW's services or data.
