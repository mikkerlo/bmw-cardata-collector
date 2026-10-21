"""The assembled collector: readiness, degradation, shutdown.

The Collector is run in-process with its BMW-facing edges replaced by the same
doubles the unit tests use, so the composition itself is exercised: lock,
storage, writer, auth retry, MQTT receiver, scheduler and telemetry together.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import aiohttp
import pytest

from bmw_collector.collector import Collector
from bmw_collector.config import Config, load_config
from bmw_collector.lock import StandaloneLeader
from bmw_collector.store import EventStore, RawEvent
from bmw_collector.telemetry import Phase
from tests.conftest import VALID_VIN, FakeBroker, FakeMessage, stream_payload

GCID = "aa111192-d570-4d3d-8835-656ee00edcd7"
PORT = 19501


def make_config(tmp_path: Path, *, port: int, mqtt: bool = True, rest: bool = False) -> Config:
    """A configuration pointing entirely inside tmp_path."""
    tasks = (
        "    tasks:\n      - type: vehicle_mapping\n        interval_hours: 24\n" if rest else ""
    )
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
node:
  name: lifecycle-test
storage:
  sqlite_path: {tmp_path / "collector.sqlite3"}
  queue_max: 1000
bmw:
  client_id: test-client
  credentials_file: {tmp_path / "secrets" / "oauth.json"}
  account_key: lifecycle
  vehicles:
    mode: allowlist
    vins:
      - {VALID_VIN}
  mqtt:
    enabled: {"true" if mqtt else "false"}
    reconnect_min_seconds: 0.05
    reconnect_max_seconds: 0.1
    auth_failure_backoff_seconds: 30
  rest:
    enabled: {"true" if rest else "false"}
{tasks}telemetry:
  listen_host: 127.0.0.1
  port: {port}
  log_level: DEBUG
""",
        encoding="utf-8",
    )
    return load_config(path)


class StubAuth:
    """Replaces AuthManager inside a running Collector."""

    def __init__(self, *, fail_until: int = 0, error: Exception | None = None) -> None:
        from bmw_collector.auth import AuthState

        self.state = AuthState()
        self.credential_path = Path("/nonexistent/oauth.json")
        self.refreshes = 0
        self.fail_until = fail_until
        self.error = error

    def load_persisted(self) -> bool:
        """Pretend a credential is stored."""
        return True

    async def refresh(self) -> Any:
        """Succeed, or fail as scripted."""
        self.refreshes += 1
        if self.error is not None and self.refreshes <= self.fail_until:
            raise self.error
        self.state.authorized = True
        self.state.gcid_present = True
        self.state.scopes = (
            "authenticate_user",
            "openid",
            "cardata:api:read",
            "cardata:streaming:read",
        )
        self.state.access_expires_at = time.time() + 3600
        return None

    def missing_scopes(self) -> tuple[str, ...]:
        """No missing scopes."""
        return ()

    async def id_token(self) -> tuple[str, str]:
        """Broker credentials."""
        return GCID, "id-token"

    def id_token_expiry(self) -> float | None:
        """Far-future expiry, so no recycling during the test."""
        return time.time() + 3600

    async def access_token(self) -> str:
        """A bearer token."""
        return "access-token"

    async def close(self) -> None:
        """Nothing to close."""


async def start_collector(
    config: Config,
    *,
    broker: FakeBroker | None = None,
    auth: Any = None,
) -> tuple[Collector, asyncio.Task[int]]:
    """Launch a Collector with its BMW edges stubbed."""
    collector = Collector(config)
    stub_auth = auth or StubAuth()
    original_build = collector._build_components

    async def build_with_doubles() -> None:
        collector.auth = stub_auth
        await original_build()
        if collector.receiver is not None and broker is not None:
            collector.receiver._client_factory = broker

    collector._build_components = build_with_doubles  # type: ignore[method-assign]

    original_authorize = collector._authorize

    async def authorize_with_stub() -> bool:
        collector.auth = stub_auth
        return await original_authorize()

    collector._authorize = authorize_with_stub  # type: ignore[method-assign]
    task = asyncio.create_task(collector.run())
    return collector, task


def ready_now(collector: Collector) -> bool:
    """Readiness as the endpoint would report it right now.

    ``health.ready`` alone is the last computed answer; the endpoint
    re-evaluates on each request, and so does this.
    """
    collector.health.refresh()
    return collector.health.ready


async def wait_for(predicate: Any, *, timeout: float = 15.0) -> None:
    """Poll until ``predicate()`` is true."""
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.02)


async def fetch(port: int, path: str) -> tuple[int, dict[str, Any]]:
    """GET a management endpoint."""
    async with (
        aiohttp.ClientSession() as session,
        session.get(f"http://127.0.0.1:{port}{path}") as response,
    ):
        if response.content_type == "application/json":
            return response.status, await response.json()
        return response.status, {"text": await response.text()}


async def shutdown(collector: Collector, task: asyncio.Task[int]) -> int:
    """Ask the collector to stop and wait for its exit code."""
    collector._request_stop(__import__("signal").SIGTERM)
    async with asyncio.timeout(30):
        return await task


# -- happy path --------------------------------------------------------------
async def test_collector_becomes_ready_and_archives_a_message(tmp_path: Path) -> None:
    config = make_config(tmp_path, port=PORT)
    broker = FakeBroker(
        messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", stream_payload(VALID_VIN))]
    )
    collector, task = await start_collector(config, broker=broker)
    try:
        await wait_for(lambda: ready_now(collector))
        status, body = await fetch(config.telemetry.port, "/health/ready")
        assert status == 200
        assert body["phase"] in {"running", "degraded"}
        assert body["components"]["mqtt"]["connected"] is True
        assert body["components"]["auth"]["authorized"] is True
        await wait_for(lambda: collector.writer.committed >= 1)
    finally:
        exit_code = await shutdown(collector, task)
    assert exit_code == 0

    store = EventStore(config.storage.sqlite_path)
    store.open_sync()
    try:
        assert store.count_events_sync() == 1
        assert store.integrity_check_sync() == "ok"
    finally:
        store.close_sync()


async def test_liveness_responds_during_startup(tmp_path: Path) -> None:
    config = make_config(tmp_path, port=PORT + 1)
    auth = StubAuth(fail_until=99, error=RuntimeError("auth is wedged"))
    collector, task = await start_collector(config, broker=FakeBroker(), auth=auth)
    try:
        # Never ready, but telemetry must answer so the stall is diagnosable.
        await wait_for(lambda: collector.health.phase is not Phase.STARTING)
        status, body = await fetch(config.telemetry.port, "/health/live")
        assert status == 200
        assert body["status"] == "alive"
        ready_status, ready_body = await fetch(config.telemetry.port, "/health/ready")
        assert ready_status == 503
        assert ready_body["ready"] is False
    finally:
        await shutdown(collector, task)


async def test_metrics_are_served_and_vin_free(tmp_path: Path) -> None:
    config = make_config(tmp_path, port=PORT + 2)
    broker = FakeBroker(
        messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", stream_payload(VALID_VIN))]
    )
    collector, task = await start_collector(config, broker=broker)
    try:
        await wait_for(lambda: collector.writer.committed >= 1)
        _status, body = await fetch(config.telemetry.port, "/metrics")
        text = body["text"]
        assert "collector_ready 1.0" in text
        assert "bmw_mqtt_connected 1.0" in text
        assert 'collector_events_committed_total{source="mqtt"}' in text
        assert VALID_VIN not in text
        assert GCID not in text
    finally:
        await shutdown(collector, task)


# -- degraded and blocked states --------------------------------------------
async def test_missing_authorization_keeps_it_not_ready_and_says_why(
    tmp_path: Path,
) -> None:
    from bmw_collector.auth import ReauthorizationRequiredError

    config = make_config(tmp_path, port=PORT + 3)
    auth = StubAuth(
        fail_until=99, error=ReauthorizationRequiredError("invalid_grant", "token expired")
    )
    collector, task = await start_collector(config, broker=FakeBroker(), auth=auth)
    try:
        await wait_for(lambda: collector.health.phase is Phase.AWAITING_AUTHORIZATION)
        status, body = await fetch(config.telemetry.port, "/health/ready")
        assert status == 503
        assert "invalid_grant" in json.dumps(body)
        # No MQTT connection is attempted without a usable token.
        assert collector.receiver is None or not collector.receiver.ever_connected
    finally:
        await shutdown(collector, task)


async def test_authorization_recovers_without_a_restart(tmp_path: Path) -> None:
    from bmw_collector.auth import AuthError

    config = make_config(tmp_path, port=PORT + 4)
    # Fail the first attempt only, as a BMW-side outage would.
    auth = StubAuth(fail_until=1, error=AuthError("server_error", "BMW outage"))
    collector, task = await start_collector(config, broker=FakeBroker(), auth=auth)
    try:
        # The supervisor retries on its own; a transient failure must not need
        # an operator or a container restart.
        await wait_for(lambda: ready_now(collector), timeout=20)
        assert auth.refreshes >= 2
        assert collector.health.phase in {Phase.RUNNING, Phase.DEGRADED}
    finally:
        assert await shutdown(collector, task) == 0


async def test_a_storage_failure_makes_it_not_ready(tmp_path: Path) -> None:
    from tests.unit.test_store import FullDiskConnection

    config = make_config(tmp_path, port=PORT + 5)
    collector, task = await start_collector(config, broker=FakeBroker())
    try:
        await wait_for(lambda: ready_now(collector))
        collector.store._conn = FullDiskConnection()  # type: ignore[assignment]
        await wait_for(lambda: not ready_now(collector), timeout=30)
        status, body = await fetch(config.telemetry.port, "/health/ready")
        assert status == 503
        assert "not writable" in body["last_error"]
        assert collector.health.phase is Phase.DEGRADED
    finally:
        # Restore a working connection so shutdown can checkpoint and close.
        collector.store.open_sync()
        await shutdown(collector, task)


async def test_mqtt_permission_failure_is_reported_not_retried_tightly(
    tmp_path: Path,
) -> None:
    import aiomqtt

    config = make_config(tmp_path, port=PORT + 6)
    broker = FakeBroker(fail_subscribe=aiomqtt.MqttError("not authorized"))
    collector, task = await start_collector(config, broker=broker)
    try:
        await wait_for(lambda: collector.receiver is not None and collector.receiver.auth_blocked)
        attempts = broker.connections
        await asyncio.sleep(0.4)
        status, body = await fetch(config.telemetry.port, "/health/ready")
        assert status == 503
        assert "MQTT streaming is not authorized" in body["last_error"]
        # The configured 30s backoff must hold: no reconnect storm.
        assert broker.connections == attempts
    finally:
        await shutdown(collector, task)


async def test_mqtt_disconnection_is_degraded_not_unready(tmp_path: Path) -> None:
    config = make_config(tmp_path, port=PORT + 7)
    broker = FakeBroker()
    collector, task = await start_collector(config, broker=broker)
    try:
        await wait_for(lambda: ready_now(collector))
        assert collector.receiver is not None
        # Simulate a dropped connection after a healthy one.
        collector.receiver.connected = False
        await collector._evaluate_readiness()
        # Still ready: the collector can archive whatever arrives next, and a
        # brief reconnect is normal. The phase records the degradation.
        assert collector.health.ready is True
        assert collector.health.phase is Phase.DEGRADED
    finally:
        await shutdown(collector, task)


# -- lifecycle ---------------------------------------------------------------
async def test_a_second_instance_is_refused(tmp_path: Path) -> None:
    config = make_config(tmp_path, port=PORT + 8)
    holder = StandaloneLeader(config.storage.resolved_lock_path())
    holder.acquire_sync()
    try:
        collector = Collector(config)
        assert await collector.run() == 1
        assert collector.health.phase is Phase.FATAL
        assert "one streaming connection" in (collector.health.last_error or "")
    finally:
        holder.release_sync()


async def test_shutdown_drains_queued_events(tmp_path: Path) -> None:
    config = make_config(tmp_path, port=PORT + 9)
    messages = [
        FakeMessage(f"{GCID}/{VALID_VIN}", stream_payload(VALID_VIN, index=i)) for i in range(200)
    ]
    broker = FakeBroker(messages_to_send=messages)
    collector, task = await start_collector(config, broker=broker)
    try:
        await wait_for(
            lambda: collector.receiver is not None and collector.receiver.messages_received >= 200
        )
    finally:
        await shutdown(collector, task)

    store = EventStore(config.storage.sqlite_path)
    store.open_sync()
    try:
        # Everything accepted before the signal must be committed, not dropped.
        assert store.count_events_sync() == collector.writer.committed
        assert collector.writer.committed >= 200
        assert collector.writer.dropped == 0
    finally:
        store.close_sync()


async def test_the_lock_is_released_on_shutdown(tmp_path: Path) -> None:
    config = make_config(tmp_path, port=PORT + 10)
    collector, task = await start_collector(config, broker=FakeBroker())
    await wait_for(lambda: ready_now(collector))
    await shutdown(collector, task)
    # A restart must not be locked out by the previous run.
    second = StandaloneLeader(config.storage.resolved_lock_path())
    second.acquire_sync()
    second.release_sync()


async def test_existing_events_survive_a_restart(tmp_path: Path) -> None:
    config = make_config(tmp_path, port=PORT + 11)
    store = EventStore(config.storage.sqlite_path)
    store.open_sync()
    store.append_events_sync(
        [RawEvent(source="rest", account_key="lifecycle", payload=b'{"pre-existing":true}')]
    )
    store.close_sync()

    broker = FakeBroker(
        messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", stream_payload(VALID_VIN))]
    )
    collector, task = await start_collector(config, broker=broker)
    try:
        await wait_for(lambda: collector.writer.committed >= 1)
    finally:
        await shutdown(collector, task)

    store = EventStore(config.storage.sqlite_path)
    store.open_sync()
    try:
        # Nothing prunes or rewrites history on startup.
        assert store.count_events_sync() == 2
    finally:
        store.close_sync()


async def test_mqtt_disabled_still_reaches_ready(tmp_path: Path) -> None:
    config = make_config(tmp_path, port=PORT + 12, mqtt=False, rest=False)
    collector, task = await start_collector(config)
    try:
        await wait_for(lambda: ready_now(collector))
        assert collector.receiver is None
    finally:
        assert await shutdown(collector, task) == 0


@pytest.mark.parametrize("endpoint", ["/", "/health/live", "/health/ready", "/metrics"])
async def test_every_endpoint_answers(tmp_path: Path, endpoint: str) -> None:
    config = make_config(tmp_path, port=PORT + 13)
    collector, task = await start_collector(config, broker=FakeBroker())
    try:
        await wait_for(lambda: ready_now(collector))
        status, _body = await fetch(config.telemetry.port, endpoint)
        assert status == 200
    finally:
        await shutdown(collector, task)
