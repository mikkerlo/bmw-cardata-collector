"""Management endpoints and metric hygiene."""

from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from bmw_collector import metrics
from bmw_collector.telemetry import HealthState, Phase, TelemetryServer, build_app

VIN = "WBA12345678901234"
GCID = "aa111192-d570-4d3d-8835-656ee00edcd7"


@pytest.fixture
def health() -> HealthState:
    """A fresh health state."""
    return HealthState(node_name="test-node", version="0.0.0-test")


@pytest.fixture
async def client(health: HealthState) -> TestClient:
    """An aiohttp test client over the management app."""
    test_client = TestClient(TestServer(build_app(health)))
    await test_client.start_server()
    yield test_client
    await test_client.close()


async def test_liveness_is_independent_of_bmw_traffic(
    client: TestClient, health: HealthState
) -> None:
    # Not ready, nothing received, no BMW connection: still alive. A parked car
    # sends nothing for days and that must not restart the container.
    health.set_ready(False)
    health.set_phase(Phase.AWAITING_AUTHORIZATION)
    response = await client.get("/health/live")
    assert response.status == 200
    assert (await response.json())["status"] == "alive"


async def test_readiness_is_503_until_ready(client: TestClient, health: HealthState) -> None:
    response = await client.get("/health/ready")
    assert response.status == 503
    body = await response.json()
    assert body["ready"] is False
    assert body["phase"] == "starting"

    health.set_ready(True)
    health.set_phase(Phase.RUNNING)
    response = await client.get("/health/ready")
    assert response.status == 200
    assert (await response.json())["ready"] is True


async def test_readiness_reports_the_phase_and_last_error(
    client: TestClient, health: HealthState
) -> None:
    health.set_phase(Phase.AWAITING_AUTHORIZATION, error="run 'collector auth login'")
    body = await (await client.get("/health/ready")).json()
    assert body["phase"] == "awaiting_authorization"
    assert "auth login" in body["last_error"]
    assert body["node"] == "test-node"
    assert body["version"] == "0.0.0-test"
    assert body["uptime_seconds"] >= 0


async def test_readiness_includes_component_status(client: TestClient, health: HealthState) -> None:
    health.register("writer", lambda: {"queue_depth": 3, "committed": 17})
    body = await (await client.get("/health/ready")).json()
    assert body["components"]["writer"] == {"queue_depth": 3, "committed": 17}


async def test_a_broken_status_provider_cannot_break_the_probe(
    client: TestClient, health: HealthState
) -> None:
    def explode() -> dict[str, object]:
        raise RuntimeError("provider is broken")

    health.register("broken", explode)
    response = await client.get("/health/ready")
    # A probe that 500s on a bug in one provider would look like an outage.
    assert response.status == 503
    assert "RuntimeError" in (await response.json())["components"]["broken"]["error"]


async def test_metrics_are_served_in_prometheus_format(client: TestClient) -> None:
    response = await client.get("/metrics")
    assert response.status == 200
    assert response.content_type.startswith("text/plain")
    body = await response.text()
    assert "collector_start_timestamp_seconds" in body
    assert "bmw_mqtt_connected" in body


async def test_metrics_carry_no_vin_or_gcid(client: TestClient, health: HealthState) -> None:
    metrics.collector_events_committed_total.labels(source="mqtt").inc()
    metrics.bmw_rest_requests_total.labels(status_class="2xx").inc()
    metrics.collector_incidents_total.labels(kind="queue_overflow").inc()
    body = await (await client.get("/metrics")).text()
    # Label values reach dashboards, alert payloads and remote-write storage.
    assert VIN not in body
    assert GCID not in body
    assert 'source="mqtt"' in body


async def test_root_lists_the_endpoints(client: TestClient) -> None:
    body = await (await client.get("/")).json()
    assert set(body["endpoints"]) == {"/health/live", "/health/ready", "/metrics"}


def test_startup_duration_is_recorded_once(health: HealthState) -> None:
    health.set_ready(True)
    first = health.ready_since
    health.set_ready(False)
    health.set_ready(True)
    # Measures startup, not the most recent recovery.
    assert health.ready_since != first or first is not None


def test_phase_changes_update_the_metric(health: HealthState) -> None:
    health.set_phase(Phase.RUNNING)
    assert health.phase is Phase.RUNNING
    snapshot = health.snapshot()
    assert snapshot["phase"] == "running"


async def test_server_binds_and_stops(health: HealthState) -> None:
    server = TelemetryServer(health, host="127.0.0.1", port=0)
    await server.start()
    await server.stop()
    # Stopping twice must not raise during a messy shutdown.
    await server.stop()


def test_becoming_ready_clears_the_stale_reason(health: HealthState) -> None:
    health.set_ready(False, error="MQTT has not connected yet")
    assert health.last_error == "MQTT has not connected yet"
    health.set_ready(True)
    # A healthy service reporting a stale fault misleads whoever reads it.
    assert health.last_error is None
    assert health.snapshot()["last_error"] is None


def test_a_new_failure_replaces_the_previous_one(health: HealthState) -> None:
    health.set_ready(False, error="first reason")
    health.set_ready(False, error="second reason")
    assert health.last_error == "second reason"


def test_readiness_evaluator_is_used_on_refresh(health: HealthState) -> None:
    answers = iter([(False, "not yet"), (True, None)])
    health.set_readiness_evaluator(lambda: next(answers))
    health.refresh()
    assert health.ready is False
    assert health.last_error == "not yet"
    health.refresh()
    assert health.ready is True
    assert health.last_error is None


def test_a_broken_evaluator_fails_closed(health: HealthState) -> None:
    def explode() -> tuple[bool, str | None]:
        raise RuntimeError("evaluator bug")

    health.set_ready(True)
    health.set_readiness_evaluator(explode)
    health.refresh()
    # A bug in the check must not report a broken collector as ready.
    assert health.ready is False
    assert "readiness check failed" in (health.last_error or "")
