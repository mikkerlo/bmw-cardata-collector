"""Shared fixtures. No test in this suite needs BMW credentials or a network."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from bmw_collector.config import Config, load_config
from bmw_collector.store import EventStore

VALID_VIN = "WBA12345678901234"
OTHER_VIN = "WBA99999999999999"


@pytest.fixture
def store_path(tmp_path: Path) -> Path:
    """Path for a throwaway SQLite database."""
    return tmp_path / "collector.sqlite3"


@pytest.fixture
def open_store(store_path: Path) -> Iterator[EventStore]:
    """An opened event store, closed on teardown."""
    store = EventStore(store_path)
    store.open_sync()
    yield store
    store.close_sync()


@pytest.fixture
async def async_store(store_path: Path) -> AsyncIterator[EventStore]:
    """An opened event store for async tests."""
    store = EventStore(store_path)
    await store.open()
    yield store
    await store.close()


def write_config(
    tmp_path: Path,
    *,
    vins: list[str] | None = None,
    mode: str = "allowlist",
    mqtt_enabled: bool = True,
    rest_enabled: bool = True,
    extra_bmw: str = "",
    tasks: str = "      - type: vehicle_mapping\n        interval_hours: 24\n",
    budget: int = 35,
) -> Path:
    """Write a minimal valid configuration file and return its path."""
    vin_lines = "".join(f"      - {v}\n" for v in (vins if vins is not None else [VALID_VIN]))
    text = f"""
node:
  name: test-node
storage:
  sqlite_path: {tmp_path / "collector.sqlite3"}
  synchronous: FULL
  queue_max: 100
bmw:
  client_id: test-client-id
  credentials_file: {tmp_path / "secrets" / "oauth.json"}
  account_key: test
{extra_bmw}  vehicles:
    mode: {mode}
    vins:
{vin_lines}  mqtt:
    enabled: {"true" if mqtt_enabled else "false"}
  rest:
    enabled: {"true" if rest_enabled else "false"}
    requests_per_day_budget: {budget}
    tasks:
{tasks}telemetry:
  listen_host: 127.0.0.1
  port: 19090
  log_level: DEBUG
"""
    path = tmp_path / "config.yaml"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def config(tmp_path: Path) -> Config:
    """A valid parsed configuration pointing at tmp_path."""
    return load_config(write_config(tmp_path))


# -- MQTT test doubles -------------------------------------------------------
@dataclass
class FakeMessage:
    """Stand-in for :class:`aiomqtt.Message`."""

    topic: str
    payload: bytes
    qos: int = 0
    retain: bool = False


@dataclass
class FakeBroker:
    """Scripted aiomqtt client, standing in for BMW's broker.

    ``messages_to_send`` is delivered once per connection. ``fail_subscribe``
    and ``connect_error`` let a test drive the failure paths without touching
    the network.
    """

    messages_to_send: list[FakeMessage] = field(default_factory=list)
    connect_error: Exception | None = None
    fail_subscribe: Exception | None = None
    #: SUBACK reason codes to answer with. 0-2 grant the QoS; anything higher
    #: is a refusal, which the real broker reports this way rather than by
    #: raising.
    suback_codes: tuple[int, ...] = (0,)
    end_stream_after: int | None = None
    connections: int = 0
    subscriptions: list[tuple[str, int]] = field(default_factory=list)
    credentials: list[tuple[str, str]] = field(default_factory=list)
    kwargs: list[dict[str, Any]] = field(default_factory=list)
    stream_finished: asyncio.Event = field(default_factory=asyncio.Event)

    def __call__(self, **kwargs: Any) -> FakeClient:
        """Mimic the ``aiomqtt.Client(...)`` constructor."""
        self.kwargs.append(kwargs)
        self.credentials.append((kwargs.get("username", ""), kwargs.get("password", "")))
        return FakeClient(self)


class FakeClient:
    """One scripted connection."""

    def __init__(self, broker: FakeBroker) -> None:
        self._broker = broker

    async def __aenter__(self) -> FakeClient:
        if self._broker.connect_error is not None:
            raise self._broker.connect_error
        self._broker.connections += 1
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def subscribe(self, topic: str, qos: int = 0) -> tuple[int, ...]:
        """Record the subscription and answer with SUBACK codes."""
        if self._broker.fail_subscribe is not None:
            raise self._broker.fail_subscribe
        self._broker.subscriptions.append((topic, qos))
        return self._broker.suback_codes

    @property
    def messages(self) -> AsyncIterator[FakeMessage]:
        """Yield the scripted messages, then idle until cancelled."""
        return self._iter_messages()

    async def _iter_messages(self) -> AsyncIterator[FakeMessage]:
        for message in self._broker.messages_to_send:
            yield message
        self._broker.stream_finished.set()
        # Stay open like a real broker would, instead of ending the stream.
        # Finish on cancellation rather than staying suspended, so the
        # generator can be closed when the loop shuts down.
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            return


def stream_payload(vin: str = VALID_VIN, *, index: int = 0, timestamp: str | None = None) -> bytes:
    """A payload shaped like a BMW streaming message."""
    return json.dumps(
        {
            "vin": vin,
            "entityId": f"entity-{index}",
            "timestamp": timestamp or "2026-10-18T12:00:00.000Z",
            "data": {
                "vehicle.powertrain.electric.battery.stateOfCharge": {
                    "value": 55 + index,
                    "unit": "percent",
                    "timestamp": "2026-10-18T12:00:00.000Z",
                }
            },
        }
    ).encode()
