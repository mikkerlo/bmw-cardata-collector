"""Ingest queue and the durable writer task."""

from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

import pytest

from bmw_collector.store import EventStore, RawEvent
from bmw_collector.writer import EventWriter
from tests.unit.test_store import FullDiskConnection


def make_event(payload: bytes = b'{"x":1}', source: str = "mqtt") -> RawEvent:
    """A minimal event."""
    return RawEvent(source=source, account_key="test", payload=payload)


async def drain(writer: EventWriter, *, expected: int, timeout: float = 5.0) -> None:
    """Wait until the writer reports the expected number of commits."""
    async with asyncio.timeout(timeout):
        while writer.committed < expected:
            await asyncio.sleep(0.01)


async def test_submitted_events_are_committed(async_store: EventStore) -> None:
    writer = EventWriter(async_store, queue_max=10)
    writer.start()
    assert writer.submit(make_event()) is True
    await drain(writer, expected=1)
    await writer.stop()
    assert await async_store.count_events() == 1


async def test_a_burst_is_batched_but_fully_committed(async_store: EventStore) -> None:
    writer = EventWriter(async_store, queue_max=1000)
    writer.start()
    for index in range(250):
        assert writer.submit(make_event(payload=str(index).encode())) is True
    await drain(writer, expected=250)
    await writer.stop()
    assert await async_store.count_events() == 250
    assert writer.dropped == 0


async def test_queue_overflow_drops_and_records_an_incident(async_store: EventStore) -> None:
    # The writer is never started, so nothing drains the queue.
    writer = EventWriter(async_store, queue_max=2)
    assert writer.submit(make_event()) is True
    assert writer.submit(make_event()) is True
    assert writer.submit(make_event()) is False
    assert writer.dropped == 1
    await asyncio.sleep(0.05)  # let the out-of-band incident write land
    incidents = await asyncio.to_thread(async_store.recent_incidents_sync)
    assert incidents[0]["kind"] == "queue_overflow"
    assert incidents[0]["details"]["dropped_total"] == 1


async def test_submit_never_blocks_even_when_saturated(async_store: EventStore) -> None:
    writer = EventWriter(async_store, queue_max=1)
    writer.submit(make_event())
    # Submitting from the broker read loop must return immediately rather than
    # waiting for room, which would stall the MQTT connection.
    async with asyncio.timeout(1.0):
        for _ in range(100):
            writer.submit(make_event())
    assert writer.dropped == 100
    await writer.stop()


async def test_sustained_overflow_coalesces_incident_rows(async_store: EventStore) -> None:
    writer = EventWriter(async_store, queue_max=1)
    writer.submit(make_event())
    for _ in range(50):
        writer.submit(make_event())
    await writer.stop()
    incidents = await asyncio.to_thread(async_store.recent_incidents_sync)
    overflow = [i for i in incidents if i["kind"] == "queue_overflow"]
    # One row for the burst, not fifty: writing fifty rows would add load to
    # the storage that is already the bottleneck.
    assert len(overflow) == 1
    assert writer.dropped == 50


async def test_failed_write_is_not_counted_as_committed(
    async_store: EventStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = EventWriter(async_store, queue_max=10)
    writer.start()
    monkeypatch.setattr(async_store, "_conn", FullDiskConnection())
    writer.submit(make_event())
    async with asyncio.timeout(5.0):
        while writer.write_failures == 0:
            await asyncio.sleep(0.01)
    await writer.stop()

    assert writer.committed == 0
    assert writer.storage_healthy is False
    assert writer.last_write_error is not None
    assert writer.status()["storage_healthy"] is False


async def test_storage_recovers_after_a_transient_failure(
    async_store: EventStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = EventWriter(async_store, queue_max=10)
    writer.start()
    real_conn = async_store._conn
    monkeypatch.setattr(async_store, "_conn", FullDiskConnection())
    writer.submit(make_event())
    async with asyncio.timeout(5.0):
        while writer.write_failures == 0:
            await asyncio.sleep(0.01)
    monkeypatch.setattr(async_store, "_conn", real_conn)
    writer.submit(make_event(payload=b"after-recovery"))
    await drain(writer, expected=1)
    await writer.stop()
    assert writer.storage_healthy is True


async def test_stop_drains_queued_events(async_store: EventStore) -> None:
    writer = EventWriter(async_store, queue_max=1000)
    writer.start()
    for index in range(100):
        writer.submit(make_event(payload=str(index).encode()))
    await writer.stop()
    # A clean shutdown must not discard what was already accepted.
    assert await async_store.count_events() == 100
    assert writer.queue_depth == 0


async def test_enqueue_waits_for_room_on_the_rest_path(async_store: EventStore) -> None:
    writer = EventWriter(async_store, queue_max=1)
    writer.start()
    # enqueue() may await: REST responses are too valuable to drop and the
    # scheduler is not in a latency-sensitive read loop.
    async with asyncio.timeout(5.0):
        for index in range(5):
            await writer.enqueue(make_event(payload=str(index).encode(), source="rest"))
    await writer.stop()
    assert await async_store.count_events() == 5


async def test_stop_is_safe_when_never_started(async_store: EventStore) -> None:
    await EventWriter(async_store).stop()


async def test_slow_storage_does_not_lose_events(async_store: EventStore) -> None:
    original = async_store.append_events

    async def slow(events: Any) -> int:
        await asyncio.sleep(0.02)
        return await original(events)

    async_store.append_events = slow  # type: ignore[method-assign]
    writer = EventWriter(async_store, queue_max=1000)
    writer.start()
    for index in range(120):
        writer.submit(make_event(payload=str(index).encode()))
    await writer.stop(drain_timeout=30.0)
    assert await async_store.count_events() == 120


def test_sqlite_errors_are_classified(async_store: EventStore) -> None:
    # Guards the marker list used to tell "disk full" from other failures.
    from bmw_collector.store import StorageFullError, _wrap_sqlite_error

    full = _wrap_sqlite_error(sqlite3.OperationalError("database or disk is full"), "x")
    other = _wrap_sqlite_error(sqlite3.OperationalError("syntax error"), "x")
    assert isinstance(full, StorageFullError)
    assert not isinstance(other, StorageFullError)
