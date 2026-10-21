"""Durable storage: byte fidelity, ordering, restart, backup, failure modes."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from bmw_collector.store import (
    EventStore,
    RawEvent,
    StorageError,
    StorageFullError,
)

# Bytes that are not valid UTF-8 and not valid JSON: the archive must not care.
BINARY_PAYLOAD = b"\x00\x01\x02\xff\xfe{not json}\x80\x81"


def make_event(**kwargs: Any) -> RawEvent:
    """A minimal event, overridable per test."""
    defaults: dict[str, Any] = {
        "source": "mqtt",
        "account_key": "test",
        "payload": b'{"a":1}',
    }
    return RawEvent(**{**defaults, **kwargs})


def test_migrations_create_the_schema_and_record_versions(open_store: EventStore) -> None:
    assert open_store.schema_version_sync() == 2
    tables = {
        row[0]
        for row in open_store._require().execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert {"raw_events", "collector_state", "collector_incidents", "vehicles"} <= tables


def test_migrations_are_idempotent_across_reopen(store_path: Path) -> None:
    first = EventStore(store_path)
    first.open_sync()
    first.append_events_sync([make_event()])
    first.close_sync()

    second = EventStore(store_path)
    second.open_sync()
    assert second.schema_version_sync() == 2
    assert second.count_events_sync() == 1
    second.close_sync()


def test_wal_mode_and_synchronous_are_applied(open_store: EventStore) -> None:
    conn = open_store._require()
    assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    # synchronous=FULL is 2 in SQLite's numbering.
    assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2


def test_payload_is_stored_byte_for_byte(open_store: EventStore) -> None:
    open_store.append_events_sync([make_event(payload=BINARY_PAYLOAD)])
    stored = next(iter(open_store.iter_events_sync()))
    assert stored.payload == BINARY_PAYLOAD
    assert isinstance(stored.payload, bytes)


def test_empty_payload_is_preserved(open_store: EventStore) -> None:
    open_store.append_events_sync([make_event(payload=b"")])
    assert next(iter(open_store.iter_events_sync())).payload == b""


def test_identical_payloads_are_stored_twice(open_store: EventStore) -> None:
    # The same value reported twice is two real events, not a duplicate.
    payload = b'{"soc": 55}'
    open_store.append_events_sync([make_event(payload=payload), make_event(payload=payload)])
    assert open_store.count_events_sync() == 2
    uuids = {e.event_uuid for e in open_store.iter_events_sync()}
    assert len(uuids) == 2


def test_events_read_back_in_insertion_order(open_store: EventStore) -> None:
    for index in range(20):
        open_store.append_events_sync([make_event(payload=str(index).encode())])
    order = [int(e.payload) for e in open_store.iter_events_sync()]
    assert order == list(range(20))


def test_metadata_round_trips(open_store: EventStore) -> None:
    metadata = {"http": {"status": 200, "url": "https://example.invalid/x"}, "task": "basic_data"}
    open_store.append_events_sync([make_event(source="rest", metadata=metadata)])
    assert next(iter(open_store.iter_events_sync())).metadata == metadata


def test_filters_narrow_the_result_set(open_store: EventStore) -> None:
    open_store.append_events_sync(
        [
            make_event(vin="AAA", received_at_utc="2026-01-01T00:00:00+00:00"),
            make_event(vin="BBB", received_at_utc="2026-02-01T00:00:00+00:00"),
            make_event(source="rest", vin="AAA", received_at_utc="2026-03-01T00:00:00+00:00"),
        ]
    )
    assert len(list(open_store.iter_events_sync(vin="AAA"))) == 2
    assert len(list(open_store.iter_events_sync(source="rest"))) == 1
    assert len(list(open_store.iter_events_sync(since="2026-02-01T00:00:00+00:00"))) == 2
    assert len(list(open_store.iter_events_sync(until="2026-02-01T00:00:00+00:00"))) == 1
    assert len(list(open_store.iter_events_sync(limit=1))) == 1


def test_after_id_resumes_an_export(open_store: EventStore) -> None:
    open_store.append_events_sync([make_event(payload=b"1"), make_event(payload=b"2")])
    first = next(iter(open_store.iter_events_sync()))
    remaining = list(open_store.iter_events_sync(after_id=first.id))
    assert [e.payload for e in remaining] == [b"2"]


def test_duplicate_event_uuid_is_rejected(open_store: EventStore) -> None:
    event = make_event()
    open_store.append_events_sync([event])
    with pytest.raises(StorageError):
        open_store.append_events_sync([make_event(event_uuid=event.event_uuid)])


def test_a_rejected_batch_commits_nothing(open_store: EventStore) -> None:
    existing = make_event()
    open_store.append_events_sync([existing])
    batch = [make_event(payload=b"new"), make_event(event_uuid=existing.event_uuid)]
    with pytest.raises(StorageError):
        open_store.append_events_sync(batch)
    # The good event in the failed batch must not be half-committed.
    assert open_store.count_events_sync() == 1


def test_invalid_source_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="source must be"):
        RawEvent(source="carrier-pigeon", account_key="t", payload=b"x")


def test_non_bytes_payload_is_rejected() -> None:
    with pytest.raises(TypeError, match="payload must be bytes"):
        RawEvent(source="mqtt", account_key="t", payload="a string")  # type: ignore[arg-type]


def test_state_round_trips_and_overwrites(open_store: EventStore) -> None:
    assert open_store.get_state_sync("absent", "fallback") == "fallback"
    open_store.set_state_sync("cursor", {"used": 1})
    open_store.set_state_sync("cursor", {"used": 2})
    assert open_store.get_state_sync("cursor") == {"used": 2}


def test_state_survives_reopen(store_path: Path) -> None:
    first = EventStore(store_path)
    first.open_sync()
    first.set_state_sync("rest_quota", {"day": "2026-10-18", "used": 7})
    first.close_sync()
    second = EventStore(store_path)
    second.open_sync()
    assert second.get_state_sync("rest_quota")["used"] == 7
    second.close_sync()


def test_incidents_are_recorded_newest_first_and_redacted(open_store: EventStore) -> None:
    open_store.record_incident_sync("first", {"n": 1})
    open_store.record_incident_sync("second", {"access_token": "super-secret-value"})
    incidents = open_store.recent_incidents_sync()
    assert [i["kind"] for i in incidents] == ["second", "first"]
    assert incidents[0]["details"]["access_token"] == "***redacted***"


def test_vehicle_upsert_preserves_first_observed(open_store: EventStore) -> None:
    open_store.upsert_vehicle_sync("VIN1", account_key="test", mapping_type="PRIMARY")
    first = open_store.list_vehicles_sync()[0]["first_observed_at_utc"]
    open_store.upsert_vehicle_sync("VIN1", account_key="test")
    row = open_store.list_vehicles_sync()[0]
    assert row["first_observed_at_utc"] == first
    # A later upsert without a mapping type must not erase the known one.
    assert row["mapping_type"] == "PRIMARY"
    assert row["collection_enabled"] is True


def test_disabling_a_vehicle_keeps_its_row_and_events(open_store: EventStore) -> None:
    open_store.upsert_vehicle_sync("VIN1", account_key="test")
    open_store.append_events_sync([make_event(vin="VIN1")])
    open_store.upsert_vehicle_sync("VIN1", account_key="test", collection_enabled=False)
    row = open_store.list_vehicles_sync()[0]
    assert row["collection_enabled"] is False
    assert open_store.count_events_sync() == 1


def test_integrity_check_passes_on_a_healthy_database(open_store: EventStore) -> None:
    assert open_store.integrity_check_sync() == "ok"


def test_event_stats_summarise_both_sources(open_store: EventStore) -> None:
    open_store.append_events_sync([make_event(), make_event(source="rest")])
    stats = open_store.event_stats_sync()
    assert stats["total"] == 2
    assert stats["by_source"] == {"mqtt": 1, "rest": 1}


def test_backup_produces_a_standalone_restorable_file(
    open_store: EventStore, tmp_path: Path
) -> None:
    open_store.append_events_sync([make_event(payload=BINARY_PAYLOAD)])
    # Deliberately do not checkpoint: the newest commit may live in the -wal
    # sidecar, which is exactly the case a naive file copy gets wrong.
    destination = open_store.backup_sync(tmp_path / "backup" / "snap.sqlite3")
    assert destination.exists()
    assert destination.stat().st_mode & 0o777 == 0o600

    restored = EventStore(destination)
    restored.open_sync()
    try:
        assert restored.integrity_check_sync() == "ok"
        assert next(iter(restored.iter_events_sync())).payload == BINARY_PAYLOAD
    finally:
        restored.close_sync()


def test_checkpoint_modes_are_validated(open_store: EventStore) -> None:
    open_store.checkpoint_sync("TRUNCATE")
    with pytest.raises(ValueError, match="unsupported checkpoint mode"):
        open_store.checkpoint_sync("SOMETIMES")


def test_database_file_is_owner_only(open_store: EventStore, store_path: Path) -> None:
    assert store_path.stat().st_mode & 0o077 == 0


def test_writable_probe_reports_true_on_a_healthy_store(open_store: EventStore) -> None:
    assert open_store.writable_sync() is True


class FullDiskConnection:
    """Connection stand-in that fails every write the way a full disk does."""

    def __init__(self, message: str = "database or disk is full") -> None:
        self._message = message

    def execute(self, sql: str, *_args: Any) -> None:
        """Allow transaction control, fail anything that would write."""
        if sql.split(maxsplit=1)[0].upper() in {"BEGIN", "COMMIT", "ROLLBACK", "PRAGMA"}:
            return None
        raise sqlite3.OperationalError(self._message)

    def executemany(self, *_args: Any) -> None:
        """Always fail."""
        raise sqlite3.OperationalError(self._message)

    def close(self) -> None:
        """Closing a dead connection is a no-op."""
        return None


def test_disk_full_is_reported_as_a_distinct_error(
    open_store: EventStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    # StorageFullError is what makes the collector report a fatal, not-ready
    # state instead of retrying forever against a full volume.
    monkeypatch.setattr(open_store, "_conn", FullDiskConnection())
    with pytest.raises(StorageFullError):
        open_store.append_events_sync([make_event()])


@pytest.mark.parametrize("message", ["disk I/O error", "database or disk is full"])
def test_writable_probe_reports_false_when_writes_fail(
    open_store: EventStore, monkeypatch: pytest.MonkeyPatch, message: str
) -> None:
    monkeypatch.setattr(open_store, "_conn", FullDiskConnection(message))
    assert open_store.writable_sync() is False


def test_using_a_closed_store_raises(store_path: Path) -> None:
    store = EventStore(store_path)
    with pytest.raises(StorageError, match="not open"):
        store.count_events_sync()


def test_import_is_idempotent_by_event_uuid(open_store: EventStore) -> None:
    events = [make_event(payload=b"a"), make_event(payload=b"b")]
    assert open_store.import_events_sync(events) == (2, 0)
    assert open_store.import_events_sync(events) == (0, 2)
    assert open_store.count_events_sync() == 2


async def test_async_api_commits_and_counts(async_store: EventStore) -> None:
    assert await async_store.append_event(make_event()) == 1
    assert await async_store.count_events() == 1
    await async_store.set_state("k", 1)
    assert await async_store.get_state("k") == 1
    await async_store.record_incident("kind", {"detail": True})
    await async_store.upsert_vehicle("VIN1", account_key="test")
    assert len(await async_store.list_vehicles()) == 1
    assert await async_store.writable() is True
    await async_store.checkpoint("PASSIVE")
