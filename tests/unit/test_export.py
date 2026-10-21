"""JSONL export and deterministic re-import."""

from __future__ import annotations

import base64
import io
import json
from pathlib import Path

import pytest

from bmw_collector.export import (
    EXPORT_FORMAT,
    export_events,
    export_to_path,
    import_events,
    import_from_path,
    record_to_event,
)
from bmw_collector.store import EventStore, RawEvent

BINARY_PAYLOAD = b"\x00\x01\xff\xfe{not json}\x80"


def seed(store: EventStore) -> list[RawEvent]:
    """Write a representative mix of events."""
    events = [
        RawEvent(
            source="mqtt",
            account_key="test",
            payload=BINARY_PAYLOAD,
            vin="WBA00000000000001",
            topic="gcid/WBA00000000000001",
            mqtt_qos=0,
            event_time_utc="2026-10-18T12:00:00.000Z",
            metadata={"mqtt": {"entity_id": "e1", "retain": False}},
        ),
        RawEvent(
            source="rest",
            account_key="test",
            payload=b'{"model":"i4"}',
            vin="WBA00000000000001",
            metadata={"http": {"status": 200, "method": "GET"}, "task": "basic_data"},
        ),
        RawEvent(source="mqtt", account_key="test", payload=b"", topic="gcid/+"),
    ]
    store.append_events_sync(events)
    return events


def test_export_writes_one_json_object_per_line(open_store: EventStore) -> None:
    seed(open_store)
    buffer = io.StringIO()
    assert export_events(open_store, buffer) == 3
    lines = buffer.getvalue().splitlines()
    assert len(lines) == 3
    assert all(json.loads(line)["format"] == EXPORT_FORMAT for line in lines)


def test_export_preserves_binary_payloads(open_store: EventStore) -> None:
    seed(open_store)
    buffer = io.StringIO()
    export_events(open_store, buffer)
    first = json.loads(buffer.getvalue().splitlines()[0])
    assert base64.b64decode(first["payload_b64"]) == BINARY_PAYLOAD


def test_round_trip_into_a_second_database_loses_nothing(
    open_store: EventStore, tmp_path: Path
) -> None:
    seed(open_store)
    buffer = io.StringIO()
    export_events(open_store, buffer)
    original = list(open_store.iter_events_sync())

    target = EventStore(tmp_path / "replica.sqlite3")
    target.open_sync()
    try:
        assert import_events(target, io.StringIO(buffer.getvalue())) == (3, 0)
        replayed = list(target.iter_events_sync())
    finally:
        target.close_sync()

    assert len(replayed) == len(original)
    for before, after in zip(original, replayed, strict=True):
        # The UUID carries across so a later replay is idempotent, and every
        # field a future consumer might need survives the trip.
        assert after.event_uuid == before.event_uuid
        assert after.payload == before.payload
        assert after.received_at_utc == before.received_at_utc
        assert after.source == before.source
        assert after.account_key == before.account_key
        assert after.vin == before.vin
        assert after.topic == before.topic
        assert after.mqtt_qos == before.mqtt_qos
        assert after.event_time_utc == before.event_time_utc
        assert after.metadata == before.metadata


def test_re_importing_the_same_file_inserts_nothing_new(
    open_store: EventStore, tmp_path: Path
) -> None:
    seed(open_store)
    path = tmp_path / "events.jsonl"
    export_to_path(open_store, path)

    target = EventStore(tmp_path / "replica.sqlite3")
    target.open_sync()
    try:
        assert import_from_path(target, path) == (3, 0)
        assert import_from_path(target, path) == (0, 3)
        assert target.count_events_sync() == 3
    finally:
        target.close_sync()


def test_import_is_idempotent_when_a_file_overlaps_existing_rows(
    open_store: EventStore, tmp_path: Path
) -> None:
    seed(open_store)
    path = tmp_path / "events.jsonl"
    export_to_path(open_store, path)
    # Importing a collector's own export back into itself must be a no-op.
    assert import_from_path(open_store, path) == (0, 3)
    assert open_store.count_events_sync() == 3


def test_export_is_deterministic(open_store: EventStore, tmp_path: Path) -> None:
    seed(open_store)
    first = tmp_path / "a.jsonl"
    second = tmp_path / "b.jsonl"
    export_to_path(open_store, first)
    export_to_path(open_store, second)
    assert first.read_bytes() == second.read_bytes()


def test_export_filters_are_applied(open_store: EventStore) -> None:
    seed(open_store)
    buffer = io.StringIO()
    assert export_events(open_store, buffer, source="rest") == 1
    assert json.loads(buffer.getvalue())["source"] == "rest"


def test_export_can_resume_after_a_row_id(open_store: EventStore) -> None:
    seed(open_store)
    first = next(iter(open_store.iter_events_sync()))
    buffer = io.StringIO()
    assert export_events(open_store, buffer, after_id=first.id) == 2


def test_export_file_is_owner_only(open_store: EventStore, tmp_path: Path) -> None:
    seed(open_store)
    path = tmp_path / "events.jsonl"
    export_to_path(open_store, path)
    # An export is raw vehicle telemetry, positions included.
    assert path.stat().st_mode & 0o077 == 0


def test_import_rejects_an_unknown_format() -> None:
    with pytest.raises(ValueError, match="unsupported export format"):
        record_to_event({"format": "something-else/9"})


def test_import_names_the_missing_fields() -> None:
    with pytest.raises(ValueError, match="missing field"):
        record_to_event({"format": EXPORT_FORMAT, "event_uuid": "x"})


def test_import_reports_the_offending_line_number(open_store: EventStore) -> None:
    good = json.dumps(
        {
            "format": EXPORT_FORMAT,
            "event_uuid": "a",
            "received_at_utc": "2026-10-18T00:00:00+00:00",
            "source": "mqtt",
            "account_key": "k",
            "payload_b64": "",
        }
    )
    text = f"{good}\nnot json\n"
    with pytest.raises(ValueError, match="line 2"):
        import_events(open_store, io.StringIO(text))


def test_import_skips_blank_lines(open_store: EventStore) -> None:
    buffer = io.StringIO()
    seed(open_store)
    export_events(open_store, buffer)
    padded = "\n\n".join(buffer.getvalue().splitlines()) + "\n\n"
    target_rows = import_events(open_store, io.StringIO(padded))
    assert target_rows == (0, 3)


def test_import_rejects_a_non_object_line(open_store: EventStore) -> None:
    with pytest.raises(ValueError, match="not a JSON object"):
        import_events(open_store, io.StringIO("[1,2,3]\n"))


def test_import_accepts_a_record_without_a_format_marker(open_store: EventStore) -> None:
    record = {
        "event_uuid": "manual-1",
        "received_at_utc": "2026-10-18T00:00:00+00:00",
        "source": "mqtt",
        "account_key": "test",
        "payload_b64": base64.b64encode(b"hand-written").decode(),
    }
    assert import_events(open_store, io.StringIO(json.dumps(record) + "\n")) == (1, 0)


def test_large_export_batches_correctly(open_store: EventStore, tmp_path: Path) -> None:
    open_store.append_events_sync(
        [RawEvent(source="mqtt", account_key="test", payload=str(i).encode()) for i in range(1200)]
    )
    path = tmp_path / "big.jsonl"
    assert export_to_path(open_store, path) == 1200

    target = EventStore(tmp_path / "replica.sqlite3")
    target.open_sync()
    try:
        # Crosses the importer's 500-record batch boundary.
        assert import_from_path(target, path) == (1200, 0)
    finally:
        target.close_sync()
