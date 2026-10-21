"""Read-only JSONL export and deterministic re-import of archived events.

The export is lossless. Payloads are opaque bytes, so they are carried as
base64 and the original ``event_uuid`` is preserved; re-importing the same file
twice inserts each event once, which makes an export safe to replay into a
second database (a future aggregation node, for instance) without inventing new
identities for events that already exist.

An export contains raw vehicle telemetry — positions among it, if the account
streams them. It is private data. Nothing here uploads anything anywhere.
"""

from __future__ import annotations

import base64
import json
import logging
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, TextIO

from .store import EventStore, RawEvent, StoredEvent

_LOGGER = logging.getLogger(__name__)

EXPORT_FORMAT = "bmw-cardata-collector/raw-events/1"


def event_to_record(event: StoredEvent) -> dict[str, Any]:
    """Convert a stored event into its JSONL record form."""
    return {
        "format": EXPORT_FORMAT,
        "event_uuid": event.event_uuid,
        "received_at_utc": event.received_at_utc,
        "source": event.source,
        "account_key": event.account_key,
        "vin": event.vin,
        "topic": event.topic,
        "mqtt_qos": event.mqtt_qos,
        "event_time_utc": event.event_time_utc,
        "payload_b64": base64.b64encode(event.payload).decode("ascii"),
        "metadata": event.metadata,
    }


def record_to_event(record: dict[str, Any]) -> RawEvent:
    """Convert a JSONL record back into an insertable event."""
    fmt = record.get("format")
    if fmt is not None and fmt != EXPORT_FORMAT:
        raise ValueError(f"unsupported export format {fmt!r}")
    missing = {"event_uuid", "received_at_utc", "source", "account_key", "payload_b64"} - set(
        record
    )
    if missing:
        raise ValueError(f"record is missing field(s): {', '.join(sorted(missing))}")
    return RawEvent(
        source=str(record["source"]),
        account_key=str(record["account_key"]),
        payload=base64.b64decode(record["payload_b64"]),
        received_at_utc=str(record["received_at_utc"]),
        event_uuid=str(record["event_uuid"]),
        vin=record.get("vin"),
        topic=record.get("topic"),
        mqtt_qos=record.get("mqtt_qos"),
        event_time_utc=record.get("event_time_utc"),
        metadata=record.get("metadata") or {},
    )


def export_events(
    store: EventStore,
    handle: TextIO,
    *,
    vin: str | None = None,
    source: str | None = None,
    since: str | None = None,
    until: str | None = None,
    after_id: int = 0,
    limit: int | None = None,
) -> int:
    """Write matching events to ``handle`` as JSONL. Returns the count."""
    written = 0
    for event in store.iter_events_sync(
        after_id=after_id, vin=vin, source=source, since=since, until=until, limit=limit
    ):
        handle.write(json.dumps(event_to_record(event), separators=(",", ":")))
        handle.write("\n")
        written += 1
    handle.flush()
    return written


def export_to_path(store: EventStore, path: str | Path, **kwargs: Any) -> int:
    """Export to a file, or to stdout when ``path`` is ``-``."""
    if str(path) == "-":
        return export_events(store, sys.stdout, **kwargs)
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as handle:
        count = export_events(store, handle, **kwargs)
    destination.chmod(0o600)
    return count


def read_records(handle: Iterable[str]) -> Iterator[dict[str, Any]]:
    """Parse JSONL, skipping blank lines and reporting the offending line."""
    for number, line in enumerate(handle, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            record = json.loads(stripped)
        except json.JSONDecodeError as err:
            raise ValueError(f"line {number} is not valid JSON: {err}") from err
        if not isinstance(record, dict):
            # A parse failure, not a caller type error, hence ValueError.
            raise ValueError(f"line {number} is not a JSON object")  # noqa: TRY004
        yield record


def import_events(
    store: EventStore, handle: Iterable[str], *, batch_size: int = 500
) -> tuple[int, int]:
    """Import JSONL records idempotently. Returns ``(inserted, skipped)``."""
    inserted = skipped = 0
    batch: list[RawEvent] = []
    for record in read_records(handle):
        batch.append(record_to_event(record))
        if len(batch) >= batch_size:
            added, dup = store.import_events_sync(batch)
            inserted += added
            skipped += dup
            batch.clear()
    if batch:
        added, dup = store.import_events_sync(batch)
        inserted += added
        skipped += dup
    return inserted, skipped


def import_from_path(store: EventStore, path: str | Path) -> tuple[int, int]:
    """Import from a file, or from stdin when ``path`` is ``-``."""
    if str(path) == "-":
        return import_events(store, sys.stdin)
    with Path(path).open("r", encoding="utf-8") as handle:
        return import_events(store, handle)
