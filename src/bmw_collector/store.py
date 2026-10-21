"""Durable SQLite event store.

Design notes
------------
* Every received payload is stored as the original bytes. Nothing is parsed on
  the write path, so an unrecognised descriptor is archived just as faithfully
  as a known one.
* SQLite runs in WAL mode with ``synchronous=FULL`` by default: each commit is
  fsynced, trading throughput for durability across power loss. The event rate
  from one BMW account is low enough that this costs nothing in practice.
* WAL is a transaction mechanism, not the archive. ``raw_events`` is the
  journal; the WAL file is checkpointed periodically so it does not grow
  without bound.
* All SQLite work is funnelled through one connection, serialized by an
  :class:`asyncio.Lock`, and executed in a worker thread so the event loop
  never blocks on fsync.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Any, Self

_LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION_KEY = "schema_version"


def utc_now_iso() -> str:
    """Current time as an ISO-8601 string in UTC, to the microsecond."""
    return datetime.now(tz=UTC).isoformat()


class StorageError(Exception):
    """Raised when durable storage cannot be read or written."""


class StorageFullError(StorageError):
    """Raised when SQLite reports the disk or database is full."""


_FULL_MARKERS = ("disk is full", "database or disk is full", "disk i/o error", "no space left")


def _wrap_sqlite_error(err: sqlite3.Error, what: str) -> StorageError:
    text = str(err).lower()
    if any(marker in text for marker in _FULL_MARKERS):
        return StorageFullError(f"{what}: {err}")
    return StorageError(f"{what}: {err}")


@dataclass(slots=True)
class RawEvent:
    """One archived payload plus the metadata needed to interpret it later."""

    source: str
    account_key: str
    payload: bytes
    received_at_utc: str = field(default_factory=utc_now_iso)
    event_uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    vin: str | None = None
    topic: str | None = None
    mqtt_qos: int | None = None
    event_time_utc: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.source not in {"mqtt", "rest"}:
            raise ValueError(f"source must be 'mqtt' or 'rest', got {self.source!r}")
        if not isinstance(self.payload, bytes | bytearray):
            raise TypeError("payload must be bytes; the raw path never re-encodes")
        self.payload = bytes(self.payload)


@dataclass(frozen=True, slots=True)
class StoredEvent:
    """A row read back out of ``raw_events``."""

    id: int
    event_uuid: str
    received_at_utc: str
    source: str
    account_key: str
    vin: str | None
    topic: str | None
    mqtt_qos: int | None
    event_time_utc: str | None
    payload: bytes
    metadata: dict[str, Any]


def _row_to_event(row: sqlite3.Row) -> StoredEvent:
    return StoredEvent(
        id=row["id"],
        event_uuid=row["event_uuid"],
        received_at_utc=row["received_at_utc"],
        source=row["source"],
        account_key=row["account_key"],
        vin=row["vin"],
        topic=row["topic"],
        mqtt_qos=row["mqtt_qos"],
        event_time_utc=row["event_time_utc"],
        payload=bytes(row["payload"]),
        metadata=json.loads(row["metadata_json"]),
    )


def _migration_scripts() -> list[tuple[int, str, str]]:
    """Return ``(version, name, sql)`` for bundled migrations, in order."""
    out: list[tuple[int, str, str]] = []
    package = resources.files(__package__).joinpath("migrations")
    for entry in sorted(p.name for p in package.iterdir() if p.name.endswith(".sql")):
        version = int(entry.split("_", 1)[0])
        out.append((version, entry, package.joinpath(entry).read_text(encoding="utf-8")))
    if [v for v, _, _ in out] != sorted(v for v, _, _ in out):
        raise StorageError("bundled migrations are not in ascending version order")
    return out


class EventStore:
    """Append-only raw event archive plus small key/value and incident tables."""

    def __init__(
        self,
        path: str | Path,
        *,
        synchronous: str = "FULL",
        busy_timeout_ms: int = 10_000,
        wal_checkpoint_pages: int = 2_000,
    ) -> None:
        self.path = Path(path)
        self._synchronous = synchronous.upper()
        self._busy_timeout_ms = busy_timeout_ms
        self._wal_checkpoint_pages = wal_checkpoint_pages
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    # -- lifecycle ----------------------------------------------------------
    def open_sync(self) -> None:
        """Open the database, apply pragmas and run pending migrations."""
        if self._conn is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            conn = sqlite3.connect(
                self.path, check_same_thread=False, isolation_level=None, timeout=30.0
            )
            conn.row_factory = sqlite3.Row
            conn.execute(f"PRAGMA busy_timeout={int(self._busy_timeout_ms)}")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(f"PRAGMA synchronous={self._synchronous}")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute(f"PRAGMA wal_autocheckpoint={int(self._wal_checkpoint_pages)}")
        except sqlite3.Error as err:
            raise _wrap_sqlite_error(err, f"cannot open {self.path}") from err
        self._conn = conn
        try:
            self._migrate(conn)
        except sqlite3.Error as err:
            raise _wrap_sqlite_error(err, "schema migration failed") from err
        # The database may hold credentials-adjacent metadata and private
        # telemetry: keep it owner-only.
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.path) + suffix)
            if candidate.exists():
                candidate.chmod(0o600)

    async def open(self) -> None:
        """Open the database in a worker thread."""
        await asyncio.to_thread(self.open_sync)

    def close_sync(self) -> None:
        """Checkpoint the WAL and close the connection."""
        conn, self._conn = self._conn, None
        if conn is None:
            return
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error as err:  # pragma: no cover - best effort on shutdown
            _LOGGER.warning("WAL checkpoint on close failed: %s", err)
        conn.close()

    async def close(self) -> None:
        """Close the database in a worker thread."""
        await asyncio.to_thread(self.close_sync)

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    def _require(self) -> sqlite3.Connection:
        if self._conn is None:
            raise StorageError("event store is not open")
        return self._conn

    async def _run(self, func: Any, *args: Any) -> Any:
        async with self._lock:
            return await asyncio.to_thread(func, *args)

    # -- migrations ---------------------------------------------------------
    def _migrate(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version INTEGER PRIMARY KEY,"
            " name TEXT NOT NULL,"
            " applied_at_utc TEXT NOT NULL)"
        )
        applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
        for version, name, sql in _migration_scripts():
            if version in applied:
                continue
            _LOGGER.info("applying migration %s", name, extra={"migration": name})
            # executescript() implicitly commits any pending transaction before
            # it runs, so BEGIN has to be inside the script itself. Python adds
            # no COMMIT of its own, so the transaction is still open afterwards
            # for the parameterized bookkeeping insert: the schema change and
            # its version row commit together, or neither does.
            try:
                conn.executescript("BEGIN IMMEDIATE;\n" + sql)
                conn.execute(
                    "INSERT INTO schema_migrations(version, name, applied_at_utc) VALUES (?, ?, ?)",
                    (version, name, utc_now_iso()),
                )
                conn.execute("COMMIT")
            except sqlite3.Error:
                with contextlib.suppress(sqlite3.Error):
                    conn.execute("ROLLBACK")
                raise

    def schema_version_sync(self) -> int:
        """Highest applied migration version, or 0 for an empty database."""
        conn = self._require()
        row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()
        return int(row[0])

    # -- raw events ---------------------------------------------------------
    def append_events_sync(self, events: Sequence[RawEvent]) -> int:
        """Commit a batch of events; returns the number of rows written.

        The caller may treat events as persisted only after this returns
        without raising: the COMMIT has been fsynced by then.
        """
        if not events:
            return 0
        conn = self._require()
        rows = [
            (
                e.event_uuid,
                e.received_at_utc,
                e.source,
                e.account_key,
                e.vin,
                e.topic,
                e.mqtt_qos,
                e.event_time_utc,
                e.payload,
                json.dumps(e.metadata, separators=(",", ":"), default=str),
            )
            for e in events
        ]
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                "INSERT INTO raw_events (event_uuid, received_at_utc, source, account_key,"
                " vin, topic, mqtt_qos, event_time_utc, payload, metadata_json)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.execute("COMMIT")
        except sqlite3.Error as err:
            with contextlib.suppress(sqlite3.Error):  # rollback of a dead txn
                conn.execute("ROLLBACK")
            raise _wrap_sqlite_error(err, "appending events failed") from err
        return len(rows)

    async def append_events(self, events: Sequence[RawEvent]) -> int:
        """Async wrapper around :meth:`append_events_sync`."""
        return int(await self._run(self.append_events_sync, list(events)))

    async def append_event(self, event: RawEvent) -> int:
        """Commit a single event."""
        return await self.append_events([event])

    def import_events_sync(self, events: Sequence[RawEvent]) -> tuple[int, int]:
        """Insert events idempotently by ``event_uuid``.

        Returns ``(inserted, skipped)``. Used by the importer so a JSONL export
        can be replayed into another database without duplicating rows.
        """
        if not events:
            return (0, 0)
        conn = self._require()
        inserted = 0
        try:
            conn.execute("BEGIN IMMEDIATE")
            for e in events:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO raw_events (event_uuid, received_at_utc, source,"
                    " account_key, vin, topic, mqtt_qos, event_time_utc, payload, metadata_json)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        e.event_uuid,
                        e.received_at_utc,
                        e.source,
                        e.account_key,
                        e.vin,
                        e.topic,
                        e.mqtt_qos,
                        e.event_time_utc,
                        e.payload,
                        json.dumps(e.metadata, separators=(",", ":"), default=str),
                    ),
                )
                inserted += cur.rowcount or 0
            conn.execute("COMMIT")
        except sqlite3.Error as err:
            with contextlib.suppress(sqlite3.Error):  # rollback of a dead txn
                conn.execute("ROLLBACK")
            raise _wrap_sqlite_error(err, "importing events failed") from err
        return (inserted, len(events) - inserted)

    def iter_events_sync(
        self,
        *,
        after_id: int = 0,
        vin: str | None = None,
        source: str | None = None,
        since: str | None = None,
        until: str | None = None,
        limit: int | None = None,
    ) -> Iterator[StoredEvent]:
        """Stream stored events in insertion order (stable, replayable)."""
        conn = self._require()
        clauses = ["id > ?"]
        params: list[Any] = [after_id]
        if vin is not None:
            clauses.append("vin = ?")
            params.append(vin)
        if source is not None:
            clauses.append("source = ?")
            params.append(source)
        if since is not None:
            clauses.append("received_at_utc >= ?")
            params.append(since)
        if until is not None:
            clauses.append("received_at_utc < ?")
            params.append(until)
        sql = f"SELECT * FROM raw_events WHERE {' AND '.join(clauses)} ORDER BY id"  # noqa: S608
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        for row in conn.execute(sql, params):
            yield _row_to_event(row)

    def count_events_sync(self) -> int:
        """Total number of archived events."""
        return int(self._require().execute("SELECT COUNT(*) FROM raw_events").fetchone()[0])

    async def count_events(self) -> int:
        """Async wrapper around :meth:`count_events_sync`."""
        return int(await self._run(self.count_events_sync))

    def event_stats_sync(self) -> dict[str, Any]:
        """Per-source counts and the newest receive timestamp."""
        conn = self._require()
        by_source = {
            row["source"]: row["n"]
            for row in conn.execute("SELECT source, COUNT(*) AS n FROM raw_events GROUP BY source")
        }
        newest = conn.execute("SELECT MAX(received_at_utc) FROM raw_events").fetchone()[0]
        return {"by_source": by_source, "total": sum(by_source.values()), "newest": newest}

    # -- state --------------------------------------------------------------
    def get_state_sync(self, key: str, default: Any = None) -> Any:
        """Read a JSON value from ``collector_state``."""
        row = (
            self._require()
            .execute("SELECT value_json FROM collector_state WHERE key = ?", (key,))
            .fetchone()
        )
        return default if row is None else json.loads(row["value_json"])

    async def get_state(self, key: str, default: Any = None) -> Any:
        """Async wrapper around :meth:`get_state_sync`."""
        return await self._run(self.get_state_sync, key, default)

    def set_state_sync(self, key: str, value: Any) -> None:
        """Write a JSON value to ``collector_state``, committing immediately."""
        conn = self._require()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO collector_state(key, value_json, updated_at_utc) VALUES (?, ?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,"
                " updated_at_utc=excluded.updated_at_utc",
                (key, json.dumps(value, separators=(",", ":"), default=str), utc_now_iso()),
            )
            conn.execute("COMMIT")
        except sqlite3.Error as err:
            with contextlib.suppress(sqlite3.Error):  # rollback of a dead txn
                conn.execute("ROLLBACK")
            raise _wrap_sqlite_error(err, f"writing state {key!r} failed") from err

    async def set_state(self, key: str, value: Any) -> None:
        """Async wrapper around :meth:`set_state_sync`."""
        await self._run(self.set_state_sync, key, value)

    # -- incidents ----------------------------------------------------------
    def record_incident_sync(self, kind: str, details: dict[str, Any]) -> None:
        """Append an operational incident."""
        from .logging_setup import redact_mapping

        conn = self._require()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO collector_incidents(occurred_at_utc, kind, details_json)"
                " VALUES (?, ?, ?)",
                (
                    utc_now_iso(),
                    kind,
                    json.dumps(redact_mapping(details), separators=(",", ":"), default=str),
                ),
            )
            conn.execute("COMMIT")
        except sqlite3.Error as err:
            with contextlib.suppress(sqlite3.Error):  # rollback of a dead txn
                conn.execute("ROLLBACK")
            raise _wrap_sqlite_error(err, "recording incident failed") from err

    async def record_incident(self, kind: str, details: dict[str, Any]) -> None:
        """Async wrapper around :meth:`record_incident_sync`."""
        await self._run(self.record_incident_sync, kind, details)

    def recent_incidents_sync(self, limit: int = 20) -> list[dict[str, Any]]:
        """Most recent incidents, newest first."""
        rows = self._require().execute(
            "SELECT occurred_at_utc, kind, details_json FROM collector_incidents"
            " ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [
            {
                "occurred_at_utc": r["occurred_at_utc"],
                "kind": r["kind"],
                "details": json.loads(r["details_json"]),
            }
            for r in rows
        ]

    # -- vehicles -----------------------------------------------------------
    def upsert_vehicle_sync(
        self,
        vin: str,
        *,
        account_key: str,
        mapping_type: str | None = None,
        mapped_since: str | None = None,
        collection_enabled: bool | None = None,
    ) -> None:
        """Record or refresh a known vehicle, preserving first-observed time."""
        conn = self._require()
        now = utc_now_iso()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO vehicles(vin, account_key, first_observed_at_utc, last_seen_at_utc,"
                " mapping_type, mapped_since, collection_enabled)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(vin) DO UPDATE SET"
                "  last_seen_at_utc=excluded.last_seen_at_utc,"
                "  mapping_type=COALESCE(excluded.mapping_type, vehicles.mapping_type),"
                "  mapped_since=COALESCE(excluded.mapped_since, vehicles.mapped_since),"
                "  collection_enabled=COALESCE(?, vehicles.collection_enabled)",
                (
                    vin,
                    account_key,
                    now,
                    now,
                    mapping_type,
                    mapped_since,
                    1 if collection_enabled is None else int(collection_enabled),
                    None if collection_enabled is None else int(collection_enabled),
                ),
            )
            conn.execute("COMMIT")
        except sqlite3.Error as err:
            with contextlib.suppress(sqlite3.Error):  # rollback of a dead txn
                conn.execute("ROLLBACK")
            raise _wrap_sqlite_error(err, f"upserting vehicle {vin} failed") from err

    async def upsert_vehicle(self, vin: str, **kwargs: Any) -> None:
        """Async wrapper around :meth:`upsert_vehicle_sync`."""
        async with self._lock:
            await asyncio.to_thread(lambda: self.upsert_vehicle_sync(vin, **kwargs))

    def list_vehicles_sync(self) -> list[dict[str, Any]]:
        """All known vehicles, including ones no longer being collected."""
        rows = self._require().execute("SELECT * FROM vehicles ORDER BY vin")
        return [
            {
                "vin": r["vin"],
                "account_key": r["account_key"],
                "first_observed_at_utc": r["first_observed_at_utc"],
                "last_seen_at_utc": r["last_seen_at_utc"],
                "mapping_type": r["mapping_type"],
                "mapped_since": r["mapped_since"],
                "collection_enabled": bool(r["collection_enabled"]),
            }
            for r in rows
        ]

    async def list_vehicles(self) -> list[dict[str, Any]]:
        """Async wrapper around :meth:`list_vehicles_sync`."""
        return list(await self._run(self.list_vehicles_sync))

    # -- maintenance --------------------------------------------------------
    def integrity_check_sync(self) -> str:
        """Run ``PRAGMA integrity_check`` and return its first result row."""
        try:
            return str(self._require().execute("PRAGMA integrity_check").fetchone()[0])
        except sqlite3.Error as err:
            raise _wrap_sqlite_error(err, "integrity check failed") from err

    def checkpoint_sync(self, mode: str = "PASSIVE") -> None:
        """Checkpoint the WAL back into the main database file."""
        if mode not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise ValueError(f"unsupported checkpoint mode {mode!r}")
        try:
            self._require().execute(f"PRAGMA wal_checkpoint({mode})")
        except sqlite3.Error as err:
            raise _wrap_sqlite_error(err, "WAL checkpoint failed") from err

    async def checkpoint(self, mode: str = "PASSIVE") -> None:
        """Async wrapper around :meth:`checkpoint_sync`."""
        await self._run(self.checkpoint_sync, mode)

    def backup_sync(self, destination: str | Path) -> Path:
        """Take a consistent snapshot using SQLite's online backup API.

        Copying the ``.sqlite3`` file alone is unsafe in WAL mode because
        recent commits may still live in the ``-wal`` sidecar. This writes a
        single self-contained file instead.
        """
        dest = Path(destination)
        dest.parent.mkdir(parents=True, exist_ok=True)
        conn = self._require()
        try:
            with sqlite3.connect(dest) as target:
                conn.backup(target)
        except sqlite3.Error as err:
            raise _wrap_sqlite_error(err, f"backup to {dest} failed") from err
        dest.chmod(0o600)
        return dest

    async def backup(self, destination: str | Path) -> Path:
        """Async wrapper around :meth:`backup_sync`."""
        return Path(await self._run(self.backup_sync, destination))

    def writable_sync(self) -> bool:
        """Whether a trivial write currently succeeds (disk-full detection)."""
        try:
            self.set_state_sync("storage_probe", {"at": utc_now_iso()})
        except (StorageError, sqlite3.Error):
            # A health probe reports; it never raises.
            return False
        return True

    async def writable(self) -> bool:
        """Async wrapper around :meth:`writable_sync`."""
        return bool(await self._run(self.writable_sync))
