"""Bounded ingest queue with a single durable writer task.

MQTT QoS 0 gives no acknowledgement back to BMW, so there is nothing to ack
and nothing to replay: whatever the receiver fails to persist is gone. The
receive callback therefore does the least possible work — enqueue — and one
writer task owns every SQLite commit.

Saturation behaviour is explicit and loud rather than silent:

* the queue is bounded (``storage.queue_max``);
* a full queue drops the *newest* event, counts it, and records an incident,
  because the alternative is blocking the MQTT read loop until the broker
  disconnects us;
* a failed commit is counted as a failure and never as a committed event.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import Any

from . import metrics
from .store import EventStore, RawEvent, StorageError, StorageFullError

_LOGGER = logging.getLogger(__name__)

#: How many queued events to group into one transaction.
MAX_BATCH = 64
#: How long the writer waits to fill a batch once it has at least one event.
BATCH_LINGER_SECONDS = 0.05
#: Minimum gap between queue-overflow incident rows. Sustained overflow means
#: storage is already the bottleneck, so one incident per dropped event would
#: pile write load onto exactly the thing that is struggling. Each row carries
#: the cumulative count, so nothing is hidden by the coalescing.
OVERFLOW_INCIDENT_INTERVAL_SECONDS = 60.0


class EventWriter:
    """Owns the write path from in-memory queue to committed rows."""

    def __init__(
        self,
        store: EventStore,
        *,
        queue_max: int = 10_000,
        max_batch: int = MAX_BATCH,
        linger_seconds: float = BATCH_LINGER_SECONDS,
    ) -> None:
        self._store = store
        self._queue: asyncio.Queue[RawEvent] = asyncio.Queue(maxsize=queue_max)
        self._max_batch = max_batch
        self._linger = linger_seconds
        self._task: asyncio.Task[None] | None = None
        self._side_tasks: set[asyncio.Task[None]] = set()
        self._last_overflow_incident: float | None = None
        self._stopping = asyncio.Event()
        self.committed = 0
        self.dropped = 0
        self.write_failures = 0
        self.last_write_error: str | None = None
        self.storage_healthy = True

    @property
    def queue_depth(self) -> int:
        """Events currently waiting to be committed."""
        return self._queue.qsize()

    def status(self) -> dict[str, Any]:
        """Snapshot for the readiness endpoint."""
        return {
            "queue_depth": self.queue_depth,
            "committed": self.committed,
            "dropped": self.dropped,
            "write_failures": self.write_failures,
            "storage_healthy": self.storage_healthy,
            "last_write_error": self.last_write_error,
        }

    def submit(self, event: RawEvent) -> bool:
        """Enqueue an event without blocking. ``False`` means it was dropped.

        Called straight from the MQTT read loop, so it must never await.
        """
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1
            metrics.collector_queue_overflows_total.inc()
            _LOGGER.error(
                "write queue full, dropped an event — this is lost data",
                extra={"queue_max": self._queue.maxsize, "dropped_total": self.dropped},
            )
            now = time.monotonic()
            if (
                self._last_overflow_incident is None
                or now - self._last_overflow_incident >= OVERFLOW_INCIDENT_INTERVAL_SECONDS
            ):
                self._last_overflow_incident = now
                # Record out of band: submit() must stay non-blocking. Keep a
                # strong reference, or the task can be collected before it runs.
                task = asyncio.get_running_loop().create_task(self._record_overflow())
                self._side_tasks.add(task)
                task.add_done_callback(self._side_tasks.discard)
            return False
        metrics.collector_queue_depth.set(self._queue.qsize())
        return True

    async def _await_side_tasks(self, *, wait_seconds: float = 5.0) -> None:
        """Let pending incident writes finish before the loop goes away."""
        pending = list(self._side_tasks)
        if not pending:
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), wait_seconds)

    async def _record_overflow(self) -> None:
        try:
            await self._store.record_incident(
                "queue_overflow",
                {"dropped_total": self.dropped, "queue_max": self._queue.maxsize},
            )
            metrics.collector_incidents_total.labels(kind="queue_overflow").inc()
        except StorageError as err:  # pragma: no cover - storage already failing
            _LOGGER.error("could not record queue overflow incident: %s", err)

    async def enqueue(self, event: RawEvent) -> None:
        """Enqueue an event, waiting for room. For the REST path only."""
        await self._queue.put(event)
        metrics.collector_queue_depth.set(self._queue.qsize())

    def start(self) -> None:
        """Start the writer task."""
        if self._task is None or self._task.done():
            self._stopping.clear()
            self._task = asyncio.create_task(self._run(), name="event-writer")

    async def stop(self, *, drain_timeout: float = 30.0) -> None:
        """Stop the writer after draining what is already queued."""
        self._stopping.set()
        await self._await_side_tasks()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self._task, timeout=drain_timeout)
        except TimeoutError:
            _LOGGER.error(
                "writer did not drain within %.0fs; %d event(s) were not committed",
                drain_timeout,
                self.queue_depth,
            )
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        finally:
            self._task = None

    async def _collect_batch(self) -> list[RawEvent]:
        """Wait for at least one event, then briefly linger to batch more."""
        try:
            first = await asyncio.wait_for(self._queue.get(), timeout=0.5)
        except TimeoutError:
            return []
        batch = [first]
        deadline = time.monotonic() + self._linger
        while len(batch) < self._max_batch:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                batch.append(await asyncio.wait_for(self._queue.get(), timeout=remaining))
            except TimeoutError:
                break
        return batch

    async def _run(self) -> None:
        _LOGGER.info("writer started", extra={"queue_max": self._queue.maxsize})
        while True:
            if self._stopping.is_set() and self._queue.empty():
                break
            batch = await self._collect_batch()
            if not batch:
                continue
            await self._commit(batch)
            metrics.collector_queue_depth.set(self._queue.qsize())
        _LOGGER.info("writer stopped", extra={"committed_total": self.committed})

    async def _commit(self, batch: list[RawEvent]) -> None:
        started = time.monotonic()
        try:
            written = await self._store.append_events(batch)
        except StorageError as err:
            # The events are gone: there is no upstream to replay them from.
            # Make that visible instead of pretending they were stored.
            self.write_failures += len(batch)
            self.last_write_error = str(err)
            self.storage_healthy = False
            metrics.collector_storage_write_failures_total.inc(len(batch))
            metrics.collector_storage_writable.set(0)
            fatal = isinstance(err, StorageFullError)
            _LOGGER.error(
                "failed to commit %d event(s): %s",
                len(batch),
                err,
                extra={"fatal_storage": fatal, "batch_size": len(batch)},
            )
            try:
                await self._store.record_incident(
                    "storage_write_failure",
                    {"batch_size": len(batch), "error": str(err), "disk_full": fatal},
                )
                metrics.collector_incidents_total.labels(kind="storage_write_failure").inc()
            except StorageError:
                pass
            return

        metrics.collector_commit_duration_seconds.observe(time.monotonic() - started)
        self.committed += written
        self.storage_healthy = True
        self.last_write_error = None
        metrics.collector_storage_writable.set(1)
        for event in batch:
            metrics.collector_events_committed_total.labels(source=event.source).inc()
        _LOGGER.debug("committed %d event(s)", written, extra={"batch_size": written})
