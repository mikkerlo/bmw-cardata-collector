"""Quota-aware scheduler for periodic REST snapshots.

BMW documents 50 REST requests per day per user, reset at 00:00 UTC, and that
allowance may be shared with any other client signed in to the same account.
Exceeding it earns an HTTP 403 for the rest of the day, which would also block
onboarding and diagnostics, so the scheduler is deliberately pessimistic:

* every request is counted *before* it is issued — a failure still consumed
  BMW's quota even if we got nothing useful back;
* the counter and each task's last-run time live in SQLite, so a restart
  neither re-runs a task that just ran nor forgets what today cost;
* a restart never "catches up" on missed runs: a task that is overdue runs
  once, not once per missed interval;
* scheduled work stops at a reserve, leaving a few requests for interactive
  commands and retries;
* retries are capped and also counted, and a quota rejection parks all REST
  activity until the next UTC midnight.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from . import metrics
from .auth import AuthError
from .bmw_source import CarDataError, CarDataRestClient, QuotaExhaustedError, TransientError
from .config import RestConfig, RestTaskConfig
from .store import EventStore, RawEvent, utc_now_iso
from .vehicle_registry import VehicleRegistry
from .writer import EventWriter

_LOGGER = logging.getLogger(__name__)

STATE_KEY_QUOTA = "rest_quota"
STATE_KEY_TASK_RUNS = "rest_task_runs"
STATE_KEY_TASK_OFFSETS = "rest_task_offsets"

#: Requests held back from scheduled work for interactive use and retries.
DEFAULT_RESERVE = 5
#: How often the scheduler re-evaluates which tasks are due.
TICK_SECONDS = 60.0


def utc_day(when: float | None = None) -> str:
    """Current UTC calendar day as ``YYYY-MM-DD`` (the quota reset boundary)."""
    moment = datetime.fromtimestamp(when if when is not None else time.time(), tz=UTC)
    return moment.date().isoformat()


def next_utc_midnight(when: float | None = None) -> float:
    """Unix timestamp of the next 00:00 UTC."""
    moment = datetime.fromtimestamp(when if when is not None else time.time(), tz=UTC)
    tomorrow = datetime.combine(moment.date() + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
    return tomorrow.timestamp()


@dataclass(slots=True)
class QuotaLedger:
    """Today's request count, persisted so restarts cannot double-spend."""

    day: str
    used: int = 0
    exhausted_until: float | None = None

    def rolled_over(self, *, now: float | None = None) -> bool:
        """Whether the UTC day changed since this ledger was written."""
        return self.day != utc_day(now)

    def as_dict(self) -> dict[str, Any]:
        """Serializable form."""
        return {"day": self.day, "used": self.used, "exhausted_until": self.exhausted_until}

    @classmethod
    def from_dict(cls, raw: Any) -> QuotaLedger:
        """Restore from persisted state, tolerating an absent or odd value."""
        if not isinstance(raw, dict):
            return cls(day=utc_day())
        return cls(
            day=str(raw.get("day") or utc_day()),
            used=int(raw.get("used") or 0),
            exhausted_until=(float(raw["exhausted_until"]) if raw.get("exhausted_until") else None),
        )


class RestScheduler:
    """Runs configured REST snapshot tasks within the daily budget."""

    def __init__(
        self,
        config: RestConfig,
        client: CarDataRestClient,
        writer: EventWriter,
        store: EventStore,
        registry: VehicleRegistry,
        *,
        account_key: str,
        reserve: int = DEFAULT_RESERVE,
        tick_seconds: float = TICK_SECONDS,
    ) -> None:
        self._config = config
        self._client = client
        self._writer = writer
        self._store = store
        self._registry = registry
        self._account_key = account_key
        # Never let the reserve swallow the whole budget.
        self._reserve = min(reserve, max(config.requests_per_day_budget // 5, 0))
        self._tick = tick_seconds
        self._ledger = QuotaLedger(day=utc_day())
        self._last_runs: dict[str, str] = {}
        # Where each per-VIN task resumes, so a run truncated by the budget
        # does not keep collecting the same VINs and starving the others.
        self._offsets: dict[str, int] = {}
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self.last_success_at: float | None = None
        self.last_error: str | None = None
        self.requests_issued = 0

    # -- state --------------------------------------------------------------
    async def load_state(self) -> None:
        """Restore the quota ledger and task cursors from SQLite."""
        self._ledger = QuotaLedger.from_dict(await self._store.get_state(STATE_KEY_QUOTA))
        if self._ledger.rolled_over():
            self._ledger = QuotaLedger(day=utc_day())
            await self._persist_ledger()
        runs = await self._store.get_state(STATE_KEY_TASK_RUNS)
        if isinstance(runs, dict):
            self._last_runs = {str(k): str(v) for k, v in runs.items() if v}
        offsets = await self._store.get_state(STATE_KEY_TASK_OFFSETS)
        if isinstance(offsets, dict):
            self._offsets = {
                str(k): int(v) for k, v in offsets.items() if isinstance(v, int | float)
            }
        metrics.bmw_rest_budget_estimate.set(self.remaining_budget())
        metrics.bmw_rest_quota_exhausted.set(1 if self.quota_blocked() else 0)

    async def _persist_ledger(self) -> None:
        await self._store.set_state(STATE_KEY_QUOTA, self._ledger.as_dict())
        metrics.bmw_rest_budget_estimate.set(self.remaining_budget())

    async def _persist_runs(self) -> None:
        await self._store.set_state(STATE_KEY_TASK_RUNS, self._last_runs)
        await self._store.set_state(STATE_KEY_TASK_OFFSETS, self._offsets)

    # -- budget -------------------------------------------------------------
    def remaining_budget(self) -> int:
        """Requests left in today's configured budget, ignoring the reserve."""
        if self._ledger.rolled_over():
            return self._config.requests_per_day_budget
        return max(self._config.requests_per_day_budget - self._ledger.used, 0)

    def quota_blocked(self, *, now: float | None = None) -> bool:
        """Whether BMW is currently refusing REST calls for this account."""
        until = self._ledger.exhausted_until
        return until is not None and (now or time.time()) < until

    async def _roll_day_if_needed(self) -> None:
        if self._ledger.rolled_over():
            _LOGGER.info(
                "REST quota day rolled over",
                extra={"previous_day": self._ledger.day, "previous_used": self._ledger.used},
            )
            self._ledger = QuotaLedger(day=utc_day())
            await self._persist_ledger()
            metrics.bmw_rest_quota_exhausted.set(0)

    async def _spend(self, count: int = 1) -> None:
        """Count requests against today's budget and persist immediately."""
        self._ledger.used += count
        self.requests_issued += count
        await self._persist_ledger()

    async def _mark_exhausted(self, error: QuotaExhaustedError) -> None:
        self._ledger.exhausted_until = next_utc_midnight()
        # Treat the whole budget as spent: BMW's counter is the real one.
        self._ledger.used = max(self._ledger.used, self._config.requests_per_day_budget)
        await self._persist_ledger()
        metrics.bmw_rest_quota_exhausted.set(1)
        self.last_error = str(error)
        _LOGGER.error(
            "BMW reports the daily REST quota is exhausted; pausing REST until 00:00 UTC. "
            "The quota is per BMW account and may be shared with other clients.",
            extra={"error_id": error.error_id},
        )
        await self._incident("rest_quota_exhausted", {"error_id": error.error_id})

    # -- scheduling ---------------------------------------------------------
    def due_tasks(self, *, now: float | None = None) -> list[RestTaskConfig]:
        """Tasks whose interval has elapsed, soonest-overdue first."""
        moment = now if now is not None else time.time()
        due: list[tuple[float, RestTaskConfig]] = []
        for task in self._config.tasks:
            if not task.enabled:
                continue
            last = self._last_runs.get(task.key)
            if last is None:
                due.append((0.0, task))
                continue
            try:
                last_ts = datetime.fromisoformat(last).timestamp()
            except ValueError:
                due.append((0.0, task))
                continue
            next_due = last_ts + task.interval_hours * 3600
            if next_due <= moment:
                due.append((next_due, task))
        due.sort(key=lambda item: item[0])
        return [task for _, task in due]

    def _requests_for(self, task: RestTaskConfig) -> int:
        """How many requests one run of this task will cost."""
        if task.type == "vehicle_mapping":
            return 1
        return max(len(self._registry.active_vins()), 0)

    def start(self) -> None:
        """Start the scheduler loop."""
        if not self._config.enabled:
            _LOGGER.info("REST scheduler disabled by configuration")
            return
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="rest-scheduler")

    async def stop(self) -> None:
        """Stop the scheduler loop."""
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def status(self) -> dict[str, Any]:
        """Snapshot for the readiness endpoint."""
        return {
            "enabled": self._config.enabled,
            "budget": self._config.requests_per_day_budget,
            "used_today": self._ledger.used,
            "remaining": self.remaining_budget(),
            "reserve": self._reserve,
            "quota_blocked": self.quota_blocked(),
            "day": self._ledger.day,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "last_runs": dict(self._last_runs),
        }

    async def _run(self) -> None:
        _LOGGER.info(
            "REST scheduler started",
            extra={
                "budget": self._config.requests_per_day_budget,
                "reserve": self._reserve,
                "tasks": [t.key for t in self._config.tasks if t.enabled],
            },
        )
        while not self._stop.is_set():
            try:
                await self._tick_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # the loop must survive anything
                _LOGGER.exception("REST scheduler tick failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self._tick)

    async def _tick_once(self) -> None:
        await self._roll_day_if_needed()
        if self.quota_blocked():
            return
        for task in self.due_tasks():
            if self._stop.is_set():
                return
            cost = self._requests_for(task)
            if cost == 0:
                # Nothing to ask about yet (no VINs discovered); try next tick.
                continue
            available = self.remaining_budget() - self._reserve
            if available < 1:
                _LOGGER.warning(
                    "skipping a REST task: today's budget is spent down to the reserve of %d",
                    self._reserve,
                    extra={"task": task.key},
                )
                continue
            if available < cost:
                # Collect what the budget allows rather than nothing at all;
                # the per-task offset makes the next run start where this one
                # stopped, so every VIN gets covered over time.
                _LOGGER.warning(
                    "REST task will be truncated: %d request(s) available after the reserve, "
                    "task needs %d; it resumes from where it stops",
                    available,
                    cost,
                    extra={"task": task.key},
                )
            try:
                await self._run_task(task)
            except QuotaExhaustedError as err:
                await self._mark_exhausted(err)
                return
            self._last_runs[task.key] = utc_now_iso()
            await self._persist_runs()

    async def _run_task(self, task: RestTaskConfig) -> None:
        """Execute one task, archiving every response it produces."""
        _LOGGER.info("running REST task", extra={"task": task.key})
        if task.type == "vehicle_mapping":
            response = await self._call(self._client.get_vehicle_mappings)
            if response is None:
                return
            await self._archive(response.body, response.metadata(task=task.type))
            with contextlib.suppress(ValueError):
                await self._registry.update_from_response(response.body)
            return

        for vin in self._rotated_vins(task):
            if self._stop.is_set():
                return
            if self.remaining_budget() - self._reserve < 1:
                _LOGGER.warning("budget reserve reached mid-task", extra={"task": task.key})
                return
            self._offsets[task.key] = self._offsets.get(task.key, 0) + 1
            if task.type == "basic_data":
                response = await self._call(self._client.get_basic_data, vin)
            elif task.type == "tyre_diagnosis":
                response = await self._call(self._client.get_tyre_diagnosis, vin)
            elif task.type == "telematic_snapshot":
                assert task.container_id is not None  # enforced by config validation
                response = await self._call(self._client.get_telematic_data, vin, task.container_id)
            else:  # pragma: no cover - config validation rejects unknown types
                raise ValueError(f"unsupported REST task type {task.type!r}")
            if response is None:
                continue
            await self._archive(
                response.body,
                response.metadata(task=task.type, container_id=task.container_id),
                vin=vin,
            )

    def _rotated_vins(self, task: RestTaskConfig) -> list[str]:
        """Active VINs, starting where this task last stopped.

        A run cut short by the daily budget would otherwise always cover the
        same first few VINs and never reach the rest.
        """
        vins = list(self._registry.active_vins())
        if not vins:
            return vins
        offset = self._offsets.get(task.key, 0) % len(vins)
        return vins[offset:] + vins[:offset]

    async def _call(self, func: Any, *args: Any) -> Any:
        """Issue a request with capped, budget-aware retries.

        Returns ``None`` when the call ultimately failed; a quota rejection is
        re-raised so the caller can park all REST activity.
        """
        attempts = max(1, min(self._config.max_retries, 3))
        for attempt in range(1, attempts + 1):
            await self._spend(1)
            try:
                response = await func(*args)
            except QuotaExhaustedError:
                metrics.bmw_rest_requests_total.labels(status_class="4xx").inc()
                raise
            except TransientError as err:
                metrics.bmw_rest_requests_total.labels(
                    status_class="5xx" if (err.status or 500) >= 500 else "4xx"
                ).inc()
                self.last_error = str(err)
                last_attempt = attempt >= attempts
                # A retry costs another request, so only retry while there is
                # budget to spare beyond the reserve.
                if last_attempt or self.remaining_budget() - self._reserve < 1:
                    _LOGGER.error("REST request failed, giving up: %s", err)
                    await self._incident(
                        "rest_request_failed", {"error": str(err), "attempts": attempt}
                    )
                    return None
                delay = min(60.0, 2.0**attempt) * random.uniform(0.5, 1.5)  # noqa: S311
                _LOGGER.warning(
                    "REST request failed, retrying in %.1fs: %s",
                    delay,
                    err,
                    extra={"attempt": attempt},
                )
                await asyncio.sleep(delay)
            except (CarDataError, AuthError) as err:
                metrics.bmw_rest_requests_total.labels(status_class="4xx").inc()
                self.last_error = str(err)
                _LOGGER.error("REST request rejected: %s", err)
                await self._incident("rest_request_rejected", {"error": str(err)})
                return None
            else:
                metrics.bmw_rest_requests_total.labels(status_class="2xx").inc()
                self.last_success_at = time.time()
                self.last_error = None
                metrics.bmw_rest_last_success_timestamp_seconds.set(self.last_success_at)
                return response
        return None

    async def _archive(
        self, payload: bytes, metadata: dict[str, Any], *, vin: str | None = None
    ) -> None:
        await self._writer.enqueue(
            RawEvent(
                source="rest",
                account_key=self._account_key,
                payload=payload,
                vin=vin,
                metadata=metadata,
            )
        )

    async def _incident(self, kind: str, details: dict[str, Any]) -> None:
        with contextlib.suppress(Exception):
            await self._store.record_incident(kind, details)
            metrics.collector_incidents_total.labels(kind=kind).inc()

    # -- one-shot helpers for the CLI ---------------------------------------
    async def run_task_now(self, task: RestTaskConfig) -> None:
        """Run one task immediately, still accounting for quota."""
        await self._roll_day_if_needed()
        if self.quota_blocked():
            raise QuotaExhaustedError("REST quota is exhausted until 00:00 UTC")
        await self._run_task(task)
        self._last_runs[task.key] = utc_now_iso()
        await self._persist_runs()
