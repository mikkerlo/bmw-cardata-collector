"""REST scheduler: quota accounting, persistence across restart, backoff."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from bmw_collector.bmw_source import CarDataError, QuotaExhaustedError, RestResponse, TransientError
from bmw_collector.config import RestConfig, RestTaskConfig, VehiclesConfig
from bmw_collector.rest_scheduler import (
    STATE_KEY_QUOTA,
    STATE_KEY_TASK_OFFSETS,
    STATE_KEY_TASK_RUNS,
    QuotaLedger,
    RestScheduler,
    next_utc_midnight,
    utc_day,
)
from bmw_collector.store import EventStore
from bmw_collector.vehicle_registry import VehicleRegistry
from bmw_collector.writer import EventWriter

VIN_A = "WBA00000000000001"
VIN_B = "WBA00000000000002"


class StubRestClient:
    """Scripted REST client. Each call pops the next scripted outcome."""

    def __init__(self, outcomes: list[Any] | None = None) -> None:
        self.outcomes = list(outcomes or [])
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def _next(self, name: str, *args: Any) -> RestResponse:
        self.calls.append((name, args))
        outcome = self.outcomes.pop(0) if self.outcomes else ok_response()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def get_vehicle_mappings(self) -> RestResponse:
        """Scripted mappings call."""
        return self._next("mappings")

    async def get_basic_data(self, vin: str) -> RestResponse:
        """Scripted basic-data call."""
        return self._next("basic_data", vin)

    async def get_tyre_diagnosis(self, vin: str) -> RestResponse:
        """Scripted tyre-diagnosis call."""
        return self._next("tyre_diagnosis", vin)

    async def get_telematic_data(self, vin: str, container_id: str) -> RestResponse:
        """Scripted telematic-data call."""
        return self._next("telematic_snapshot", vin, container_id)


def ok_response(body: bytes = b'{"ok":true}') -> RestResponse:
    """A successful captured response."""
    return RestResponse(method="GET", url="https://api.invalid/x", status=200, body=body)


def mappings_response(*vins: str) -> RestResponse:
    """A mappings response listing the given VINs as primary."""
    import json

    body = json.dumps([{"vin": v, "mappingType": "PRIMARY"} for v in vins]).encode()
    return RestResponse(method="GET", url="https://api.invalid/m", status=200, body=body)


def quota_error() -> QuotaExhaustedError:
    """BMW's daily-limit rejection."""
    return QuotaExhaustedError("quota", status=403, error_id="CU-429")


async def build(
    store: EventStore,
    *,
    tasks: tuple[RestTaskConfig, ...],
    outcomes: list[Any] | None = None,
    budget: int = 35,
    vins: tuple[str, ...] = (VIN_A,),
    reserve: int = 5,
) -> tuple[RestScheduler, StubRestClient, EventWriter]:
    """Assemble a scheduler over real storage with a scripted client."""
    client = StubRestClient(outcomes)
    writer = EventWriter(store, queue_max=1000)
    registry = VehicleRegistry(
        VehiclesConfig(mode="allowlist", vins=vins), store, account_key="test"
    )
    await registry.load_persisted()
    config = RestConfig(enabled=True, requests_per_day_budget=budget, tasks=tasks, max_retries=3)
    scheduler = RestScheduler(
        config, client, writer, store, registry, account_key="test", reserve=reserve
    )
    await scheduler.load_state()
    return scheduler, client, writer


MAPPING_TASK = RestTaskConfig(type="vehicle_mapping", interval_hours=24)
BASIC_TASK = RestTaskConfig(type="basic_data", interval_hours=4)


# -- day boundaries ----------------------------------------------------------
def test_utc_day_is_the_quota_reset_boundary() -> None:
    noon = datetime(2026, 10, 18, 12, 0, tzinfo=UTC).timestamp()
    assert utc_day(noon) == "2026-10-18"
    # BMW resets at 00:00 UTC, not local midnight.
    assert utc_day(datetime(2026, 10, 18, 23, 59, 59, tzinfo=UTC).timestamp()) == "2026-10-18"
    assert next_utc_midnight(noon) == datetime(2026, 10, 19, 0, 0, tzinfo=UTC).timestamp()


def test_ledger_detects_the_day_rolling_over() -> None:
    ledger = QuotaLedger(day="2026-10-17", used=40)
    assert ledger.rolled_over(now=datetime(2026, 10, 18, 1, 0, tzinfo=UTC).timestamp()) is True
    assert ledger.rolled_over(now=datetime(2026, 10, 17, 23, 0, tzinfo=UTC).timestamp()) is False


def test_ledger_tolerates_corrupt_persisted_state() -> None:
    assert QuotaLedger.from_dict(None).used == 0
    assert QuotaLedger.from_dict({"day": "2026-01-01", "used": 3}).used == 3


# -- scheduling --------------------------------------------------------------
async def test_a_never_run_task_is_due_immediately(async_store: EventStore) -> None:
    scheduler, _, _ = await build(async_store, tasks=(MAPPING_TASK,))
    assert [t.key for t in scheduler.due_tasks()] == ["vehicle_mapping"]


async def test_a_task_is_not_due_again_until_its_interval_elapses(
    async_store: EventStore,
) -> None:
    scheduler, client, writer = await build(
        async_store, tasks=(MAPPING_TASK,), outcomes=[mappings_response(VIN_A)]
    )
    await scheduler._tick_once()
    assert scheduler.due_tasks() == []
    # Half an interval later it is still not due.
    assert scheduler.due_tasks(now=time.time() + 12 * 3600) == []
    assert [t.key for t in scheduler.due_tasks(now=time.time() + 25 * 3600)] == ["vehicle_mapping"]
    await writer.stop()


async def test_disabled_tasks_are_never_due(async_store: EventStore) -> None:
    disabled = RestTaskConfig(type="basic_data", interval_hours=1, enabled=False)
    scheduler, _, _ = await build(async_store, tasks=(disabled,))
    assert scheduler.due_tasks() == []


async def test_tasks_with_corrupt_cursors_are_treated_as_due(async_store: EventStore) -> None:
    await async_store.set_state(STATE_KEY_TASK_RUNS, {"vehicle_mapping": "not-a-timestamp"})
    scheduler, _, _ = await build(async_store, tasks=(MAPPING_TASK,))
    assert [t.key for t in scheduler.due_tasks()] == ["vehicle_mapping"]


# -- archival ----------------------------------------------------------------
async def test_responses_are_archived_with_their_endpoint(async_store: EventStore) -> None:
    scheduler, _, writer = await build(
        async_store,
        tasks=(MAPPING_TASK, BASIC_TASK),
        outcomes=[mappings_response(VIN_A), ok_response(b'{"model":"i4"}')],
    )
    writer.start()
    await scheduler._tick_once()
    await writer.stop()

    events = await _events(async_store)
    assert {e.metadata["task"] for e in events} == {"vehicle_mapping", "basic_data"}
    basic = next(e for e in events if e.metadata["task"] == "basic_data")
    assert basic.payload == b'{"model":"i4"}'
    assert basic.vin == VIN_A
    assert basic.source == "rest"


async def test_one_request_is_issued_per_active_vin(async_store: EventStore) -> None:
    scheduler, client, writer = await build(async_store, tasks=(BASIC_TASK,), vins=(VIN_A, VIN_B))
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    assert [c[1][0] for c in client.calls] == [VIN_A, VIN_B]
    assert scheduler.status()["used_today"] == 2


async def test_mappings_update_the_registry(async_store: EventStore) -> None:
    scheduler, _, writer = await build(
        async_store,
        tasks=(MAPPING_TASK,),
        outcomes=[mappings_response(VIN_A, VIN_B)],
        vins=(VIN_A, VIN_B),
    )
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    assert set(scheduler._registry.subscribable_vins()) == {VIN_A, VIN_B}


async def test_a_task_is_skipped_when_no_vin_is_active_yet(async_store: EventStore) -> None:
    scheduler, client, _ = await build(async_store, tasks=(BASIC_TASK,), vins=())
    await scheduler._tick_once()
    # Nothing to ask about: do not burn a request, and stay due for next tick.
    assert client.calls == []
    assert [t.key for t in scheduler.due_tasks()] == ["basic_data"]


# -- quota -------------------------------------------------------------------
async def test_every_request_is_counted_including_failures(async_store: EventStore) -> None:
    scheduler, _, writer = await build(
        async_store,
        tasks=(BASIC_TASK,),
        outcomes=[CarDataError("rejected", status=404)],
    )
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    # A failed call still consumed BMW's allowance.
    assert scheduler.status()["used_today"] == 1


async def test_budget_is_persisted_across_restart(async_store: EventStore) -> None:
    first, _, writer = await build(async_store, tasks=(BASIC_TASK,), vins=(VIN_A, VIN_B))
    writer.start()
    await first._tick_once()
    await writer.stop()
    used = first.status()["used_today"]
    assert used == 2

    second, _, _ = await build(async_store, tasks=(BASIC_TASK,), vins=(VIN_A, VIN_B))
    # A restart must not forget what today already cost.
    assert second.status()["used_today"] == used
    assert second.remaining_budget() == 35 - used


async def test_restart_does_not_re_run_a_task_that_just_ran(async_store: EventStore) -> None:
    first, client, writer = await build(
        async_store, tasks=(MAPPING_TASK,), outcomes=[mappings_response(VIN_A)]
    )
    writer.start()
    await first._tick_once()
    await writer.stop()

    second, client2, _ = await build(async_store, tasks=(MAPPING_TASK,))
    await second._tick_once()
    # Restart loops would otherwise spend the whole daily budget on startup.
    assert client2.calls == []


async def test_an_overdue_task_runs_once_not_once_per_missed_interval(
    async_store: EventStore,
) -> None:
    long_ago = (datetime.now(tz=UTC) - timedelta(days=5)).isoformat()
    await async_store.set_state(STATE_KEY_TASK_RUNS, {"basic_data": long_ago})
    scheduler, client, writer = await build(async_store, tasks=(BASIC_TASK,))
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    # 5 days of missed 4-hour runs must not become 30 catch-up requests.
    assert len(client.calls) == 1


async def test_scheduled_work_stops_at_the_reserve(async_store: EventStore) -> None:
    await async_store.set_state(STATE_KEY_QUOTA, {"day": utc_day(), "used": 8})
    scheduler, client, _ = await build(
        async_store, tasks=(BASIC_TASK,), budget=10, reserve=2, vins=(VIN_A, VIN_B)
    )
    await scheduler._tick_once()
    # 2 left, 2 reserved: scheduled work must leave room for interactive use.
    assert client.calls == []


async def test_a_truncated_task_resumes_from_where_it_stopped(
    async_store: EventStore,
) -> None:
    # Budget for one request per tick, with two VINs to cover.
    await async_store.set_state(STATE_KEY_QUOTA, {"day": utc_day(), "used": 4})
    first, client, writer = await build(
        async_store, tasks=(BASIC_TASK,), budget=5, reserve=0, vins=(VIN_A, VIN_B)
    )
    writer.start()
    await first._tick_once()
    await writer.stop()
    assert [c[1][0] for c in client.calls] == [VIN_A]

    # Next day, same situation: the second VIN must get its turn rather than
    # the first being collected forever.
    await async_store.set_state(STATE_KEY_QUOTA, {"day": utc_day(), "used": 4})
    await async_store.set_state(STATE_KEY_TASK_RUNS, {})
    second, client2, writer2 = await build(
        async_store, tasks=(BASIC_TASK,), budget=5, reserve=0, vins=(VIN_A, VIN_B)
    )
    writer2.start()
    await second._tick_once()
    await writer2.stop()
    assert [c[1][0] for c in client2.calls] == [VIN_B]
    assert (await async_store.get_state(STATE_KEY_TASK_OFFSETS))["basic_data"] == 2


async def test_reserve_cannot_consume_the_whole_budget(async_store: EventStore) -> None:
    scheduler, client, writer = await build(async_store, tasks=(BASIC_TASK,), budget=3, reserve=5)
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    # A reserve larger than the budget would block collection entirely.
    assert len(client.calls) == 1
    assert scheduler.status()["reserve"] == 0


async def test_budget_runs_out_mid_task_without_losing_what_was_fetched(
    async_store: EventStore,
) -> None:
    await async_store.set_state(STATE_KEY_QUOTA, {"day": utc_day(), "used": 4})
    scheduler, client, writer = await build(
        async_store, tasks=(BASIC_TASK,), budget=5, reserve=0, vins=(VIN_A, VIN_B)
    )
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    assert len(client.calls) == 1
    assert await async_store.count_events() == 1


async def test_quota_rejection_parks_rest_until_utc_midnight(async_store: EventStore) -> None:
    scheduler, client, writer = await build(
        async_store, tasks=(BASIC_TASK,), outcomes=[quota_error()]
    )
    writer.start()
    await scheduler._tick_once()
    assert scheduler.quota_blocked() is True

    before = len(client.calls)
    await scheduler._tick_once()
    await writer.stop()
    # No further calls today, whatever the local budget says.
    assert len(client.calls) == before
    assert scheduler.status()["quota_blocked"] is True
    incidents = await _incidents(async_store)
    assert any(i["kind"] == "rest_quota_exhausted" for i in incidents)


async def test_quota_block_is_persisted_across_restart(async_store: EventStore) -> None:
    first, _, writer = await build(async_store, tasks=(BASIC_TASK,), outcomes=[quota_error()])
    writer.start()
    await first._tick_once()
    await writer.stop()

    second, client, _ = await build(async_store, tasks=(BASIC_TASK,))
    assert second.quota_blocked() is True
    await second._tick_once()
    assert client.calls == []


async def test_quota_block_clears_after_the_reset(async_store: EventStore) -> None:
    yesterday = utc_day(time.time() - 86400)
    await async_store.set_state(
        STATE_KEY_QUOTA,
        {"day": yesterday, "used": 50, "exhausted_until": time.time() - 10},
    )
    scheduler, client, writer = await build(async_store, tasks=(BASIC_TASK,))
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    assert scheduler.quota_blocked() is False
    assert scheduler.status()["used_today"] == 1
    assert len(client.calls) == 1


# -- retries -----------------------------------------------------------------
async def test_transient_failures_are_retried_then_give_up(
    async_store: EventStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr("bmw_collector.rest_scheduler.asyncio.sleep", fake_sleep)
    scheduler, client, writer = await build(
        async_store,
        tasks=(BASIC_TASK,),
        outcomes=[TransientError("503", status=503)] * 3,
    )
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    assert len(client.calls) == 3
    # Backoff grows, with jitter keeping the exact values loose.
    assert len(delays) == 2
    assert delays[1] > delays[0]
    assert scheduler.status()["used_today"] == 3


async def test_a_retry_succeeds_and_is_archived(
    async_store: EventStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("bmw_collector.rest_scheduler.asyncio.sleep", _no_sleep)
    scheduler, _, writer = await build(
        async_store,
        tasks=(BASIC_TASK,),
        outcomes=[TransientError("503", status=503), ok_response(b'{"retried":true}')],
    )
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    events = await _events(async_store)
    assert len(events) == 1
    assert events[0].payload == b'{"retried":true}'


async def test_retries_stop_rather_than_eat_the_reserve(
    async_store: EventStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("bmw_collector.rest_scheduler.asyncio.sleep", _no_sleep)
    await async_store.set_state(STATE_KEY_QUOTA, {"day": utc_day(), "used": 7})
    scheduler, client, writer = await build(
        async_store,
        tasks=(BASIC_TASK,),
        outcomes=[TransientError("503", status=503)] * 3,
        budget=10,
        reserve=2,
    )
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    # One attempt, then no budget to spare beyond the reserve.
    assert len(client.calls) == 1


async def test_a_rejected_request_is_not_retried(async_store: EventStore) -> None:
    scheduler, client, writer = await build(
        async_store, tasks=(BASIC_TASK,), outcomes=[CarDataError("404", status=404)]
    )
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    # Retrying a 404 would spend quota to be told the same thing again.
    assert len(client.calls) == 1
    incidents = await _incidents(async_store)
    assert any(i["kind"] == "rest_request_rejected" for i in incidents)


async def test_timeouts_are_recorded_as_incidents(
    async_store: EventStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("bmw_collector.rest_scheduler.asyncio.sleep", _no_sleep)
    scheduler, _, writer = await build(
        async_store,
        tasks=(BASIC_TASK,),
        outcomes=[TransientError("GET timed out")] * 3,
    )
    writer.start()
    await scheduler._tick_once()
    await writer.stop()
    incidents = await _incidents(async_store)
    assert any(i["kind"] == "rest_request_failed" for i in incidents)
    assert scheduler.last_error is not None


# -- one-shot ----------------------------------------------------------------
async def test_run_task_now_records_the_run(async_store: EventStore) -> None:
    scheduler, client, writer = await build(async_store, tasks=())
    writer.start()
    await scheduler.run_task_now(BASIC_TASK)
    await writer.stop()
    assert len(client.calls) == 1
    assert await async_store.count_events() == 1
    assert "basic_data" in (await async_store.get_state(STATE_KEY_TASK_RUNS))


async def test_run_task_now_refuses_when_quota_is_exhausted(async_store: EventStore) -> None:
    await async_store.set_state(
        STATE_KEY_QUOTA,
        {"day": utc_day(), "used": 50, "exhausted_until": time.time() + 3600},
    )
    scheduler, client, _ = await build(async_store, tasks=())
    with pytest.raises(QuotaExhaustedError):
        await scheduler.run_task_now(BASIC_TASK)
    assert client.calls == []


# -- lifecycle ---------------------------------------------------------------
async def test_disabled_scheduler_never_starts(async_store: EventStore) -> None:
    client = StubRestClient()
    writer = EventWriter(async_store)
    registry = VehicleRegistry(VehiclesConfig(vins=(VIN_A,)), async_store, account_key="test")
    scheduler = RestScheduler(
        RestConfig(enabled=False, tasks=()),
        client,
        writer,
        async_store,
        registry,
        account_key="test",
    )
    await scheduler.load_state()
    scheduler.start()
    await scheduler.stop()
    assert client.calls == []


async def _no_sleep(_seconds: float) -> None:
    """Skip backoff waits in tests."""
    return None


async def _events(store: EventStore) -> list[Any]:
    import asyncio

    return await asyncio.to_thread(lambda: list(store.iter_events_sync()))


async def _incidents(store: EventStore) -> list[dict[str, Any]]:
    import asyncio

    return await asyncio.to_thread(store.recent_incidents_sync)
