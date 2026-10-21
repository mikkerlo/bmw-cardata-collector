"""Vehicle discovery, allowlist resolution and enable/disable semantics."""

from __future__ import annotations

import json

import pytest

from bmw_collector.config import VehiclesConfig
from bmw_collector.store import EventStore
from bmw_collector.vehicle_registry import VehicleRegistry, parse_mappings

VIN_A = "WBA00000000000001"
VIN_B = "WBA00000000000002"
VIN_C = "WBA00000000000003"


def mappings_payload(*entries: dict[str, str]) -> bytes:
    """A mappings response body."""
    return json.dumps(list(entries)).encode()


def primary(vin: str) -> dict[str, str]:
    """A PRIMARY mapping entry."""
    return {"vin": vin, "mappingType": "PRIMARY", "mappedSince": "2026-01-01T00:00:00Z"}


def secondary(vin: str) -> dict[str, str]:
    """A SECONDARY mapping entry."""
    return {"vin": vin, "mappingType": "SECONDARY", "mappedSince": "2026-01-01T00:00:00Z"}


def make_registry(store: EventStore, **kwargs: object) -> VehicleRegistry:
    """A registry over the given store."""
    config = VehiclesConfig(**kwargs)  # type: ignore[arg-type]
    return VehicleRegistry(config, store, account_key="test")


# -- parsing -----------------------------------------------------------------
def test_parses_a_json_list() -> None:
    parsed = parse_mappings(mappings_payload(primary(VIN_A), secondary(VIN_B)))
    assert [m.vin for m in parsed] == [VIN_A, VIN_B]
    assert parsed[0].is_primary is True
    assert parsed[1].is_primary is False
    assert parsed[0].mapped_since == "2026-01-01T00:00:00Z"


def test_parses_a_single_object() -> None:
    # BMW's schema documents one object rather than a list.
    assert [m.vin for m in parse_mappings(json.dumps(primary(VIN_A)).encode())] == [VIN_A]


@pytest.mark.parametrize("key", ["mappings", "vehicleMappings", "items", "content"])
def test_parses_a_wrapped_list(key: str) -> None:
    payload = json.dumps({key: [primary(VIN_A)]}).encode()
    assert [m.vin for m in parse_mappings(payload)] == [VIN_A]


def test_vins_are_normalised_to_upper_case() -> None:
    assert parse_mappings(json.dumps([{"vin": VIN_A.lower()}]).encode())[0].vin == VIN_A


def test_entries_without_a_vin_are_skipped() -> None:
    payload = json.dumps([{"mappingType": "PRIMARY"}, primary(VIN_A), "junk"]).encode()
    assert [m.vin for m in parse_mappings(payload)] == [VIN_A]


def test_missing_mapping_type_is_not_assumed_primary() -> None:
    # Guessing PRIMARY would make all_primary mode collect cars it should not.
    assert parse_mappings(json.dumps([{"vin": VIN_A}]).encode())[0].is_primary is False


def test_non_json_payload_is_reported() -> None:
    with pytest.raises(ValueError, match="not JSON"):
        parse_mappings(b"\xff\xfe not json")


def test_unexpected_shape_is_reported() -> None:
    with pytest.raises(ValueError, match="unexpected shape"):
        parse_mappings(b"42")


# -- allowlist resolution ----------------------------------------------------
async def test_allowlist_mode_collects_exactly_the_configured_vins(
    async_store: EventStore,
) -> None:
    registry = make_registry(async_store, mode="allowlist", vins=(VIN_A,))
    await registry.update_from_response(mappings_payload(primary(VIN_A), primary(VIN_B)))
    assert registry.active_vins() == (VIN_A,)


async def test_all_primary_mode_skips_secondary_mappings(async_store: EventStore) -> None:
    registry = make_registry(async_store, mode="all_primary")
    await registry.update_from_response(
        mappings_payload(primary(VIN_B), secondary(VIN_C), primary(VIN_A))
    )
    # Sorted, so topic subscription order is stable across restarts.
    assert registry.active_vins() == (VIN_A, VIN_B)


async def test_configured_vin_that_is_not_mapped_is_flagged(async_store: EventStore) -> None:
    registry = make_registry(async_store, mode="allowlist", vins=(VIN_A, VIN_C))
    await registry.update_from_response(mappings_payload(primary(VIN_A)))
    incidents = await asyncio_to_thread_incidents(async_store)
    assert incidents[0]["kind"] == "configured_vin_not_mapped"
    assert incidents[0]["details"]["count"] == 1
    # No VIN in the incident row: it is read back by operators and tooling.
    assert VIN_C not in json.dumps(incidents[0])


async def asyncio_to_thread_incidents(store: EventStore) -> list[dict[str, object]]:
    """Read incidents from the async test context."""
    import asyncio

    return await asyncio.to_thread(store.recent_incidents_sync)


async def test_subscribable_vins_exclude_unmapped_ones(async_store: EventStore) -> None:
    registry = make_registry(async_store, mode="allowlist", vins=(VIN_A, VIN_C))
    await registry.update_from_response(mappings_payload(primary(VIN_A)))
    # Subscribing to an unmapped VIN is refused by the broker per topic, so it
    # is not even attempted.
    assert registry.subscribable_vins() == (VIN_A,)
    assert registry.active_vins() == (VIN_A, VIN_C)


async def test_subscribable_falls_back_to_the_allowlist_before_discovery(
    async_store: EventStore,
) -> None:
    registry = make_registry(async_store, mode="allowlist", vins=(VIN_A,))
    # No discovery yet: using the allowlist lets a restart subscribe at once
    # rather than waiting to spend REST quota.
    assert registry.subscribable_vins() == (VIN_A,)


# -- persistence -------------------------------------------------------------
async def test_mappings_are_cached_for_the_next_start(async_store: EventStore) -> None:
    first = make_registry(async_store, mode="all_primary")
    await first.update_from_response(mappings_payload(primary(VIN_A)))

    second = make_registry(async_store, mode="all_primary")
    await second.load_persisted()
    assert second.active_vins() == (VIN_A,)
    assert second.last_discovery_at is not None


async def test_first_observed_time_is_recorded_and_kept(async_store: EventStore) -> None:
    registry = make_registry(async_store, mode="all_primary")
    await registry.update_from_response(mappings_payload(primary(VIN_A)))
    first_seen = (await async_store.list_vehicles())[0]["first_observed_at_utc"]
    await registry.update_from_response(mappings_payload(primary(VIN_A)))
    row = (await async_store.list_vehicles())[0]
    assert row["first_observed_at_utc"] == first_seen
    assert row["mapping_type"] == "PRIMARY"


async def test_disabling_stops_collection_and_survives_a_restart(
    async_store: EventStore,
) -> None:
    registry = make_registry(async_store, mode="allowlist", vins=(VIN_A, VIN_B))
    await registry.update_from_response(mappings_payload(primary(VIN_A), primary(VIN_B)))
    await registry.set_enabled(VIN_B, False)
    assert registry.active_vins() == (VIN_A,)

    restarted = make_registry(async_store, mode="allowlist", vins=(VIN_A, VIN_B))
    await restarted.load_persisted()
    assert restarted.active_vins() == (VIN_A,)


async def test_disabling_never_deletes_history(async_store: EventStore) -> None:
    from bmw_collector.store import RawEvent

    await async_store.append_event(
        RawEvent(source="mqtt", account_key="test", payload=b"{}", vin=VIN_B)
    )
    registry = make_registry(async_store, mode="allowlist", vins=(VIN_A, VIN_B))
    await registry.set_enabled(VIN_B, False)
    assert await async_store.count_events() == 1
    # The vehicle row survives too, so its first-observed time is not lost.
    assert any(v["vin"] == VIN_B for v in await async_store.list_vehicles())


async def test_re_enabling_resumes_collection(async_store: EventStore) -> None:
    registry = make_registry(async_store, mode="allowlist", vins=(VIN_A,))
    await registry.set_enabled(VIN_A, False)
    assert registry.active_vins() == ()
    await registry.set_enabled(VIN_A, True)
    assert registry.active_vins() == (VIN_A,)


async def test_mapping_event_is_tagged_for_the_archive(async_store: EventStore) -> None:
    registry = make_registry(async_store, mode="all_primary")
    payload = mappings_payload(primary(VIN_A))
    event = registry.mapping_event(payload, {"http": {"status": 200}})
    assert event.source == "rest"
    assert event.payload == payload
    assert event.metadata["task"] == "vehicle_mapping"


async def test_empty_mapping_response_is_handled(async_store: EventStore) -> None:
    registry = make_registry(async_store, mode="all_primary")
    assert await registry.update_from_response(b"[]") == []
    assert registry.active_vins() == ()
