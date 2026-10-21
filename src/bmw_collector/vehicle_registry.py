"""Which VINs the collector may archive, and what it knows about them.

The registry resolves the configured allowlist (or ``all_primary``) against the
VINs BMW reports as mapped to the account. Being listed by BMW is not the same
as being collectable: a secondary mapping, a revoked consent or a vehicle that
simply never streams all look identical from here, so the registry reports what
it observed rather than asserting a vehicle is active.

Disabling a vehicle stops collection. It never deletes stored events.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from .config import VehiclesConfig
from .store import EventStore, RawEvent, utc_now_iso

_LOGGER = logging.getLogger(__name__)

STATE_KEY_MAPPINGS = "vehicle_mappings"


@dataclass(frozen=True, slots=True)
class VehicleMapping:
    """One VIN as reported by ``/customers/vehicles/mappings``."""

    vin: str
    mapping_type: str | None = None
    mapped_since: str | None = None

    @property
    def is_primary(self) -> bool:
        """Whether BMW reports this account as the vehicle's primary user."""
        return (self.mapping_type or "").upper() == "PRIMARY"


def parse_mappings(payload: bytes) -> list[VehicleMapping]:
    """Parse a mappings response body.

    BMW's schema documents a single object, but the endpoint returns a list in
    practice and may be wrapped; all three shapes are accepted so an unexpected
    envelope does not stall collection. The raw bytes are archived regardless.
    """
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValueError(f"vehicle mappings response is not JSON: {err}") from err

    items: list[Any]
    if isinstance(parsed, list):
        items = parsed
    elif isinstance(parsed, dict):
        for key in ("mappings", "vehicleMappings", "items", "content"):
            nested = parsed.get(key)
            if isinstance(nested, list):
                items = nested
                break
        else:
            items = [parsed]
    else:
        # A parse failure on BMW's response, not a caller type error.
        raise ValueError(  # noqa: TRY004
            "vehicle mappings response has an unexpected shape"
        )

    out: list[VehicleMapping] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        vin = item.get("vin")
        if not vin:
            continue
        out.append(
            VehicleMapping(
                vin=str(vin).strip().upper(),
                mapping_type=(str(item["mappingType"]) if item.get("mappingType") else None),
                mapped_since=(str(item["mappedSince"]) if item.get("mappedSince") else None),
            )
        )
    return out


class VehicleRegistry:
    """Resolves and remembers the set of VINs to collect."""

    def __init__(self, config: VehiclesConfig, store: EventStore, *, account_key: str) -> None:
        self._config = config
        self._store = store
        self._account_key = account_key
        self._mappings: dict[str, VehicleMapping] = {}
        self._disabled: set[str] = set()
        self._last_discovery: str | None = None

    @property
    def last_discovery_at(self) -> str | None:
        """When mappings were last refreshed from BMW."""
        return self._last_discovery

    @property
    def known_mappings(self) -> tuple[VehicleMapping, ...]:
        """Mappings as last reported by BMW."""
        return tuple(self._mappings.values())

    async def load_persisted(self) -> None:
        """Restore the last known mappings so a restart can subscribe at once.

        Without this the collector would have to spend REST quota before it
        could subscribe to anything.
        """
        cached = await self._store.get_state(STATE_KEY_MAPPINGS)
        if isinstance(cached, dict) and isinstance(cached.get("vehicles"), list):
            for item in cached["vehicles"]:
                if isinstance(item, dict) and item.get("vin"):
                    mapping = VehicleMapping(
                        vin=str(item["vin"]),
                        mapping_type=item.get("mapping_type"),
                        mapped_since=item.get("mapped_since"),
                    )
                    self._mappings[mapping.vin] = mapping
            self._last_discovery = cached.get("discovered_at")
        for row in await self._store.list_vehicles():
            if not row["collection_enabled"]:
                self._disabled.add(row["vin"])

    async def update_from_response(self, payload: bytes) -> list[VehicleMapping]:
        """Apply a freshly fetched mappings payload."""
        mappings = parse_mappings(payload)
        self._mappings = {m.vin: m for m in mappings}
        self._last_discovery = utc_now_iso()
        await self._store.set_state(
            STATE_KEY_MAPPINGS,
            {
                "discovered_at": self._last_discovery,
                "vehicles": [
                    {
                        "vin": m.vin,
                        "mapping_type": m.mapping_type,
                        "mapped_since": m.mapped_since,
                    }
                    for m in mappings
                ],
            },
        )
        for mapping in mappings:
            await self._store.upsert_vehicle(
                mapping.vin,
                account_key=self._account_key,
                mapping_type=mapping.mapping_type,
                mapped_since=mapping.mapped_since,
            )
        unknown = sorted(set(self._config.vins) - set(self._mappings))
        if unknown:
            _LOGGER.warning(
                "%d configured VIN(s) are not mapped to this BMW account and cannot be "
                "collected; check the vehicle is linked in the BMW portal",
                len(unknown),
                extra={"unmapped_count": len(unknown)},
            )
            await self._store.record_incident("configured_vin_not_mapped", {"count": len(unknown)})
        return mappings

    def active_vins(self) -> tuple[str, ...]:
        """VINs to collect right now, in a stable order."""
        if self._config.mode == "allowlist":
            candidates = list(self._config.vins)
        else:
            candidates = sorted(m.vin for m in self._mappings.values() if m.is_primary)
        return tuple(v for v in candidates if v not in self._disabled)

    def subscribable_vins(self) -> tuple[str, ...]:
        """Active VINs BMW has confirmed as mapped.

        Subscribing to an unmapped VIN is refused by the broker, which in MQTT
        looks like a per-topic failure rather than a connection error, so the
        receiver only asks for topics that have a chance of succeeding. Until
        discovery has run even once, the configured allowlist is used as-is.
        """
        active = self.active_vins()
        if not self._mappings:
            return active
        return tuple(v for v in active if v in self._mappings)

    async def set_enabled(self, vin: str, enabled: bool) -> None:
        """Enable or disable collection for a VIN. History is never touched."""
        vin = vin.strip().upper()
        if enabled:
            self._disabled.discard(vin)
        else:
            self._disabled.add(vin)
        await self._store.upsert_vehicle(
            vin, account_key=self._account_key, collection_enabled=enabled
        )
        _LOGGER.info(
            "collection %s for a vehicle",
            "enabled" if enabled else "disabled",
            extra={"collection_enabled": enabled},
        )

    def mapping_event(self, payload: bytes, metadata: dict[str, Any]) -> RawEvent:
        """Wrap a mappings response for archival."""
        return RawEvent(
            source="rest",
            account_key=self._account_key,
            payload=payload,
            metadata={**metadata, "task": "vehicle_mapping"},
        )
