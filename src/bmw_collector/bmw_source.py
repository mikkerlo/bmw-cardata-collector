"""Thin REST client for the BMW CarData customer API.

Every call returns the response's **original bytes** plus redacted metadata.
Nothing on this path is parsed into models: the archive must contain exactly
what BMW sent, including fields this version of the collector knows nothing
about. Callers that need a value (the vehicle registry, for instance) parse a
copy of those bytes themselves.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

import aiohttp

from .const import API_VERSION, CARDATA_API_HOST, HEADER_X_VERSION, QUOTA_ERROR_ID
from .logging_setup import SENSITIVE_KEYS

_LOGGER = logging.getLogger(__name__)

#: Response headers worth archiving for diagnostics. Everything else is
#: dropped rather than filtered, so a new credential-bearing header cannot
#: leak into storage by default.
_KEPT_HEADERS = frozenset(
    {
        "content-type",
        "content-length",
        "date",
        "x-correlation-id",
        "x-request-id",
        "retry-after",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
    }
)


class CarDataError(Exception):
    """A CarData REST call failed."""

    def __init__(self, message: str, *, status: int | None = None, error_id: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.error_id = error_id


class QuotaExhaustedError(CarDataError):
    """BMW rejected the call because the daily request quota is used up."""


class TransientError(CarDataError):
    """A retryable failure (network, timeout, 429, 5xx)."""


@dataclass(slots=True)
class RestResponse:
    """A captured REST response: original bytes plus safe metadata."""

    method: str
    url: str
    status: int
    body: bytes
    headers: dict[str, str] = field(default_factory=dict)
    request_id: str | None = None

    @property
    def ok(self) -> bool:
        """Whether the status is a 2xx."""
        return 200 <= self.status < 300

    def metadata(self, **extra: Any) -> dict[str, Any]:
        """Metadata dict to store alongside the payload."""
        meta: dict[str, Any] = {
            "http": {
                "method": self.method,
                "url": self.url,
                "status": self.status,
                "response_headers": self.headers,
            },
            "payload_bytes": len(self.body),
        }
        if self.request_id:
            meta["http"]["request_id"] = self.request_id
        meta.update(extra)
        return meta

    def json(self) -> Any:
        """Parse the body as JSON, for callers that need a value from it."""
        return json.loads(self.body.decode("utf-8"))

    def error_id(self) -> str:
        """BMW's ``exveErrorId`` from an error body, if present."""
        try:
            parsed = self.json()
        except (UnicodeDecodeError, json.JSONDecodeError):
            return ""
        return str(parsed.get("exveErrorId", "")) if isinstance(parsed, dict) else ""


class TokenProvider(Protocol):
    """Supplies a bearer token; implemented by :class:`~bmw_collector.auth.AuthManager`."""

    async def access_token(self) -> str:
        """Return a currently valid access token."""


def _safe_headers(headers: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in headers.items():
        lowered = key.lower()
        if lowered in _KEPT_HEADERS and lowered not in SENSITIVE_KEYS:
            out[lowered] = str(value)
    return out


class CarDataRestClient:
    """Issues CarData REST calls and hands back raw responses."""

    def __init__(
        self,
        token_provider: TokenProvider,
        *,
        session: aiohttp.ClientSession | None = None,
        base_url: str = CARDATA_API_HOST,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._tokens = token_provider
        self._session = session
        self._owns_session = session is None
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        """Close the HTTP session if this client created it."""
        if self._owns_session and self._session is not None and not self._session.closed:
            await self._session.close()

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: Any | None = None,
    ) -> RestResponse:
        """Perform one request and capture it.

        Every call consumes BMW quota, including the ones that fail, so this
        method never retries on its own: that policy lives in the scheduler.
        """
        token = await self._tokens.access_token()
        session = await self._ensure_session()
        url = f"{self._base_url}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            HEADER_X_VERSION: API_VERSION,
            "Accept": "application/json",
        }
        try:
            async with session.request(
                method, url, params=params, json=json_body, headers=headers
            ) as response:
                body = await response.read()
                captured = RestResponse(
                    method=method,
                    url=str(response.url.with_query(None)),
                    status=response.status,
                    body=body,
                    headers=_safe_headers(response.headers),
                    request_id=response.headers.get("x-correlation-id"),
                )
        except TimeoutError as err:
            raise TransientError(f"{method} {path} timed out") from err
        except aiohttp.ClientError as err:
            raise TransientError(f"{method} {path} failed: {type(err).__name__}") from err

        if captured.ok:
            return captured

        error_id = captured.error_id()
        if captured.status == 403 and QUOTA_ERROR_ID in error_id:
            raise QuotaExhaustedError(
                f"{method} {path}: BMW daily REST quota exhausted ({error_id})",
                status=403,
                error_id=error_id,
            )
        if captured.status == 429 or captured.status >= 500:
            raise TransientError(
                f"{method} {path}: HTTP {captured.status}",
                status=captured.status,
                error_id=error_id,
            )
        raise CarDataError(
            f"{method} {path}: HTTP {captured.status}" + (f" ({error_id})" if error_id else ""),
            status=captured.status,
            error_id=error_id,
        )

    # -- endpoints we use ---------------------------------------------------
    async def get_vehicle_mappings(self) -> RestResponse:
        """``GET /customers/vehicles/mappings`` — VINs mapped to this account."""
        return await self.request("GET", "/customers/vehicles/mappings")

    async def get_basic_data(self, vin: str) -> RestResponse:
        """``GET /customers/vehicles/{vin}/basicData``."""
        return await self.request("GET", f"/customers/vehicles/{vin}/basicData")

    async def get_telematic_data(self, vin: str, container_id: str) -> RestResponse:
        """``GET /customers/vehicles/{vin}/telematicData`` for one container."""
        return await self.request(
            "GET",
            f"/customers/vehicles/{vin}/telematicData",
            params={"containerId": container_id},
        )

    async def get_tyre_diagnosis(self, vin: str) -> RestResponse:
        """``GET /customers/vehicles/{vin}/smartMaintenanceTyreDiagnosis``."""
        return await self.request("GET", f"/customers/vehicles/{vin}/smartMaintenanceTyreDiagnosis")

    async def list_containers(self) -> RestResponse:
        """``GET /customers/containers``."""
        return await self.request("GET", "/customers/containers")

    async def create_container(
        self, *, name: str, purpose: str, technical_descriptors: list[str]
    ) -> RestResponse:
        """``POST /customers/containers`` — define a telematic data container."""
        return await self.request(
            "POST",
            "/customers/containers",
            json_body={
                "name": name,
                "purpose": purpose,
                "technicalDescriptors": technical_descriptors,
            },
        )
