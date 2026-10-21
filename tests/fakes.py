"""HTTP test doubles.

Both :class:`~bmw_collector.auth.AuthManager` and
:class:`~bmw_collector.bmw_source.CarDataRestClient` accept an injected
``aiohttp.ClientSession``, so tests substitute this instead of patching
aiohttp internals. That keeps the suite independent of aiohttp's private
response machinery, which is what made the aioresponses route brittle.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Self


class FakeResponseError(Exception):
    """Raised when a test asks for more requests than it scripted."""


@dataclass
class FakeResponse:
    """One scripted HTTP response."""

    status: int = 200
    payload: Any | None = None
    body: bytes | None = None
    headers: dict[str, str] = field(default_factory=dict)
    raise_on_read: Exception | None = None
    url: str = "https://example.invalid/"

    def _bytes(self) -> bytes:
        if self.body is not None:
            return self.body
        if self.payload is None:
            return b""
        return json.dumps(self.payload).encode()

    async def json(self, content_type: str | None = None) -> Any:
        """Parse the body as JSON, like aiohttp's response.json()."""
        del content_type
        if self.raise_on_read is not None:
            raise self.raise_on_read
        return json.loads(self._bytes().decode())

    async def text(self) -> str:
        """Body as text."""
        if self.raise_on_read is not None:
            raise self.raise_on_read
        return self._bytes().decode(errors="replace")

    async def read(self) -> bytes:
        """Body as the original bytes."""
        if self.raise_on_read is not None:
            raise self.raise_on_read
        return self._bytes()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None


class _FakeUrl(str):
    """Minimal stand-in for yarl.URL, enough for the client's use of it."""

    def with_query(self, _query: Any) -> _FakeUrl:
        """Return the URL without its query string."""
        return _FakeUrl(self.split("?", 1)[0])


@dataclass
class RecordedRequest:
    """What the code under test actually sent."""

    method: str
    url: str
    data: Any = None
    json: Any = None
    params: dict[str, str] | None = None
    headers: dict[str, str] = field(default_factory=dict)


class FakeSession:
    """Scripted ``aiohttp.ClientSession`` replacement.

    Responses are consumed in order. A response may also be an exception
    instance, which is raised instead — that is how timeouts and connection
    errors are exercised.
    """

    def __init__(self, responses: list[FakeResponse | Exception] | None = None) -> None:
        self.responses: list[FakeResponse | Exception] = list(responses or [])
        self.requests: list[RecordedRequest] = []
        self.closed = False

    def queue(self, *responses: FakeResponse | Exception) -> None:
        """Append more scripted responses."""
        self.responses.extend(responses)

    def _next(self, recorded: RecordedRequest) -> FakeResponse:
        self.requests.append(recorded)
        if not self.responses:
            raise FakeResponseError(
                f"unscripted {recorded.method} {recorded.url} (request #{len(self.requests)})"
            )
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        nxt.url = recorded.url
        return nxt

    def post(
        self,
        url: str,
        *,
        data: Any = None,
        json: Any = None,
        headers: dict[str, str] | None = None,
        **_kwargs: Any,
    ) -> FakeResponse:
        """Record and answer a POST."""
        return self._next(RecordedRequest("POST", url, data=data, json=json, headers=headers or {}))

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, str] | None = None,
        json: Any = None,
        data: Any = None,
        headers: dict[str, str] | None = None,
        **_kwargs: Any,
    ) -> FakeResponse:
        """Record and answer any request."""
        response = self._next(
            RecordedRequest(method, url, data=data, json=json, params=params, headers=headers or {})
        )
        response.url = _FakeUrl(url)  # type: ignore[assignment]
        return response

    async def close(self) -> None:
        """Mark the session closed."""
        self.closed = True

    @property
    def last_request(self) -> RecordedRequest:
        """The most recent recorded request."""
        return self.requests[-1]
