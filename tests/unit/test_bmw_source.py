"""REST client: raw-byte capture, metadata safety, error classification."""

from __future__ import annotations

import json

import aiohttp
import pytest

from bmw_collector.bmw_source import (
    CarDataError,
    CarDataRestClient,
    QuotaExhaustedError,
    RestResponse,
    TransientError,
)
from bmw_collector.const import API_VERSION, HEADER_X_VERSION
from tests.fakes import FakeResponse, FakeSession

VIN = "WBA12345678901234"
# Bytes that are neither valid UTF-8 nor valid JSON.
BINARY_BODY = b"\x89PNG\r\n\x1a\n\xff\xfe"


class StubTokens:
    """Token provider stand-in."""

    def __init__(self, token: str = "test-access-token") -> None:
        self.token = token
        self.calls = 0

    async def access_token(self) -> str:
        """Return the canned token."""
        self.calls += 1
        return self.token


def make_client(*responses: FakeResponse | Exception) -> tuple[CarDataRestClient, FakeSession]:
    """A REST client wired to a scripted session."""
    session = FakeSession(list(responses))
    return CarDataRestClient(StubTokens(), session=session), session


async def test_successful_response_is_captured_byte_for_byte() -> None:
    body = json.dumps({"vin": VIN, "unexpected_future_field": [1, 2, 3]}).encode()
    client, _ = make_client(FakeResponse(status=200, body=body))
    response = await client.get_basic_data(VIN)
    assert response.body == body
    assert response.ok is True


async def test_non_utf8_body_is_captured_unchanged() -> None:
    client, _ = make_client(FakeResponse(status=200, body=BINARY_BODY))
    response = await client.get_basic_data(VIN)
    assert response.body == BINARY_BODY


async def test_request_carries_the_bearer_token_and_version_header() -> None:
    client, session = make_client(FakeResponse(status=200, body=b"{}"))
    await client.get_vehicle_mappings()
    headers = session.last_request.headers
    assert headers["Authorization"] == "Bearer test-access-token"
    # BMW rejects the call outright without the version header.
    assert headers[HEADER_X_VERSION] == API_VERSION


async def test_endpoints_use_the_documented_paths() -> None:
    client, session = make_client(
        FakeResponse(body=b"{}"),
        FakeResponse(body=b"{}"),
        FakeResponse(body=b"{}"),
        FakeResponse(body=b"{}"),
        FakeResponse(body=b"{}"),
    )
    await client.get_vehicle_mappings()
    assert session.last_request.url.endswith("/customers/vehicles/mappings")
    await client.get_basic_data(VIN)
    assert session.last_request.url.endswith(f"/customers/vehicles/{VIN}/basicData")
    await client.get_telematic_data(VIN, "container-9")
    assert session.last_request.url.endswith(f"/customers/vehicles/{VIN}/telematicData")
    assert session.last_request.params == {"containerId": "container-9"}
    await client.get_tyre_diagnosis(VIN)
    assert session.last_request.url.endswith("/smartMaintenanceTyreDiagnosis")
    await client.list_containers()
    assert session.last_request.url.endswith("/customers/containers")


async def test_create_container_sends_the_documented_field_names() -> None:
    client, session = make_client(FakeResponse(status=201, body=b"{}"))
    await client.create_container(
        name="archive", purpose="archival", technical_descriptors=["a.b.c"]
    )
    assert session.last_request.json == {
        "name": "archive",
        "purpose": "archival",
        "technicalDescriptors": ["a.b.c"],
    }


async def test_metadata_keeps_only_safe_headers() -> None:
    client, _ = make_client(
        FakeResponse(
            status=200,
            body=b"{}",
            headers={
                "Content-Type": "application/json",
                "x-correlation-id": "corr-1",
                "Authorization": "Bearer leaked-token",
                "Set-Cookie": "session=leaked",
                "X-Internal-Debug": "some internal detail",
            },
        )
    )
    response = await client.get_basic_data(VIN)
    metadata = response.metadata(task="basic_data")
    serialized = json.dumps(metadata)
    # Allowlisted, so credentials cannot leak via a header we never thought of.
    assert metadata["http"]["response_headers"] == {
        "content-type": "application/json",
        "x-correlation-id": "corr-1",
    }
    assert "leaked-token" not in serialized
    assert "leaked" not in serialized
    assert "internal detail" not in serialized


async def test_metadata_records_the_request_without_its_query_string() -> None:
    client, _ = make_client(FakeResponse(status=200, body=b"{}"))
    response = await client.get_telematic_data(VIN, "container-9")
    metadata = response.metadata()
    assert metadata["http"]["method"] == "GET"
    assert "?" not in metadata["http"]["url"]
    assert metadata["http"]["status"] == 200
    assert metadata["payload_bytes"] == 2


async def test_quota_rejection_is_classified_distinctly() -> None:
    client, _ = make_client(
        FakeResponse(
            status=403,
            payload={"exveErrorId": "CU-429", "exveErrorMsg": "API rate limit reached"},
        )
    )
    with pytest.raises(QuotaExhaustedError) as excinfo:
        await client.get_basic_data(VIN)
    assert excinfo.value.error_id == "CU-429"
    assert excinfo.value.status == 403


async def test_other_403_is_not_treated_as_quota_exhaustion() -> None:
    client, _ = make_client(
        FakeResponse(status=403, payload={"exveErrorId": "CU-001", "exveErrorMsg": "no consent"})
    )
    # Mistaking a consent problem for quota exhaustion would park REST for the
    # rest of the day for no reason.
    with pytest.raises(CarDataError) as excinfo:
        await client.get_basic_data(VIN)
    assert not isinstance(excinfo.value, QuotaExhaustedError)


@pytest.mark.parametrize("status", [429, 500, 502, 503])
async def test_retryable_statuses_raise_transient(status: int) -> None:
    client, _ = make_client(FakeResponse(status=status, body=b"{}"))
    with pytest.raises(TransientError):
        await client.get_basic_data(VIN)


@pytest.mark.parametrize("status", [400, 401, 404])
async def test_client_errors_are_not_retryable(status: int) -> None:
    client, _ = make_client(FakeResponse(status=status, body=b"{}"))
    with pytest.raises(CarDataError) as excinfo:
        await client.get_basic_data(VIN)
    assert not isinstance(excinfo.value, TransientError)


async def test_timeout_is_transient() -> None:
    client, _ = make_client(TimeoutError())
    with pytest.raises(TransientError, match="timed out"):
        await client.get_basic_data(VIN)


async def test_connection_error_is_transient_and_names_the_class() -> None:
    client, _ = make_client(aiohttp.ClientConnectorError(None, OSError("refused")))  # type: ignore[arg-type]
    with pytest.raises(TransientError, match="ClientConnectorError"):
        await client.get_basic_data(VIN)


async def test_error_id_tolerates_a_non_json_error_body() -> None:
    response = RestResponse(method="GET", url="https://x.invalid/y", status=500, body=BINARY_BODY)
    assert response.error_id() == ""


async def test_error_id_tolerates_a_json_array_body() -> None:
    response = RestResponse(method="GET", url="https://x.invalid/y", status=500, body=b"[1,2]")
    assert response.error_id() == ""


async def test_injected_session_is_not_closed_by_the_client() -> None:
    client, session = make_client()
    await client.close()
    assert session.closed is False


async def test_a_token_is_fetched_for_every_request() -> None:
    tokens = StubTokens()
    session = FakeSession([FakeResponse(body=b"{}"), FakeResponse(body=b"{}")])
    client = CarDataRestClient(tokens, session=session)
    await client.get_vehicle_mappings()
    await client.get_vehicle_mappings()
    # Asking the manager each time is what lets it refresh transparently.
    assert tokens.calls == 2
