"""Device-code flow, token refresh and credential durability.

Every HTTP interaction is scripted through :class:`tests.fakes.FakeSession`:
no BMW account, no network, and no real token anywhere in this file.
"""

from __future__ import annotations

import base64
import json
import os
import time
from pathlib import Path
from typing import Any

import pytest

from bmw_collector.auth import (
    REFRESH_SKEW_SECONDS,
    AuthError,
    AuthManager,
    CredentialStore,
    DeviceCodeGrant,
    ReauthorizationRequiredError,
    TokenSet,
    _decode_jwt_exp,
    generate_pkce_pair,
)
from bmw_collector.const import DEVICE_CODE_PATH, TOKEN_PATH
from tests.fakes import FakeResponse, FakeSession

# Values below are fabricated for tests and authorize nothing.
FAKE_GCID = "aa111192-d570-4d3d-8835-656ee00edcd7"


def fake_jwt(exp: float) -> str:
    """A syntactically valid, unsigned JWT carrying the given ``exp`` claim."""
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    body = base64.urlsafe_b64encode(json.dumps({"exp": int(exp)}).encode()).rstrip(b"=").decode()
    return f"{header}.{body}.signature"


def token_response(**overrides: Any) -> dict[str, Any]:
    """A successful token payload."""
    payload = {
        "access_token": "access-1",
        "refresh_token": "refresh-1",
        "token_type": "Bearer",
        "expires_in": 3599,
        "scope": "authenticate_user openid cardata:api:read cardata:streaming:read",
        "gcid": FAKE_GCID,
        "id_token": fake_jwt(time.time() + 3600),
    }
    payload.update(overrides)
    return payload


def ok(payload: Any) -> FakeResponse:
    """A 200 response carrying ``payload`` as JSON."""
    return FakeResponse(status=200, payload=payload)


def err(status: int, error: str, description: str = "") -> FakeResponse:
    """An OAuth error response."""
    return FakeResponse(status=status, payload={"error": error, "error_description": description})


def make_manager(
    credentials_path: Path, *responses: FakeResponse | Exception
) -> tuple[AuthManager, FakeSession]:
    """An AuthManager wired to a scripted session."""
    session = FakeSession(list(responses))
    return AuthManager("test-client", credentials_path, session=session), session


def grant(expires_in: int = 300) -> DeviceCodeGrant:
    """A device-code grant with no polling delay."""
    return DeviceCodeGrant(
        device_code="device-1",
        user_code="ABCD1234",
        verification_uri="https://customer.bmwgroup.com/oneid/link",
        expires_in=expires_in,
        interval=0,
    )


def save_credential(path: Path, refresh_token: str = "stored-refresh") -> None:
    """Pre-seed a stored refresh credential."""
    CredentialStore(path).save(refresh_token=refresh_token, gcid=FAKE_GCID, scope=("openid",))


@pytest.fixture
def credentials_path(tmp_path: Path) -> Path:
    """Where the refresh credential is written in tests."""
    return tmp_path / "secrets" / "oauth.json"


@pytest.fixture
def slept(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record poll delays instead of really waiting them out."""
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr("bmw_collector.auth.asyncio.sleep", fake_sleep)
    return delays


# -- PKCE --------------------------------------------------------------------
def test_pkce_pair_is_random_and_uses_s256() -> None:
    import hashlib

    verifier, challenge = generate_pkce_pair()
    expected = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    )
    assert challenge == expected
    assert generate_pkce_pair()[0] != verifier
    # Unpadded base64url, as the spec requires.
    assert "=" not in verifier and "=" not in challenge


# -- device code flow --------------------------------------------------------
async def test_device_flow_returns_the_codes_to_show_the_operator(
    credentials_path: Path,
) -> None:
    manager, _ = make_manager(
        credentials_path,
        ok(
            {
                "device_code": "device-1",
                "user_code": "ABCD1234",
                "verification_uri": "https://customer.bmwgroup.com/oneid/link",
                "expires_in": 300,
                "interval": 5,
            }
        ),
    )
    result = await manager.start_device_flow()
    assert result.user_code == "ABCD1234"
    assert result.verification_uri.endswith("/oneid/link")
    assert result.interval == 5


async def test_device_flow_sends_pkce_and_the_required_scopes(credentials_path: Path) -> None:
    manager, session = make_manager(
        credentials_path, ok({"device_code": "d", "user_code": "u", "expires_in": 300})
    )
    await manager.start_device_flow()
    request = session.last_request
    assert request.url.endswith(DEVICE_CODE_PATH)
    assert request.data["code_challenge_method"] == "S256"
    assert request.data["response_type"] == "device_code"
    assert "cardata:api:read" in request.data["scope"]
    assert "cardata:streaming:read" in request.data["scope"]
    # Without openid there is no id_token, and so no MQTT streaming.
    assert "openid" in request.data["scope"]


async def test_polling_tolerates_authorization_pending_then_succeeds(
    credentials_path: Path, slept: list[float]
) -> None:
    manager, session = make_manager(
        credentials_path,
        err(403, "authorization_pending"),
        err(403, "authorization_pending"),
        ok(token_response()),
    )
    manager._code_verifier = "verifier"
    tokens = await manager.poll_device_flow(grant())
    assert tokens.gcid == FAKE_GCID
    assert tokens.id_token is not None
    assert len(session.requests) == 3
    assert session.last_request.url.endswith(TOKEN_PATH)
    # authorization_pending keeps the interval as issued (floored at 1s); only
    # slow_down widens it.
    assert slept == [1, 1]


async def test_polling_backs_off_when_told_to_slow_down(
    credentials_path: Path, slept: list[float]
) -> None:
    manager, _ = make_manager(
        credentials_path,
        err(400, "slow_down"),
        err(400, "slow_down"),
        ok(token_response()),
    )
    manager._code_verifier = "verifier"
    tokens = await manager.poll_device_flow(grant())
    assert tokens.access_token == "access-1"
    # Each slow_down widens the interval by 5s, per RFC 8628. The base is the
    # 1s floor applied to the grant's interval.
    assert slept == [6, 11]


async def test_user_denial_stops_polling_immediately(credentials_path: Path) -> None:
    manager, session = make_manager(credentials_path, err(403, "access_denied"))
    manager._code_verifier = "verifier"
    with pytest.raises(AuthError) as excinfo:
        await manager.poll_device_flow(grant())
    assert excinfo.value.error == "access_denied"
    # One attempt only: polling on after a refusal would be abusive.
    assert len(session.requests) == 1


async def test_polling_without_starting_the_flow_is_refused(credentials_path: Path) -> None:
    manager, _ = make_manager(credentials_path)
    with pytest.raises(AuthError, match="missing_code_verifier"):
        await manager.poll_device_flow(grant())


async def test_expired_device_code_is_reported(credentials_path: Path) -> None:
    manager, _ = make_manager(credentials_path, err(403, "authorization_pending"))
    manager._code_verifier = "verifier"
    with pytest.raises(AuthError) as excinfo:
        await manager.poll_device_flow(grant(expires_in=0))
    assert excinfo.value.error == "expired_token"


# -- credential persistence --------------------------------------------------
async def test_successful_flow_persists_only_the_refresh_token(credentials_path: Path) -> None:
    manager, _ = make_manager(credentials_path, ok(token_response()))
    manager._code_verifier = "verifier"
    await manager.poll_device_flow(grant())

    stored = json.loads(credentials_path.read_text(encoding="utf-8"))
    assert stored["refresh_token"] == "refresh-1"
    # An access or id token on disk would be a liability with no benefit.
    assert "access_token" not in stored
    assert "id_token" not in stored


def test_credential_file_and_directory_are_owner_only(credentials_path: Path) -> None:
    CredentialStore(credentials_path).save(refresh_token="r", gcid=FAKE_GCID, scope=("openid",))
    assert credentials_path.stat().st_mode & 0o777 == 0o600
    assert credentials_path.parent.stat().st_mode & 0o077 == 0


def test_credential_save_replaces_atomically(credentials_path: Path) -> None:
    store = CredentialStore(credentials_path)
    store.save(refresh_token="first", gcid=FAKE_GCID, scope=())
    store.save(refresh_token="second", gcid=FAKE_GCID, scope=())
    assert json.loads(credentials_path.read_text())["refresh_token"] == "second"
    # No temporary file left behind to be mistaken for the credential.
    assert [p.name for p in credentials_path.parent.iterdir()] == [credentials_path.name]


def test_unreadable_credential_is_reported_clearly(credentials_path: Path) -> None:
    credentials_path.parent.mkdir(parents=True, exist_ok=True)
    credentials_path.write_text("this is not json", encoding="utf-8")
    with pytest.raises(AuthError, match="credential_unreadable"):
        CredentialStore(credentials_path).load()


def test_credential_without_a_refresh_token_is_rejected(credentials_path: Path) -> None:
    credentials_path.parent.mkdir(parents=True, exist_ok=True)
    credentials_path.write_text(json.dumps({"gcid": FAKE_GCID}), encoding="utf-8")
    with pytest.raises(AuthError, match="credential_invalid"):
        CredentialStore(credentials_path).load()


def test_absent_credential_loads_as_none(credentials_path: Path) -> None:
    assert CredentialStore(credentials_path).load() is None


def test_umask_does_not_loosen_the_credential_file(credentials_path: Path) -> None:
    previous = os.umask(0o000)
    try:
        CredentialStore(credentials_path).save(refresh_token="r", gcid=None, scope=())
        assert credentials_path.stat().st_mode & 0o777 == 0o600
    finally:
        os.umask(previous)


# -- refresh -----------------------------------------------------------------
async def test_refresh_persists_the_rotated_token(credentials_path: Path) -> None:
    save_credential(credentials_path, "old-refresh")
    manager, session = make_manager(
        credentials_path, ok(token_response(refresh_token="rotated-refresh"))
    )
    await manager.refresh()
    # BMW rotates the refresh token on every exchange; losing the new one
    # would force the operator to authorize again by hand.
    assert json.loads(credentials_path.read_text())["refresh_token"] == "rotated-refresh"
    assert session.last_request.data["grant_type"] == "refresh_token"
    assert session.last_request.data["refresh_token"] == "old-refresh"


async def test_refresh_uses_the_rotated_token_next_time(credentials_path: Path) -> None:
    save_credential(credentials_path, "first")
    manager, session = make_manager(
        credentials_path,
        ok(token_response(refresh_token="second", expires_in=1)),
        ok(token_response(refresh_token="third")),
    )
    await manager.refresh()
    await manager.refresh()
    assert session.requests[1].data["refresh_token"] == "second"
    assert json.loads(credentials_path.read_text())["refresh_token"] == "third"


async def test_refresh_without_a_credential_asks_for_login(credentials_path: Path) -> None:
    manager, _ = make_manager(credentials_path)
    with pytest.raises(ReauthorizationRequiredError, match="collector auth login"):
        await manager.refresh()


async def test_invalid_grant_demands_reauthorization(credentials_path: Path) -> None:
    save_credential(credentials_path, "stale")
    manager, _ = make_manager(credentials_path, err(400, "invalid_grant", "token expired"))
    with pytest.raises(ReauthorizationRequiredError, match="14 days"):
        await manager.refresh()
    assert manager.state.reauthorization_required is True
    assert manager.state.authorized is False
    assert manager.state.error_count == 1


async def test_transient_refresh_failure_does_not_demand_reauthorization(
    credentials_path: Path,
) -> None:
    save_credential(credentials_path)
    manager, _ = make_manager(credentials_path, err(500, "server_error"))
    with pytest.raises(AuthError) as excinfo:
        await manager.refresh()
    # A BMW outage must not be mistaken for a revoked credential, or the
    # collector would demand a human for something that will fix itself.
    assert not isinstance(excinfo.value, ReauthorizationRequiredError)
    assert manager.state.reauthorization_required is False


async def test_non_json_error_body_is_surfaced_not_crashed(credentials_path: Path) -> None:
    save_credential(credentials_path)
    manager, _ = make_manager(
        credentials_path,
        FakeResponse(status=502, body=b"<html>gateway error</html>"),
    )
    with pytest.raises(AuthError, match="invalid_response"):
        await manager.refresh()


async def test_access_token_is_reused_until_near_expiry(credentials_path: Path) -> None:
    save_credential(credentials_path)
    manager, session = make_manager(credentials_path, ok(token_response()))
    first = await manager.access_token()
    second = await manager.access_token()
    assert first == second == "access-1"
    # One exchange only: the session would raise on an unscripted request.
    assert len(session.requests) == 1


async def test_expiring_access_token_is_refreshed(credentials_path: Path) -> None:
    save_credential(credentials_path)
    manager, _ = make_manager(
        credentials_path,
        ok(token_response(expires_in=10)),
        ok(token_response(access_token="access-2")),
    )
    assert await manager.access_token() == "access-1"
    # Within the refresh skew, so the next call must exchange again.
    assert await manager.access_token() == "access-2"


def test_token_validity_accounts_for_skew() -> None:
    now = time.time()
    assert TokenSet("a", "r", now + REFRESH_SKEW_SECONDS - 1).access_valid(now=now) is False
    assert TokenSet("a", "r", now + 3600).access_valid(now=now) is True
    # A clock that jumped backwards must not make a dead token look fresh.
    assert TokenSet("a", "r", now - 10).access_valid(now=now) is False


async def test_short_expiry_is_floored_against_clock_skew(credentials_path: Path) -> None:
    save_credential(credentials_path)
    manager, _ = make_manager(credentials_path, ok(token_response(expires_in=0)))
    tokens = await manager.refresh()
    assert tokens.expires_at > time.time()


# -- MQTT credentials --------------------------------------------------------
async def test_id_token_returns_the_broker_credentials(credentials_path: Path) -> None:
    save_credential(credentials_path)
    manager, _ = make_manager(credentials_path, ok(token_response()))
    gcid, id_token = await manager.id_token()
    # The broker takes the GCID as username and the id token as password.
    assert gcid == FAKE_GCID
    assert id_token.count(".") == 2


async def test_missing_id_token_explains_the_openid_scope(credentials_path: Path) -> None:
    save_credential(credentials_path)
    payload = token_response()
    del payload["id_token"]
    manager, _ = make_manager(credentials_path, ok(payload))
    with pytest.raises(AuthError, match="openid"):
        await manager.id_token()


async def test_missing_gcid_is_reported(credentials_path: Path) -> None:
    CredentialStore(credentials_path).save(refresh_token="r", gcid=None, scope=())
    payload = token_response()
    del payload["gcid"]
    manager, _ = make_manager(credentials_path, ok(payload))
    with pytest.raises(AuthError, match="missing_gcid"):
        await manager.id_token()


async def test_gcid_is_remembered_when_a_refresh_omits_it(credentials_path: Path) -> None:
    save_credential(credentials_path)
    second = token_response(access_token="access-2")
    del second["gcid"]
    manager, _ = make_manager(credentials_path, ok(token_response()), ok(second))
    await manager.refresh()
    await manager.refresh()
    assert manager.gcid == FAKE_GCID


async def test_id_token_expiry_drives_connection_recycling(credentials_path: Path) -> None:
    save_credential(credentials_path)
    expiry = time.time() + 1800
    manager, _ = make_manager(credentials_path, ok(token_response(id_token=fake_jwt(expiry))))
    await manager.refresh()
    assert manager.id_token_expiry() == pytest.approx(expiry, abs=2)


async def test_opaque_id_token_falls_back_to_access_expiry(credentials_path: Path) -> None:
    save_credential(credentials_path)
    manager, _ = make_manager(credentials_path, ok(token_response(id_token="opaque-token")))
    tokens = await manager.refresh()
    assert manager.id_token_expiry() == tokens.expires_at


def test_unparseable_id_token_yields_no_expiry() -> None:
    assert _decode_jwt_exp("not-a-jwt") is None
    assert _decode_jwt_exp("a.b.c") is None
    assert _decode_jwt_exp("") is None


# -- scopes and state --------------------------------------------------------
async def test_missing_scopes_are_reported(credentials_path: Path) -> None:
    save_credential(credentials_path)
    manager, _ = make_manager(
        credentials_path, ok(token_response(scope="authenticate_user openid"))
    )
    await manager.refresh()
    assert set(manager.missing_scopes()) == {"cardata:api:read", "cardata:streaming:read"}
    assert set(manager.state.missing_scopes) == set(manager.missing_scopes())


async def test_empty_scope_response_claims_nothing_is_missing(credentials_path: Path) -> None:
    save_credential(credentials_path)
    payload = token_response()
    del payload["scope"]
    manager, _ = make_manager(credentials_path, ok(payload))
    await manager.refresh()
    # BMW returns every registered scope when none were requested, so an empty
    # list proves nothing and must not be reported as a problem.
    assert manager.missing_scopes() == ()


async def test_auth_state_never_exposes_a_token_or_gcid(credentials_path: Path) -> None:
    save_credential(credentials_path)
    manager, _ = make_manager(credentials_path, ok(token_response()))
    await manager.refresh()
    serialized = json.dumps(manager.state.as_dict())
    assert "access-1" not in serialized
    assert "refresh-1" not in serialized
    assert FAKE_GCID not in serialized
    # Presence is reportable; the value is not.
    assert manager.state.gcid_present is True
    assert manager.state.authorized is True


def test_load_persisted_reports_absence_without_raising(credentials_path: Path) -> None:
    manager = AuthManager("client", credentials_path)
    assert manager.load_persisted() is False
    assert manager.state.last_error is not None


def test_load_persisted_restores_scope_and_gcid_presence(credentials_path: Path) -> None:
    save_credential(credentials_path)
    manager = AuthManager("client", credentials_path)
    assert manager.load_persisted() is True
    assert manager.state.gcid_present is True
    assert "openid" in manager.state.scopes


async def test_injected_session_is_not_closed_by_the_manager(credentials_path: Path) -> None:
    manager, session = make_manager(credentials_path)
    await manager.close()
    # The collector owns the shared session; the manager must not close it.
    assert session.closed is False
