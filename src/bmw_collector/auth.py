"""OAuth 2.0 Device Code Flow with PKCE against BMW's GCDM, plus token upkeep.

The collector never sees the user's password. The device-code flow is run once
interactively; afterwards only the refresh token is persisted, with 0600
permissions, and exchanged for fresh access/id tokens as they expire.

BMW rotates the refresh token on every exchange, so the stored credential is
replaced atomically after each refresh. Losing that rotation would force a
re-authorization, which is why the write is a rename of a temporary file rather
than an in-place truncate.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp

from .const import (
    CODE_CHALLENGE_METHOD,
    DEFAULT_SCOPES,
    DEVICE_CODE_PATH,
    GCDM_HOST,
    GRANT_TYPE_DEVICE_CODE,
    GRANT_TYPE_REFRESH_TOKEN,
    RESPONSE_TYPE_DEVICE_CODE,
    SCOPE_API_READ,
    SCOPE_STREAMING_READ,
    TOKEN_PATH,
)

_LOGGER = logging.getLogger(__name__)

#: Refresh this long before the access token actually expires.
REFRESH_SKEW_SECONDS = 300
#: Never treat a token as usable for less than this, to survive clock skew.
MIN_TOKEN_LIFETIME_SECONDS = 30


class AuthError(Exception):
    """Base class for authorization failures."""

    def __init__(self, error: str, description: str = "") -> None:
        super().__init__(f"{error}: {description}" if description else error)
        self.error = error
        self.description = description


class AuthorizationPendingError(AuthError):
    """The user has not yet approved the device code."""


class SlowDownError(AuthError):
    """Polling too fast; the interval must be increased."""


class AccessDeniedError(AuthError):
    """The user declined the authorization request."""


class ExpiredTokenError(AuthError):
    """The device code expired before the user approved it."""


class ReauthorizationRequiredError(AuthError):
    """The refresh token is no longer usable; a human must log in again."""


_OAUTH_ERRORS: dict[str, type[AuthError]] = {
    "authorization_pending": AuthorizationPendingError,
    "slow_down": SlowDownError,
    "access_denied": AccessDeniedError,
    "expired_token": ExpiredTokenError,
    "invalid_grant": ReauthorizationRequiredError,
    "invalid_request": AuthError,
    "invalid_client": AuthError,
}

#: OAuth errors that mean retrying with the same refresh token is pointless.
_FATAL_REFRESH_ERRORS = frozenset({"invalid_grant", "access_denied", "expired_token"})


def generate_pkce_pair() -> tuple[str, str]:
    """Return ``(code_verifier, code_challenge)`` for PKCE S256."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode("ascii")
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
        .rstrip(b"=")
        .decode("ascii")
    )
    return verifier, challenge


@dataclass(frozen=True, slots=True)
class DeviceCodeGrant:
    """Response from the device-code endpoint, shown to the operator."""

    device_code: str
    user_code: str
    verification_uri: str
    expires_in: int
    interval: int = 5


@dataclass(slots=True)
class TokenSet:
    """Tokens held in memory. Only the refresh token is ever persisted."""

    access_token: str
    refresh_token: str
    expires_at: float
    scope: tuple[str, ...] = ()
    gcid: str | None = None
    id_token: str | None = None
    id_token_expires_at: float | None = None

    def access_valid(self, *, now: float | None = None, skew: int = REFRESH_SKEW_SECONDS) -> bool:
        """Whether the access token is still usable, accounting for skew."""
        return (self.expires_at - (now or time.time())) > skew


@dataclass(slots=True)
class AuthState:
    """Non-secret view of authorization health, for CLI and telemetry."""

    authorized: bool = False
    gcid_present: bool = False
    scopes: tuple[str, ...] = ()
    access_expires_at: float | None = None
    last_refresh_at: float | None = None
    last_error: str | None = None
    reauthorization_required: bool = False
    error_count: int = 0
    missing_scopes: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        """Serializable form with no secrets in it."""
        return {
            "authorized": self.authorized,
            "gcid_present": self.gcid_present,
            "scopes": list(self.scopes),
            "missing_scopes": list(self.missing_scopes),
            "access_expires_at": self.access_expires_at,
            "last_refresh_at": self.last_refresh_at,
            "last_error": self.last_error,
            "reauthorization_required": self.reauthorization_required,
            "error_count": self.error_count,
        }


def _decode_jwt_exp(token: str) -> float | None:
    """Best-effort ``exp`` claim of a JWT, without verifying the signature.

    Used only to schedule MQTT reconnects before the id token expires; it is
    never a security decision.
    """
    try:
        payload = token.split(".")[1]
        padded = payload + "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
    except Exception:  # noqa: BLE001 - opaque third-party token
        return None
    exp = claims.get("exp")
    return float(exp) if isinstance(exp, int | float) else None


class CredentialStore:
    """The persisted refresh credential, written with owner-only permissions."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def exists(self) -> bool:
        """Whether a credential file is present."""
        return self.path.is_file()

    def load(self) -> dict[str, Any] | None:
        """Read the credential file, or ``None`` if it does not exist."""
        if not self.path.is_file():
            return None
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as err:
            raise AuthError("credential_unreadable", f"{self.path}: {err}") from err
        if not isinstance(data, dict) or "refresh_token" not in data:
            raise AuthError("credential_invalid", f"{self.path} has no refresh_token")
        return data

    def save(self, *, refresh_token: str, gcid: str | None, scope: tuple[str, ...]) -> None:
        """Atomically replace the credential file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            self.path.parent.chmod(0o700)
        payload = {
            "version": 1,
            "refresh_token": refresh_token,
            "gcid": gcid,
            "scope": list(scope),
            "updated_at": time.time(),
        }
        tmp = self.path.with_name(self.path.name + ".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
        os.replace(tmp, self.path)
        os.chmod(self.path, 0o600)
        # fsync the directory so the rename itself survives a power cut.
        dir_fd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)


class AuthManager:
    """Owns the token lifecycle for one BMW account."""

    def __init__(
        self,
        client_id: str,
        credentials_path: str | Path,
        *,
        session: aiohttp.ClientSession | None = None,
        scopes: tuple[str, ...] = DEFAULT_SCOPES,
        base_url: str = GCDM_HOST,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._client_id = client_id
        self._credentials = CredentialStore(credentials_path)
        self._session = session
        self._owns_session = session is None
        self._scopes = scopes
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._tokens: TokenSet | None = None
        self._refresh_token: str | None = None
        self._code_verifier: str | None = None
        self._lock = asyncio.Lock()
        self.state = AuthState()

    # -- plumbing -----------------------------------------------------------
    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        """Close the HTTP session if this manager created it."""
        if self._owns_session and self._session is not None and not self._session.closed:
            await self._session.close()

    @property
    def credential_path(self) -> Path:
        """Where the refresh credential is stored."""
        return self._credentials.path

    @property
    def gcid(self) -> str | None:
        """GCID of the authorized account, once known."""
        return self._tokens.gcid if self._tokens else None

    def load_persisted(self) -> bool:
        """Load the stored refresh token. Returns whether one was found."""
        data = self._credentials.load()
        if data is None:
            self.state.last_error = "no stored credential"
            return False
        self._refresh_token = str(data["refresh_token"])
        stored_scope = data.get("scope") or []
        self.state.scopes = tuple(str(s) for s in stored_scope)
        self.state.gcid_present = bool(data.get("gcid"))
        return True

    async def _post_form(self, path: str, data: dict[str, str]) -> dict[str, Any]:
        session = await self._ensure_session()
        async with session.post(
            f"{self._base_url}{path}",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as response:
            try:
                payload = await response.json(content_type=None)
            except Exception:  # noqa: BLE001 - non-JSON error body
                payload = {"error": "invalid_response", "error_description": await response.text()}
            if not isinstance(payload, dict):
                payload = {"error": "invalid_response"}
            if response.status != 200:
                raise self._to_error(payload)
            return payload

    @staticmethod
    def _to_error(payload: dict[str, Any]) -> AuthError:
        error = str(payload.get("error", "unknown_error"))
        description = str(payload.get("error_description", ""))
        return _OAUTH_ERRORS.get(error, AuthError)(error, description)

    def _store_tokens(self, payload: dict[str, Any]) -> TokenSet:
        now = time.time()
        expires_in = int(payload.get("expires_in", 3599))
        id_token = payload.get("id_token")
        scope = tuple(str(payload.get("scope", "")).split()) if payload.get("scope") else ()
        tokens = TokenSet(
            access_token=str(payload["access_token"]),
            refresh_token=str(payload["refresh_token"]),
            expires_at=now + max(expires_in, MIN_TOKEN_LIFETIME_SECONDS),
            scope=scope,
            gcid=str(payload["gcid"]) if payload.get("gcid") else None,
            id_token=str(id_token) if id_token else None,
            id_token_expires_at=_decode_jwt_exp(str(id_token)) if id_token else None,
        )
        if tokens.gcid is None and self._tokens is not None:
            # BMW does not always repeat the gcid on refresh.
            tokens.gcid = self._tokens.gcid
        self._tokens = tokens
        self._refresh_token = tokens.refresh_token
        self._credentials.save(
            refresh_token=tokens.refresh_token, gcid=tokens.gcid, scope=tokens.scope
        )
        self.state.authorized = True
        self.state.gcid_present = tokens.gcid is not None
        self.state.scopes = tokens.scope
        self.state.access_expires_at = tokens.expires_at
        self.state.last_refresh_at = now
        self.state.last_error = None
        self.state.reauthorization_required = False
        self.state.missing_scopes = self.missing_scopes()
        return tokens

    def missing_scopes(self) -> tuple[str, ...]:
        """Scopes the collector needs that the grant did not include.

        An empty granted-scope list means BMW returned every scope registered
        for the client, so nothing can be concluded and nothing is reported.
        """
        granted = set(self.state.scopes)
        if not granted:
            return ()
        return tuple(s for s in (SCOPE_API_READ, SCOPE_STREAMING_READ) if s not in granted)

    # -- device code flow ---------------------------------------------------
    async def start_device_flow(self) -> DeviceCodeGrant:
        """Begin the device-code flow and return the operator-facing codes."""
        verifier, challenge = generate_pkce_pair()
        self._code_verifier = verifier
        payload = await self._post_form(
            DEVICE_CODE_PATH,
            {
                "client_id": self._client_id,
                "response_type": RESPONSE_TYPE_DEVICE_CODE,
                "scope": " ".join(self._scopes),
                "code_challenge": challenge,
                "code_challenge_method": CODE_CHALLENGE_METHOD,
            },
        )
        return DeviceCodeGrant(
            device_code=str(payload["device_code"]),
            user_code=str(payload["user_code"]),
            verification_uri=str(payload.get("verification_uri", "")),
            expires_in=int(payload.get("expires_in", 300)),
            interval=int(payload.get("interval", 5)),
        )

    async def poll_device_flow(self, grant: DeviceCodeGrant) -> TokenSet:
        """Poll until the user approves, honouring ``slow_down`` responses."""
        if self._code_verifier is None:
            raise AuthError("missing_code_verifier", "call start_device_flow first")
        interval = max(1, grant.interval)
        deadline = time.time() + grant.expires_in
        while True:
            try:
                payload = await self._post_form(
                    TOKEN_PATH,
                    {
                        "client_id": self._client_id,
                        "device_code": grant.device_code,
                        "grant_type": GRANT_TYPE_DEVICE_CODE,
                        "code_verifier": self._code_verifier,
                    },
                )
            except SlowDownError:
                interval += 5
            except AuthorizationPendingError:
                pass
            else:
                return self._store_tokens(payload)
            if time.time() >= deadline:
                raise ExpiredTokenError(
                    "expired_token", "the device code expired before it was approved"
                )
            await asyncio.sleep(interval)

    # -- refresh ------------------------------------------------------------
    async def refresh(self) -> TokenSet:
        """Exchange the stored refresh token for a fresh token set."""
        if self._refresh_token is None and not self.load_persisted():
            raise ReauthorizationRequiredError(
                "not_authorized",
                f"no credential at {self._credentials.path}; run 'collector auth login'",
            )
        assert self._refresh_token is not None
        try:
            payload = await self._post_form(
                TOKEN_PATH,
                {
                    "client_id": self._client_id,
                    "grant_type": GRANT_TYPE_REFRESH_TOKEN,
                    "refresh_token": self._refresh_token,
                },
            )
        except AuthError as err:
            self.state.last_error = err.error
            self.state.error_count += 1
            if err.error in _FATAL_REFRESH_ERRORS:
                self.state.authorized = False
                self.state.reauthorization_required = True
                raise ReauthorizationRequiredError(
                    err.error,
                    "the stored refresh token is no longer valid (BMW refresh tokens expire "
                    "after about 14 days of disuse); run 'collector auth login' again",
                ) from err
            raise
        return self._store_tokens(payload)

    async def access_token(self) -> str:
        """A currently valid access token, refreshing if needed."""
        async with self._lock:
            if self._tokens is not None and self._tokens.access_valid():
                return self._tokens.access_token
            tokens = await self.refresh()
            return tokens.access_token

    async def id_token(self) -> tuple[str, str]:
        """A valid ``(gcid, id_token)`` pair for the MQTT broker.

        The broker takes the GCID as username and the OIDC id token as
        password, so both come from the same exchange.
        """
        async with self._lock:
            tokens = self._tokens
            if tokens is None or tokens.id_token is None or not tokens.access_valid():
                tokens = await self.refresh()
            if tokens.id_token is None:
                raise AuthError(
                    "missing_id_token",
                    "BMW returned no id_token; the 'openid' scope must be granted to this "
                    "CarData client for MQTT streaming to work",
                )
            if tokens.gcid is None:
                raise AuthError(
                    "missing_gcid", "BMW returned no gcid; cannot authenticate to the broker"
                )
            return tokens.gcid, tokens.id_token

    def id_token_expiry(self) -> float | None:
        """Expiry of the current id token, used to schedule MQTT reconnects."""
        if self._tokens is None:
            return None
        return self._tokens.id_token_expires_at or self._tokens.expires_at
