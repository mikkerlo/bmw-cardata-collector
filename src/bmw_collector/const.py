"""Protocol constants for BMW CarData.

Values are taken from BMW's official public specifications:

* Device Code Flow — ``Customer OAuth2.0 Login - Device Code Flow`` v1.6
* REST — ``CARDATA API`` v1.0.0 (``https://api-cardata.bmwgroup.com``)
* Integration guide v1.6 (2026-07-30)

See ``docs/architecture.md`` for the reference URLs.
"""

from __future__ import annotations

from typing import Final

# --- REST -------------------------------------------------------------------
CARDATA_API_HOST: Final = "https://api-cardata.bmwgroup.com"
API_VERSION: Final = "v1"
HEADER_X_VERSION: Final = "x-version"

# --- OAuth 2.0 Device Code Flow (GCDM) --------------------------------------
GCDM_HOST: Final = "https://customer.bmwgroup.com"
DEVICE_CODE_PATH: Final = "/gcdm/oauth/device/code"
TOKEN_PATH: Final = "/gcdm/oauth/token"  # noqa: S105 - a URL path, not a secret

GRANT_TYPE_DEVICE_CODE: Final = "urn:ietf:params:oauth:grant-type:device_code"
GRANT_TYPE_REFRESH_TOKEN: Final = "refresh_token"  # noqa: S105 - OAuth grant name
RESPONSE_TYPE_DEVICE_CODE: Final = "device_code"
CODE_CHALLENGE_METHOD: Final = "S256"

SCOPE_AUTHENTICATE_USER: Final = "authenticate_user"
SCOPE_OPENID: Final = "openid"
SCOPE_API_READ: Final = "cardata:api:read"
SCOPE_STREAMING_READ: Final = "cardata:streaming:read"

DEFAULT_SCOPES: Final[tuple[str, ...]] = (
    SCOPE_AUTHENTICATE_USER,
    SCOPE_OPENID,
    SCOPE_API_READ,
    SCOPE_STREAMING_READ,
)

# --- MQTT streaming ---------------------------------------------------------
# The broker authenticates with the GCID as username and the OIDC id_token as
# password. BMW allows a single active streaming connection per GCID.
STREAMING_HOST: Final = "customer.streaming-cardata.bmwgroup.com"
STREAMING_PORT: Final = 9000
STREAMING_QOS: Final = 0

# --- Documented service limits ---------------------------------------------
# Integration guide: 50 REST requests per day per user, reset at 00:00 UTC.
# The quota may be shared with other clients using the same BMW account.
REST_DAILY_LIMIT_DOCUMENTED: Final = 50
# HTTP 403 with this error id means the daily REST quota is exhausted.
QUOTA_ERROR_ID: Final = "CU-429"
