"""Structured JSON logging with hard redaction of credentials.

Redaction happens in the formatter, so it also covers log records emitted by
third-party libraries (aiohttp, aiomqtt) that we do not control.
"""

from __future__ import annotations

import contextvars
import json
import logging
import re
import sys
from datetime import UTC, datetime
from typing import Any

correlation_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)

#: Header and field names whose values must never reach the log or the database.
SENSITIVE_KEYS: frozenset[str] = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "access_token",
        "id_token",
        "refresh_token",
        "device_code",
        "user_code",
        "code_verifier",
        "code_challenge",
        "client_id",
        "password",
        "x-api-key",
        "api_key",
        "gcid",
    }
)

REDACTED = "***redacted***"

_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Bearer <token>
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,}"),
    # key=value / "key": "value" for sensitive keys
    re.compile(
        r"(?i)\b(" + "|".join(sorted(SENSITIVE_KEYS)) + r")\b(\"?\s*[:=]\s*\"?)([^\"\s,&}]{4,})"
    ),
    # Bare JWTs anywhere in the message
    re.compile(r"\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}"),
)


def redact_text(text: str) -> str:
    """Replace credential-looking substrings in free-form text."""
    out = _PATTERNS[0].sub("Bearer " + REDACTED, text)
    out = _PATTERNS[1].sub(lambda m: f"{m.group(1)}{m.group(2)}{REDACTED}", out)
    return _PATTERNS[2].sub(REDACTED, out)


def redact_mapping(data: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``data`` with sensitive keys replaced, recursively."""
    out: dict[str, Any] = {}
    for key, value in data.items():
        if key.lower() in SENSITIVE_KEYS:
            out[key] = REDACTED
        elif isinstance(value, dict):
            out[key] = redact_mapping(value)
        elif isinstance(value, str):
            out[key] = redact_text(value)
        else:
            out[key] = value
    return out


_RESERVED = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)


class JsonFormatter(logging.Formatter):
    """Render log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "component": record.name,
            "message": redact_text(record.getMessage()),
        }
        cid = correlation_id.get()
        if cid:
            payload["correlation_id"] = cid
        if record.exc_info and record.exc_info[0] is not None:
            payload["error_class"] = record.exc_info[0].__name__
            payload["traceback"] = redact_text(self.formatException(record.exc_info))
        extras = {k: v for k, v in record.__dict__.items() if k not in _RESERVED and k[0] != "_"}
        if extras:
            payload["extra"] = redact_mapping(extras)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(level: str = "INFO", *, plain: bool = False) -> None:
    """Install the JSON formatter on the root logger."""
    handler = logging.StreamHandler(sys.stderr)
    if plain:
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    else:
        handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # aiohttp access logs would echo URLs with query strings; keep them quiet.
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
