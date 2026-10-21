"""Credential redaction in logs and stored metadata."""

from __future__ import annotations

import json
import logging

import pytest

from bmw_collector.logging_setup import (
    REDACTED,
    JsonFormatter,
    configure_logging,
    redact_mapping,
    redact_text,
)

# Fabricated credential-shaped strings. None of these authorize anything.
JWT = "eyJhbGciOiJub25lIn0.eyJleHAiOjE3NjAwMDAwMDB9.c2lnbmF0dXJl"
OPAQUE_TOKEN = "11Y7pXE5FwLtUnOQRpyyxRn9h4p4ctvOuNkVyrEsvtO6vnAdrkXlE"


@pytest.mark.parametrize(
    "text",
    [
        f"Authorization: Bearer {OPAQUE_TOKEN}",
        f"bearer {OPAQUE_TOKEN}",
        f'{{"access_token": "{OPAQUE_TOKEN}"}}',
        f"refresh_token={OPAQUE_TOKEN}",
        f"id_token: {OPAQUE_TOKEN}",
        f"device_code={OPAQUE_TOKEN}",
        f"code_verifier={OPAQUE_TOKEN}",
        f"client_id={OPAQUE_TOKEN}",
    ],
)
def test_credential_shapes_are_redacted(text: str) -> None:
    assert OPAQUE_TOKEN not in redact_text(text)
    assert REDACTED in redact_text(text)


def test_bare_jwts_are_redacted_anywhere_in_the_text() -> None:
    assert JWT not in redact_text(f"broker rejected the token {JWT} at connect")


def test_ordinary_text_is_left_alone() -> None:
    message = "subscribed to 2 topic(s); committed 14 events"
    assert redact_text(message) == message


def test_mapping_redaction_is_recursive() -> None:
    redacted = redact_mapping(
        {
            "http": {"headers": {"Authorization": f"Bearer {OPAQUE_TOKEN}"}},
            "access_token": OPAQUE_TOKEN,
            "gcid": "aa111192-d570-4d3d-8835-656ee00edcd7",
            "status": 200,
            "note": f"token was {JWT}",
        }
    )
    serialized = json.dumps(redacted)
    assert OPAQUE_TOKEN not in serialized
    assert JWT not in serialized
    assert "aa111192" not in serialized
    # Non-sensitive values must survive, or the logs become useless.
    assert redacted["status"] == 200


def test_formatter_redacts_the_message() -> None:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="connecting with Bearer %s",
        args=(OPAQUE_TOKEN,),
        exc_info=None,
    )
    rendered = JsonFormatter().format(record)
    assert OPAQUE_TOKEN not in rendered
    assert json.loads(rendered)["component"] == "test"


def test_formatter_redacts_structured_extras() -> None:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="x",
        args=(),
        exc_info=None,
    )
    record.id_token = OPAQUE_TOKEN  # type: ignore[attr-defined]
    record.topic_count = 2  # type: ignore[attr-defined]
    payload = json.loads(JsonFormatter().format(record))
    assert payload["extra"]["id_token"] == REDACTED
    assert payload["extra"]["topic_count"] == 2


def test_formatter_redacts_tracebacks() -> None:
    try:
        raise ValueError(f"refresh failed for refresh_token={OPAQUE_TOKEN}")
    except ValueError:
        import sys

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="boom",
            args=(),
            exc_info=sys.exc_info(),
        )
    rendered = JsonFormatter().format(record)
    assert OPAQUE_TOKEN not in rendered
    assert json.loads(rendered)["error_class"] == "ValueError"


def test_formatter_emits_single_line_json() -> None:
    record = logging.LogRecord(
        name="c",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="multi\nline\nmessage",
        args=(),
        exc_info=None,
    )
    rendered = JsonFormatter().format(record)
    assert "\n" not in rendered
    assert json.loads(rendered)["message"] == "multi\nline\nmessage"


def test_formatter_includes_a_correlation_id_when_set() -> None:
    from bmw_collector.logging_setup import correlation_id

    token = correlation_id.set("req-42")
    try:
        record = logging.LogRecord(
            name="c",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="x",
            args=(),
            exc_info=None,
        )
        assert json.loads(JsonFormatter().format(record))["correlation_id"] == "req-42"
    finally:
        correlation_id.reset(token)


def test_configure_logging_replaces_handlers(caplog: pytest.LogCaptureFixture) -> None:
    configure_logging("DEBUG")
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, JsonFormatter)
    assert root.level == logging.DEBUG
    # aiohttp access logs would echo URLs with query strings.
    assert logging.getLogger("aiohttp.access").level == logging.WARNING
    configure_logging("INFO", plain=True)
    assert not isinstance(logging.getLogger().handlers[0].formatter, JsonFormatter)
