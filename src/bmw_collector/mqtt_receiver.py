"""MQTT streaming receiver.

One TLS connection per BMW account, because BMW permits exactly one active
streaming connection per user (GCID) — not one per vehicle. Several VIN topics
are subscribed over that single connection.

Reconnect policy, in order of importance:

* An authorization or permission failure backs off hard (15 minutes by
  default). A revoked client that reconnected every five seconds would be
  abusive and would not start working any sooner.
* The id token used as the broker password expires; the receiver proactively
  recycles the connection shortly before that, with a freshly refreshed token.
* Everything else uses exponential backoff with full jitter, so a BMW-side
  outage does not produce a synchronised reconnect storm.

Delivery is QoS 0: there is no acknowledgement and no replay. Anything sent
while the collector is disconnected is lost, permanently. The receiver's job is
to minimise that window and to be honest about it.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import ssl
import time
from collections.abc import Callable
from typing import Any

import aiomqtt

from . import metrics
from .auth import AuthError, AuthManager, ReauthorizationRequiredError
from .config import MqttConfig
from .const import STREAMING_HOST, STREAMING_PORT, STREAMING_QOS
from .store import EventStore, RawEvent, utc_now_iso
from .writer import EventWriter

_LOGGER = logging.getLogger(__name__)

#: Recycle the connection this long before the id token expires.
TOKEN_RECYCLE_MARGIN_SECONDS = 120
#: How often to re-check the clock while waiting for messages. The recycle
#: deadline must be honoured on an idle connection too — a parked car sends
#: nothing for days, and waiting for a message before checking the clock means
#: never checking it.
CLOCK_CHECK_SECONDS = 5.0
#: MQTT SUBACK reason code for "not authorized".
SUBACK_NOT_AUTHORIZED = 135
#: How often to log an activity summary while messages keep arriving. Without
#: one, nothing at INFO level ever shows that data is flowing: an operator
#: tailing the journal sees the same silence whether the car is reporting or
#: the subscription is quietly broken. Per-message logging is not an option —
#: a drive produces bursts, and the log would bury everything else.
ACTIVITY_SUMMARY_SECONDS = 300.0
#: Floor on the pause between connection attempts, so the loop always yields.
MIN_RECONNECT_YIELD_SECONDS = 0.001

ClientFactory = Callable[..., Any]


class MqttReceiver:
    """Maintains the single streaming connection and archives what arrives."""

    def __init__(
        self,
        config: MqttConfig,
        auth: AuthManager,
        writer: EventWriter,
        store: EventStore,
        *,
        account_key: str,
        topics_provider: Callable[[], tuple[str, ...]],
        host: str = STREAMING_HOST,
        port: int = STREAMING_PORT,
        client_factory: ClientFactory | None = None,
        tls_context: ssl.SSLContext | None = None,
    ) -> None:
        self._config = config
        self._auth = auth
        self._writer = writer
        self._store = store
        self._account_key = account_key
        self._topics_provider = topics_provider
        self._host = host
        self._port = port
        self._client_factory = client_factory or aiomqtt.Client
        self._tls_context = tls_context
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

        self.connected = False
        self.reconnects = 0
        self.messages_received = 0
        self.last_message_at: float | None = None
        self.last_connect_at: float | None = None
        self.last_error: str | None = None
        self.subscribed_topics: tuple[str, ...] = ()
        self.ever_connected = False
        self.auth_blocked = False
        self._messages_this_connection = 0
        self._last_summary_at = 0.0

    # -- lifecycle ----------------------------------------------------------
    def start(self) -> None:
        """Start the connection-maintenance task."""
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run_forever(), name="mqtt-receiver")

    async def stop(self) -> None:
        """Stop the receiver and close the connection."""
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self.connected = False
        metrics.bmw_mqtt_connected.set(0)

    def status(self) -> dict[str, Any]:
        """Snapshot for the readiness endpoint."""
        return {
            "connected": self.connected,
            "ever_connected": self.ever_connected,
            "reconnects": self.reconnects,
            "messages_received": self.messages_received,
            "last_message_at": self.last_message_at,
            "last_connect_at": self.last_connect_at,
            "subscribed_topics": len(self.subscribed_topics),
            "auth_blocked": self.auth_blocked,
            "last_error": self.last_error,
        }

    # -- topics -------------------------------------------------------------
    def _topics(self, gcid: str) -> tuple[str, ...]:
        """Topics to subscribe for this connection."""
        if self._config.subscribe_mode == "wildcard":
            return (f"{gcid}/+",)
        return tuple(f"{gcid}/{vin}" for vin in self._topics_provider())

    # -- main loop ----------------------------------------------------------
    async def _run_forever(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            delay = 0.0
            try:
                await self._connect_and_consume()
                # A clean session resets the backoff, but never to zero: if the
                # broker accepted and immediately closed the connection, a
                # zero delay would hammer it in a tight loop (and, with every
                # await completing synchronously, starve this event loop).
                attempt = 0
                delay = self._config.reconnect_min_seconds
            except asyncio.CancelledError:
                raise
            except (ReauthorizationRequiredError, PermissionError) as err:
                self.auth_blocked = True
                self.last_error = str(err)
                metrics.bmw_auth_errors_total.inc()
                delay = self._config.auth_failure_backoff_seconds
                _LOGGER.error(
                    "MQTT streaming is not authorized; backing off %.0fs before retrying: %s",
                    delay,
                    err,
                    extra={"auth_blocked": True},
                )
                await self._incident("mqtt_auth_denied", {"error": str(err)})
            except AuthError as err:
                self.last_error = str(err)
                metrics.bmw_auth_errors_total.inc()
                attempt += 1
                delay = self._backoff(attempt)
                _LOGGER.error("MQTT token acquisition failed: %s", err)
            except aiomqtt.MqttError as err:
                self.last_error = f"{type(err).__name__}: {err}"
                attempt += 1
                delay = self._backoff(attempt)
                _LOGGER.warning(
                    "MQTT connection lost, reconnecting in %.1fs: %s",
                    delay,
                    err,
                    extra={"attempt": attempt},
                )
            except Exception as err:  # the loop must survive anything
                self.last_error = f"{type(err).__name__}: {err}"
                attempt += 1
                delay = self._backoff(attempt)
                _LOGGER.exception("unexpected MQTT receiver failure", extra={"attempt": attempt})
                await self._incident("mqtt_unexpected_error", {"error": str(err)})
            finally:
                self.connected = False
                metrics.bmw_mqtt_connected.set(0)
            if not self._stop.is_set():
                # Always yield, even with a zero delay, so the loop can never
                # monopolise the event loop.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=max(delay, MIN_RECONNECT_YIELD_SECONDS)
                    )

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with full jitter, clamped to the configured window."""
        ceiling = min(
            self._config.reconnect_max_seconds,
            self._config.reconnect_min_seconds * (2 ** min(attempt - 1, 10)),
        )
        return random.uniform(self._config.reconnect_min_seconds, max(ceiling, 1.0))  # noqa: S311

    async def _connect_and_consume(self) -> None:
        gcid, id_token = await self._auth.id_token()
        topics = self._topics(gcid)
        if not topics:
            _LOGGER.warning(
                "no VINs to subscribe to; waiting for vehicle discovery before connecting"
            )
            await asyncio.sleep(min(60.0, self._config.reconnect_max_seconds))
            return

        self.reconnects += 1
        metrics.bmw_mqtt_reconnects_total.inc()
        tls_context = self._tls_context or ssl.create_default_context()
        client = self._client_factory(
            hostname=self._host,
            port=self._port,
            username=gcid,
            password=id_token,
            identifier=f"collector-{gcid}",
            tls_context=tls_context,
            keepalive=self._config.keepalive,
        )
        _LOGGER.info(
            "connecting to BMW streaming broker",
            extra={"host": self._host, "port": self._port, "topic_count": len(topics)},
        )
        async with client:
            self.connected = True
            self.ever_connected = True
            self.auth_blocked = False
            self._messages_this_connection = 0
            self._last_summary_at = time.time()
            self.last_connect_at = time.time()
            self.last_error = None
            metrics.bmw_mqtt_connected.set(1)

            await self._subscribe(client, topics)
            await self._consume_until_token_expiry(client)

    async def _subscribe(self, client: Any, topics: tuple[str, ...]) -> None:
        """Subscribe to every topic, treating a refusal as a real failure."""
        subscribed: list[str] = []
        for topic in topics:
            try:
                await client.subscribe(topic, qos=STREAMING_QOS)
            except aiomqtt.MqttError as err:
                # A refused subscribe usually means the data stream has not
                # been configured in the BMW portal for this VIN, or the VIN is
                # not mapped. Both need a human, so do not retry in a loop.
                _LOGGER.error(
                    "broker refused a topic subscription: %s. Check that streaming "
                    "descriptors are selected for this vehicle in the BMW CarData portal "
                    "and that the VIN is mapped to this account.",
                    err,
                )
                await self._incident("mqtt_subscribe_refused", {"error": str(err)})
                if (
                    SUBACK_NOT_AUTHORIZED in getattr(err, "rc", ())
                    or "not authorized" in str(err).lower()
                ):
                    raise PermissionError(f"subscription refused by broker: {err}") from err
                continue
            subscribed.append(topic)
        self.subscribed_topics = tuple(subscribed)
        metrics.bmw_mqtt_subscribed_topics.set(len(subscribed))
        if not subscribed:
            raise aiomqtt.MqttError("no topic could be subscribed")
        _LOGGER.info("subscribed to %d topic(s)", len(subscribed))

    async def _consume_until_token_expiry(self, client: Any) -> None:
        """Read messages until the token nears expiry or the stream ends.

        The deadline is checked on a timer rather than after each message:
        gating it behind message arrival means an idle connection never checks
        at all, so the token expires and BMW drops us reactively instead. A
        parked car is silent for days, which makes idle the normal case.

        The pending-message task is deliberately *not* cancelled on each clock
        tick. ``aiomqtt``'s ``__anext__`` cancels its underlying queue read
        when interrupted, and a message already dequeued at that moment is
        discarded — with QoS 0 that would be unrecoverable data loss. The task
        is carried across iterations instead, and anything it has already
        delivered is archived before this returns.
        """
        expiry = self._auth.id_token_expiry()
        recycle_at = expiry - TOKEN_RECYCLE_MARGIN_SECONDS if expiry else time.time() + 1800
        iterator = client.messages.__aiter__()
        pending: asyncio.Task[Any] | None = None
        try:
            while not self._stop.is_set():
                remaining = recycle_at - time.time()
                if remaining <= 0:
                    # Reconnecting with a fresh token is the documented way to
                    # survive id-token expiry; the broker drops us otherwise.
                    _LOGGER.info("recycling MQTT connection before the id token expires")
                    return
                if pending is None:
                    pending = asyncio.ensure_future(iterator.__anext__())
                done, _ = await asyncio.wait({pending}, timeout=min(remaining, CLOCK_CHECK_SECONDS))
                if not done:
                    continue  # clock tick only; the read stays armed
                finished, pending = pending, None
                try:
                    message = finished.result()
                except StopAsyncIteration:
                    return
                self._handle_message(message)
        finally:
            if pending is not None:
                # Archive anything that landed while we were deciding to stop,
                # then drop the read.
                if pending.done() and not pending.cancelled():
                    with contextlib.suppress(StopAsyncIteration, aiomqtt.MqttError):
                        self._handle_message(pending.result())
                else:
                    pending.cancel()
                    with contextlib.suppress(
                        asyncio.CancelledError, StopAsyncIteration, aiomqtt.MqttError
                    ):
                        await pending

    # -- message handling ---------------------------------------------------
    def _handle_message(self, message: Any) -> None:
        """Archive one message. Must stay cheap: it runs in the read loop."""
        received_at = utc_now_iso()
        self.messages_received += 1
        self.last_message_at = time.time()
        metrics.bmw_mqtt_messages_received_total.inc()
        metrics.bmw_mqtt_last_message_timestamp_seconds.set(self.last_message_at)

        payload = message.payload
        payload_bytes = bytes(payload) if isinstance(payload, bytes | bytearray) else b""
        topic = str(message.topic)
        vin, event_time, entity_id = _peek(payload_bytes, topic)

        # Make "data is flowing" visible at INFO, without logging every
        # message: the first one on each connection, then a periodic summary.
        self._messages_this_connection += 1
        if self._messages_this_connection == 1:
            _LOGGER.info(
                "receiving vehicle data: first message on this connection (%d bytes)",
                len(payload_bytes),
                extra={"payload_bytes": len(payload_bytes), "has_timestamp": bool(event_time)},
            )
            self._last_summary_at = self.last_message_at or time.time()
        elif (self.last_message_at or 0) - self._last_summary_at >= ACTIVITY_SUMMARY_SECONDS:
            _LOGGER.info(
                "receiving vehicle data: %d message(s) on this connection",
                self._messages_this_connection,
                extra={"messages_this_connection": self._messages_this_connection},
            )
            self._last_summary_at = self.last_message_at or time.time()

        self._writer.submit(
            RawEvent(
                source="mqtt",
                account_key=self._account_key,
                payload=payload_bytes,
                received_at_utc=received_at,
                vin=vin,
                topic=topic,
                mqtt_qos=getattr(message, "qos", STREAMING_QOS),
                event_time_utc=event_time,
                metadata={
                    "mqtt": {
                        "retain": bool(getattr(message, "retain", False)),
                        "entity_id": entity_id,
                    },
                    "payload_bytes": len(payload_bytes),
                },
            )
        )

    async def _incident(self, kind: str, details: dict[str, Any]) -> None:
        with contextlib.suppress(Exception):
            await self._store.record_incident(kind, details)
            metrics.collector_incidents_total.labels(kind=kind).inc()


def _peek(payload: bytes, topic: str) -> tuple[str | None, str | None, str | None]:
    """Extract VIN, BMW timestamp and entity id for indexing.

    Deliberately failure-tolerant: the payload is already queued for byte-exact
    archival, and a malformed or unexpected body must never cost us the event.
    """
    vin: str | None = None
    parts = topic.split("/", 1)
    if len(parts) == 2 and parts[1] and "+" not in parts[1] and "#" not in parts[1]:
        vin = parts[1]
    if not payload:
        return vin, None, None
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return vin, None, None
    if not isinstance(parsed, dict):
        return vin, None, None
    payload_vin = parsed.get("vin")
    if isinstance(payload_vin, str) and payload_vin:
        vin = payload_vin
    timestamp = parsed.get("timestamp")
    entity_id = parsed.get("entityId")
    return (
        vin,
        timestamp if isinstance(timestamp, str) else None,
        entity_id if isinstance(entity_id, str) else None,
    )
