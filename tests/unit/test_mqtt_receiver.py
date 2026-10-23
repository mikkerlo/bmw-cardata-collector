"""MQTT receiver: archival fidelity, reconnect policy, token recycling.

The broker is a scripted in-process double (:class:`tests.conftest.FakeBroker`);
nothing here opens a socket.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import aiomqtt
import pytest

from bmw_collector.auth import AuthError, ReauthorizationRequiredError
from bmw_collector.config import MqttConfig
from bmw_collector.mqtt_receiver import TOKEN_RECYCLE_MARGIN_SECONDS, MqttReceiver, _peek
from bmw_collector.store import EventStore
from bmw_collector.writer import EventWriter
from tests.conftest import VALID_VIN, FakeBroker, FakeMessage, stream_payload

OTHER_VIN = "WBA99999999999999"
GCID = "aa111192-d570-4d3d-8835-656ee00edcd7"


class StubAuth:
    """Auth stand-in handing out broker credentials."""

    def __init__(
        self,
        *,
        error: Exception | None = None,
        expiry: float | None = None,
        gcid: str = GCID,
    ) -> None:
        self.error = error
        self.expiry = expiry
        self.gcid = gcid
        self.calls = 0

    async def id_token(self) -> tuple[str, str]:
        """Return (gcid, id_token), or raise the scripted error."""
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.gcid, f"id-token-{self.calls}"

    def id_token_expiry(self) -> float | None:
        """Scripted id-token expiry."""
        return self.expiry


def build(
    store: EventStore,
    broker: FakeBroker,
    *,
    auth: Any = None,
    vins: tuple[str, ...] = (VALID_VIN,),
    config: MqttConfig | None = None,
    queue_max: int = 1000,
) -> tuple[MqttReceiver, EventWriter]:
    """Assemble a receiver over real storage with a scripted broker."""
    writer = EventWriter(store, queue_max=queue_max)
    receiver = MqttReceiver(
        config or MqttConfig(reconnect_min_seconds=0.01, reconnect_max_seconds=0.02),
        auth or StubAuth(),  # type: ignore[arg-type]
        writer,
        store,
        account_key="test",
        topics_provider=lambda: vins,
        client_factory=broker,
    )
    return receiver, writer


async def run_until(predicate: Any, *, timeout: float = 5.0) -> None:
    """Poll until ``predicate()`` is true."""
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


# -- payload inspection ------------------------------------------------------
def test_peek_takes_the_vin_from_the_payload() -> None:
    vin, timestamp, entity = _peek(stream_payload(VALID_VIN), f"{GCID}/{VALID_VIN}")
    assert vin == VALID_VIN
    assert timestamp == "2026-10-18T12:00:00.000Z"
    assert entity == "entity-0"


def test_peek_falls_back_to_the_topic_for_the_vin() -> None:
    vin, _, _ = _peek(b'{"data":{}}', f"{GCID}/{VALID_VIN}")
    assert vin == VALID_VIN


def test_peek_ignores_a_wildcard_topic_segment() -> None:
    assert _peek(b"{}", f"{GCID}/+")[0] is None


def test_peek_survives_a_malformed_payload() -> None:
    # The event is already queued for byte-exact archival by this point: a
    # parse failure must never cost us the event.
    assert _peek(b"\xff\xfe not json", f"{GCID}/{VALID_VIN}") == (VALID_VIN, None, None)
    assert _peek(b"[1,2,3]", f"{GCID}/{VALID_VIN}") == (VALID_VIN, None, None)
    assert _peek(b"", f"{GCID}/{VALID_VIN}") == (VALID_VIN, None, None)


def test_peek_ignores_non_string_timestamps() -> None:
    payload = json.dumps({"vin": VALID_VIN, "timestamp": 1760000000}).encode()
    assert _peek(payload, f"{GCID}/{VALID_VIN}")[1] is None


# -- archival ----------------------------------------------------------------
async def test_messages_are_archived_byte_for_byte(async_store: EventStore) -> None:
    payload = stream_payload(VALID_VIN)
    broker = FakeBroker(messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", payload)])
    receiver, writer = build(async_store, broker)
    writer.start()
    receiver.start()
    await run_until(lambda: writer.committed >= 1)
    await receiver.stop()
    await writer.stop()

    event = next(iter(await asyncio.to_thread(lambda: list(async_store.iter_events_sync()))))
    assert event.payload == payload
    assert event.source == "mqtt"
    assert event.topic == f"{GCID}/{VALID_VIN}"
    assert event.vin == VALID_VIN
    assert event.mqtt_qos == 0
    assert event.event_time_utc == "2026-10-18T12:00:00.000Z"
    assert event.metadata["mqtt"]["entity_id"] == "entity-0"
    assert event.metadata["payload_bytes"] == len(payload)


async def test_an_unparseable_payload_is_still_archived(async_store: EventStore) -> None:
    payload = b"\x00\xff not json at all"
    broker = FakeBroker(messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", payload)])
    receiver, writer = build(async_store, broker)
    writer.start()
    receiver.start()
    await run_until(lambda: writer.committed >= 1)
    await receiver.stop()
    await writer.stop()
    stored = next(iter(await asyncio.to_thread(lambda: list(async_store.iter_events_sync()))))
    # An unknown or malformed descriptor is exactly what the raw archive is for.
    assert stored.payload == payload
    assert stored.vin == VALID_VIN


async def test_multiple_messages_are_all_archived_in_order(async_store: EventStore) -> None:
    messages = [
        FakeMessage(f"{GCID}/{VALID_VIN}", stream_payload(VALID_VIN, index=i)) for i in range(25)
    ]
    broker = FakeBroker(messages_to_send=messages)
    receiver, writer = build(async_store, broker)
    writer.start()
    receiver.start()
    await run_until(lambda: writer.committed >= 25)
    await receiver.stop()
    await writer.stop()
    events = await asyncio.to_thread(lambda: list(async_store.iter_events_sync()))
    assert [e.metadata["mqtt"]["entity_id"] for e in events] == [f"entity-{i}" for i in range(25)]


async def test_counters_separate_connection_health_from_traffic(
    async_store: EventStore,
) -> None:
    broker = FakeBroker(messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", b"{}")])
    receiver, writer = build(async_store, broker)
    writer.start()
    receiver.start()
    await run_until(lambda: receiver.messages_received >= 1)
    status = receiver.status()
    await receiver.stop()
    await writer.stop()
    # "Connected" and "recently heard from" are different questions: a parked
    # car is silent for days while the connection is perfectly healthy.
    assert status["connected"] is True
    assert status["messages_received"] == 1
    assert status["last_message_at"] is not None
    assert status["last_connect_at"] is not None


# -- subscriptions -----------------------------------------------------------
async def test_one_connection_carries_every_vin_topic(async_store: EventStore) -> None:
    broker = FakeBroker()
    receiver, writer = build(async_store, broker, vins=(VALID_VIN, OTHER_VIN))
    writer.start()
    receiver.start()
    await run_until(lambda: len(broker.subscriptions) >= 2)
    await receiver.stop()
    await writer.stop()
    # BMW allows one streaming connection per account, not one per vehicle.
    assert broker.connections == 1
    assert {t for t, _ in broker.subscriptions} == {
        f"{GCID}/{VALID_VIN}",
        f"{GCID}/{OTHER_VIN}",
    }
    assert {q for _, q in broker.subscriptions} == {0}


async def test_wildcard_mode_subscribes_one_topic(async_store: EventStore) -> None:
    broker = FakeBroker()
    receiver, writer = build(
        async_store,
        broker,
        vins=(VALID_VIN, OTHER_VIN),
        config=MqttConfig(subscribe_mode="wildcard", reconnect_min_seconds=0.01),
    )
    writer.start()
    receiver.start()
    await run_until(lambda: len(broker.subscriptions) >= 1)
    await receiver.stop()
    await writer.stop()
    assert broker.subscriptions == [(f"{GCID}/+", 0)]


async def test_broker_credentials_are_the_gcid_and_id_token(async_store: EventStore) -> None:
    broker = FakeBroker()
    receiver, writer = build(async_store, broker)
    writer.start()
    receiver.start()
    await run_until(lambda: broker.connections >= 1)
    await receiver.stop()
    await writer.stop()
    username, password = broker.credentials[0]
    assert username == GCID
    assert password == "id-token-1"


async def test_no_vins_means_no_connection_attempt(async_store: EventStore) -> None:
    broker = FakeBroker()
    receiver, writer = build(async_store, broker, vins=())
    writer.start()
    receiver.start()
    await asyncio.sleep(0.1)
    await receiver.stop()
    await writer.stop()
    # Connecting with nothing to subscribe to would burn the account's single
    # allowed streaming connection for no benefit.
    assert broker.connections == 0


async def test_a_refused_subscription_is_recorded(async_store: EventStore) -> None:
    broker = FakeBroker(fail_subscribe=aiomqtt.MqttError("topic refused"))
    receiver, writer = build(async_store, broker)
    writer.start()
    receiver.start()
    await run_until(lambda: receiver.reconnects >= 1)
    await asyncio.sleep(0.1)
    await receiver.stop()
    await writer.stop()
    incidents = await asyncio.to_thread(async_store.recent_incidents_sync)
    assert any(i["kind"] == "mqtt_subscribe_refused" for i in incidents)


async def test_an_unauthorized_subscription_backs_off_hard(async_store: EventStore) -> None:
    broker = FakeBroker(fail_subscribe=aiomqtt.MqttError("not authorized"))
    receiver, writer = build(
        async_store,
        broker,
        config=MqttConfig(
            reconnect_min_seconds=0.01,
            reconnect_max_seconds=0.02,
            auth_failure_backoff_seconds=30.0,
        ),
    )
    writer.start()
    receiver.start()
    await run_until(lambda: receiver.auth_blocked)
    attempts_at_block = receiver.reconnects
    await asyncio.sleep(0.2)
    await receiver.stop()
    await writer.stop()
    # A permission problem needs a human. Retrying every 10ms would be abusive
    # and would not start working any sooner.
    assert receiver.reconnects == attempts_at_block
    assert receiver.status()["auth_blocked"] is True


# -- reconnect policy --------------------------------------------------------
async def test_a_connection_failure_is_retried(async_store: EventStore) -> None:
    broker = FakeBroker(connect_error=aiomqtt.MqttError("connection refused"))
    receiver, writer = build(async_store, broker)
    writer.start()
    receiver.start()
    await run_until(lambda: receiver.reconnects >= 3)
    await receiver.stop()
    await writer.stop()
    assert receiver.connected is False
    assert receiver.ever_connected is False
    assert receiver.last_error is not None


async def test_reauthorization_required_backs_off_hard(async_store: EventStore) -> None:
    auth = StubAuth(error=ReauthorizationRequiredError("invalid_grant", "expired"))
    broker = FakeBroker()
    receiver, writer = build(
        async_store,
        broker,
        auth=auth,
        config=MqttConfig(
            reconnect_min_seconds=0.01,
            reconnect_max_seconds=0.02,
            auth_failure_backoff_seconds=30.0,
        ),
    )
    writer.start()
    receiver.start()
    await run_until(lambda: receiver.auth_blocked)
    calls_at_block = auth.calls
    await asyncio.sleep(0.2)
    await receiver.stop()
    await writer.stop()
    assert auth.calls == calls_at_block
    incidents = await asyncio.to_thread(async_store.recent_incidents_sync)
    assert any(i["kind"] == "mqtt_auth_denied" for i in incidents)


async def test_a_transient_auth_error_retries_with_normal_backoff(
    async_store: EventStore,
) -> None:
    auth = StubAuth(error=AuthError("server_error", "BMW is having a moment"))
    broker = FakeBroker()
    receiver, writer = build(async_store, broker, auth=auth)
    writer.start()
    receiver.start()
    await run_until(lambda: auth.calls >= 3)
    await receiver.stop()
    await writer.stop()
    # A BMW outage is not a permission problem: keep trying, with backoff.
    assert receiver.auth_blocked is False


def test_backoff_grows_and_stays_inside_the_configured_window(
    async_store: EventStore,
) -> None:
    config = MqttConfig(reconnect_min_seconds=5.0, reconnect_max_seconds=300.0)
    receiver, _ = build(async_store, FakeBroker(), config=config)
    for attempt in range(1, 20):
        delay = receiver._backoff(attempt)
        assert 5.0 <= delay <= 300.0
    # Full jitter: the ceiling grows with the attempt count, so late attempts
    # can reach values early ones cannot.
    assert max(receiver._backoff(12) for _ in range(200)) > 100.0
    assert max(receiver._backoff(1) for _ in range(200)) <= 10.0


def test_backoff_is_jittered(async_store: EventStore) -> None:
    receiver, _ = build(
        async_store,
        FakeBroker(),
        config=MqttConfig(reconnect_min_seconds=1.0, reconnect_max_seconds=60.0),
    )
    delays = {receiver._backoff(6) for _ in range(50)}
    # Identical delays across instances would synchronise every collector's
    # reconnect after a BMW-side outage.
    assert len(delays) > 10


# -- token lifecycle ---------------------------------------------------------
async def test_connection_is_recycled_before_the_id_token_expires(
    async_store: EventStore,
) -> None:
    # Expiry already inside the recycle margin, so the first message triggers
    # a reconnect with a freshly refreshed token.
    auth = StubAuth(expiry=time.time() + TOKEN_RECYCLE_MARGIN_SECONDS - 1)
    broker = FakeBroker(messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", b"{}")])
    receiver, writer = build(async_store, broker, auth=auth)
    writer.start()
    receiver.start()
    await run_until(lambda: broker.connections >= 2)
    await receiver.stop()
    await writer.stop()
    # A new token on each reconnect: the broker rejects an expired one.
    assert broker.credentials[0][1] != broker.credentials[1][1]


async def test_a_healthy_token_is_not_recycled(async_store: EventStore) -> None:
    auth = StubAuth(expiry=time.time() + 3600)
    broker = FakeBroker(messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", b"{}")])
    receiver, writer = build(async_store, broker, auth=auth)
    writer.start()
    receiver.start()
    await run_until(lambda: receiver.messages_received >= 1)
    await asyncio.sleep(0.15)
    await receiver.stop()
    await writer.stop()
    assert broker.connections == 1


# -- lifecycle ---------------------------------------------------------------
async def test_stop_is_idempotent_and_safe_before_start(async_store: EventStore) -> None:
    receiver, _ = build(async_store, FakeBroker())
    await receiver.stop()
    receiver.start()
    await receiver.stop()
    await receiver.stop()
    assert receiver.connected is False


async def test_an_unexpected_error_does_not_kill_the_loop(async_store: EventStore) -> None:
    broker = FakeBroker(connect_error=RuntimeError("something odd"))
    receiver, writer = build(async_store, broker)
    writer.start()
    receiver.start()
    await run_until(lambda: receiver.reconnects >= 2)
    await receiver.stop()
    await writer.stop()
    incidents = await asyncio.to_thread(async_store.recent_incidents_sync)
    assert any(i["kind"] == "mqtt_unexpected_error" for i in incidents)


async def test_queue_saturation_is_visible_rather_than_silent(
    async_store: EventStore,
) -> None:
    messages = [FakeMessage(f"{GCID}/{VALID_VIN}", b'{"n":%d}' % i) for i in range(50)]
    broker = FakeBroker(messages_to_send=messages)
    # Writer is never started, so the queue cannot drain.
    receiver, writer = build(async_store, broker, queue_max=5)
    receiver.start()
    await run_until(lambda: writer.dropped > 0)
    await receiver.stop()
    # QoS 0 means a dropped event is gone for good; it must be counted.
    assert writer.dropped > 0
    assert writer.status()["dropped"] == writer.dropped
    await writer.stop()


async def test_the_first_message_is_announced_at_info(
    async_store: EventStore, caplog: pytest.LogCaptureFixture
) -> None:
    broker = FakeBroker(
        messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", stream_payload(VALID_VIN))]
    )
    receiver, writer = build(async_store, broker)
    writer.start()
    with caplog.at_level("INFO", logger="bmw_collector.mqtt_receiver"):
        receiver.start()
        await run_until(lambda: receiver.messages_received >= 1)
    await receiver.stop()
    await writer.stop()
    # At INFO, nothing else in the log distinguishes "car is parked" from
    # "subscription is quietly broken".
    assert "first message on this connection" in caplog.text
    assert VALID_VIN not in caplog.text


async def test_further_messages_are_not_logged_individually(
    async_store: EventStore, caplog: pytest.LogCaptureFixture
) -> None:
    messages = [
        FakeMessage(f"{GCID}/{VALID_VIN}", stream_payload(VALID_VIN, index=i)) for i in range(40)
    ]
    broker = FakeBroker(messages_to_send=messages)
    receiver, writer = build(async_store, broker)
    writer.start()
    with caplog.at_level("INFO", logger="bmw_collector.mqtt_receiver"):
        receiver.start()
        await run_until(lambda: receiver.messages_received >= 40)
    await receiver.stop()
    await writer.stop()
    # A drive arrives in bursts; per-message INFO would bury everything else.
    assert caplog.text.count("receiving vehicle data") == 1


async def test_the_first_message_counter_resets_per_connection(
    async_store: EventStore,
) -> None:
    broker = FakeBroker(messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", b"{}")])
    receiver, writer = build(async_store, broker)
    writer.start()
    receiver.start()
    await run_until(lambda: receiver.messages_received >= 1)
    assert receiver._messages_this_connection == 1
    await receiver.stop()
    await writer.stop()


async def test_an_idle_connection_still_recycles_before_token_expiry(
    async_store: EventStore,
) -> None:
    """The regression test for a silent hourly disconnect.

    With the recycle check gated behind message arrival, a parked car meant the
    clock was never checked: the id token expired, BMW dropped the connection,
    and the collector reconnected reactively every hour with a backoff gap.
    """
    auth = StubAuth(expiry=time.time() + TOKEN_RECYCLE_MARGIN_SECONDS + 0.3)
    # No messages at all: exactly the parked-car case.
    broker = FakeBroker()
    receiver, writer = build(async_store, broker, auth=auth)
    writer.start()
    receiver.start()
    await run_until(lambda: broker.connections >= 2, timeout=15)
    await receiver.stop()
    await writer.stop()
    # It recycled on its own rather than waiting to be disconnected, and did
    # so with a freshly minted token.
    assert broker.credentials[0][1] != broker.credentials[1][1]


async def test_a_message_arriving_at_the_recycle_deadline_is_not_lost(
    async_store: EventStore,
) -> None:
    payload = stream_payload(VALID_VIN, index=99)
    auth = StubAuth(expiry=time.time() + TOKEN_RECYCLE_MARGIN_SECONDS + 0.2)
    broker = FakeBroker(messages_to_send=[FakeMessage(f"{GCID}/{VALID_VIN}", payload)])
    receiver, writer = build(async_store, broker, auth=auth)
    writer.start()
    receiver.start()
    await run_until(lambda: writer.committed >= 1, timeout=15)
    await receiver.stop()
    await writer.stop()
    stored = await asyncio.to_thread(lambda: list(async_store.iter_events_sync()))
    # QoS 0 has no redelivery, so a message dequeued as we tear the connection
    # down must still reach storage.
    assert any(e.payload == payload for e in stored)


async def test_idle_recycle_does_not_spin(async_store: EventStore) -> None:
    # A far-future expiry must not cause repeated reconnects just because the
    # clock is now checked on a timer.
    auth = StubAuth(expiry=time.time() + 3600)
    broker = FakeBroker()
    receiver, writer = build(async_store, broker, auth=auth)
    writer.start()
    receiver.start()
    await run_until(lambda: broker.connections >= 1)
    await asyncio.sleep(0.5)
    await receiver.stop()
    await writer.stop()
    assert broker.connections == 1
