"""Prometheus metrics.

No metric carries a VIN or GCID label: the management endpoint is meant to be
scraped by whatever already runs on the host, and label values end up in
dashboards, alert payloads and remote-write storage. Per-vehicle detail belongs
in the archive, not in metrics.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, Info

REGISTRY = CollectorRegistry()

collector_start_timestamp_seconds = Gauge(
    "collector_start_timestamp_seconds",
    "Unix timestamp at which this collector process started.",
    registry=REGISTRY,
)
collector_ready = Gauge(
    "collector_ready",
    "1 when the collector is ready (config valid, storage writable, receiver running).",
    registry=REGISTRY,
)
collector_startup_duration_seconds = Gauge(
    "collector_startup_duration_seconds",
    "Seconds from process start until the collector first became ready.",
    registry=REGISTRY,
)
collector_phase_info = Info(
    "collector_phase",
    "Current lifecycle phase of the collector.",
    registry=REGISTRY,
)
collector_build_info = Info(
    "collector_build",
    "Build and node identity of this collector instance.",
    registry=REGISTRY,
)

bmw_mqtt_connected = Gauge(
    "bmw_mqtt_connected",
    "1 while the MQTT streaming connection to BMW is established.",
    registry=REGISTRY,
)
bmw_mqtt_reconnects_total = Counter(
    "bmw_mqtt_reconnects_total",
    "Number of MQTT (re)connection attempts.",
    registry=REGISTRY,
)
bmw_mqtt_messages_received_total = Counter(
    "bmw_mqtt_messages_received_total",
    "MQTT messages received from BMW, before storage.",
    registry=REGISTRY,
)
bmw_mqtt_last_message_timestamp_seconds = Gauge(
    "bmw_mqtt_last_message_timestamp_seconds",
    "Unix timestamp of the most recent MQTT message. A parked car sends nothing, "
    "so a stale value is not by itself a fault.",
    registry=REGISTRY,
)
bmw_mqtt_subscribed_topics = Gauge(
    "bmw_mqtt_subscribed_topics",
    "Number of MQTT topics currently subscribed.",
    registry=REGISTRY,
)

collector_events_committed_total = Counter(
    "collector_events_committed_total",
    "Events durably committed to SQLite.",
    ["source"],
    registry=REGISTRY,
)
collector_storage_write_failures_total = Counter(
    "collector_storage_write_failures_total",
    "Storage write attempts that failed. These events are NOT committed.",
    registry=REGISTRY,
)
collector_storage_writable = Gauge(
    "collector_storage_writable",
    "1 when the last durable-storage write probe succeeded.",
    registry=REGISTRY,
)
collector_queue_depth = Gauge(
    "collector_queue_depth",
    "Events waiting in the in-process write queue.",
    registry=REGISTRY,
)
collector_queue_overflows_total = Counter(
    "collector_queue_overflows_total",
    "Events dropped because the write queue was full. Each one is lost data.",
    registry=REGISTRY,
)
collector_commit_duration_seconds = Histogram(
    "collector_commit_duration_seconds",
    "Time spent committing a batch of events to SQLite.",
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
    registry=REGISTRY,
)

bmw_rest_requests_total = Counter(
    "bmw_rest_requests_total",
    "CarData REST requests issued, by response class.",
    ["status_class"],
    registry=REGISTRY,
)
bmw_rest_budget_estimate = Gauge(
    "bmw_rest_budget_estimate",
    "Locally estimated REST requests left in today's configured budget. BMW's own "
    "counter may be lower if another client shares the account.",
    registry=REGISTRY,
)
bmw_rest_last_success_timestamp_seconds = Gauge(
    "bmw_rest_last_success_timestamp_seconds",
    "Unix timestamp of the last successful REST request.",
    registry=REGISTRY,
)
bmw_rest_quota_exhausted = Gauge(
    "bmw_rest_quota_exhausted",
    "1 while BMW is rejecting REST calls with a quota error.",
    registry=REGISTRY,
)

bmw_auth_expires_at_seconds = Gauge(
    "bmw_auth_expires_at_seconds",
    "Unix timestamp at which the current access token expires.",
    registry=REGISTRY,
)
bmw_auth_errors_total = Counter(
    "bmw_auth_errors_total",
    "Authorization failures.",
    registry=REGISTRY,
)
bmw_auth_reauthorization_required = Gauge(
    "bmw_auth_reauthorization_required",
    "1 when a human must run 'collector auth login' again.",
    registry=REGISTRY,
)

collector_incidents_total = Counter(
    "collector_incidents_total",
    "Operational incidents recorded, by kind.",
    ["kind"],
    registry=REGISTRY,
)
