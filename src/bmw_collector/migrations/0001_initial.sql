-- Raw, byte-exact archive of everything received from BMW CarData.
CREATE TABLE raw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_uuid TEXT NOT NULL UNIQUE,
    received_at_utc TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('mqtt', 'rest')),
    account_key TEXT NOT NULL,
    vin TEXT,
    topic TEXT,
    mqtt_qos INTEGER,
    event_time_utc TEXT,
    payload BLOB NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_raw_events_received ON raw_events(received_at_utc);
CREATE INDEX idx_raw_events_vin_received ON raw_events(vin, received_at_utc);

-- Scheduler cursors, quota counters and other durable process state.
CREATE TABLE collector_state (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

-- Operational incidents worth surfacing after the fact (write failures,
-- auth denials, queue overflow, quota exhaustion).
CREATE TABLE collector_incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at_utc TEXT NOT NULL,
    kind TEXT NOT NULL,
    details_json TEXT NOT NULL
);

CREATE INDEX idx_collector_incidents_occurred ON collector_incidents(occurred_at_utc);
