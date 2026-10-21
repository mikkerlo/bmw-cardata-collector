-- Vehicles this collector knows about. Rows are never deleted when a VIN is
-- removed from the allowlist: history and first-observed data are retained.
CREATE TABLE vehicles (
    vin TEXT PRIMARY KEY,
    account_key TEXT NOT NULL,
    first_observed_at_utc TEXT NOT NULL,
    last_seen_at_utc TEXT,
    mapping_type TEXT,
    mapped_since TEXT,
    collection_enabled INTEGER NOT NULL DEFAULT 1,
    notes_json TEXT NOT NULL DEFAULT '{}'
);
