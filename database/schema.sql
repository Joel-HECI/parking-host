-- ============================================================
-- PARKING IoT DATABASE
-- ============================================================

BEGIN;

-- ============================================================
-- DEVICES / ESP32 SLAVES
-- ============================================================

CREATE TABLE IF NOT EXISTS devices (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    device_id       TEXT NOT NULL UNIQUE,

    name            TEXT,

    spot            TEXT,

    token_hash      TEXT NOT NULL,

    enabled         BOOLEAN NOT NULL DEFAULT TRUE,

    last_seen       TIMESTAMPTZ,

    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ============================================================
-- EVENTS
-- ============================================================

CREATE TABLE IF NOT EXISTS events (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    device_id       BIGINT NOT NULL
                    REFERENCES devices(id)
                    ON DELETE CASCADE,

    event_type      TEXT NOT NULL,

    event_time      TIMESTAMPTZ,

    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,

    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb
);


-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_events_device_id
    ON events(device_id);

CREATE INDEX IF NOT EXISTS idx_events_event_type
    ON events(event_type);

CREATE INDEX IF NOT EXISTS idx_events_event_time
    ON events(event_time);

CREATE INDEX IF NOT EXISTS idx_events_received_at
    ON events(received_at);


-- ============================================================
-- AUTOMATIC updated_at
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS devices_updated_at
ON devices;


CREATE TRIGGER devices_updated_at
BEFORE UPDATE ON devices
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();


COMMIT;
