CREATE TABLE stock (
    sku       VARCHAR(64) PRIMARY KEY,
    available INT    NOT NULL CHECK (available >= 0), -- last line of defence against overselling
    version   BIGINT NOT NULL DEFAULT 0               -- optimistic locking (@Version)
);

-- Idempotent consumer: one row per event we have already handled.
-- Written in the same transaction as the stock change.
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
