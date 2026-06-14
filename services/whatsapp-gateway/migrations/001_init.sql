-- Migration 001: core tables for the WhatsApp gateway.
-- Applied automatically on gateway startup (idempotent: IF NOT EXISTS everywhere).

-- Connections owned/claimed by gateway pods. One row per WhatsApp phone (JID).
CREATE TABLE IF NOT EXISTS whatsapp_connections (
    id           BIGSERIAL PRIMARY KEY,
    jid          TEXT,                       -- WhatsApp JID, NULL until first login
    label        TEXT NOT NULL DEFAULT '',   -- human label for the phone slot
    pod_id       TEXT,                       -- which pod currently owns this connection
    status       TEXT NOT NULL DEFAULT 'disconnected', -- disconnected | claimed | connected
    session_data BYTEA,                      -- whatsmeow device session blob (gob-encoded)
    last_seen    TIMESTAMPTZ,                -- heartbeat from the owning pod
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index used by the claim query: find disconnected or stale-owned rows fast.
CREATE INDEX IF NOT EXISTS idx_connections_claimable
    ON whatsapp_connections (status, last_seen);

CREATE INDEX IF NOT EXISTS idx_connections_pod
    ON whatsapp_connections (pod_id);

-- Append-only log of every message in/out. Powers management analytics.
CREATE TABLE IF NOT EXISTS message_logs (
    id            BIGSERIAL PRIMARY KEY,
    event_id      TEXT NOT NULL,             -- deterministic id (matches Kafka event_id)
    connection_id BIGINT REFERENCES whatsapp_connections(id) ON DELETE SET NULL,
    jid           TEXT NOT NULL,             -- conversation JID
    direction     TEXT NOT NULL,             -- inbound | outbound
    body          TEXT,
    bytes         INTEGER NOT NULL DEFAULT 0,
    trace_id      TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dedupe guard at the DB layer too (in addition to Redis on the bot side).
CREATE UNIQUE INDEX IF NOT EXISTS uq_message_logs_event_dir
    ON message_logs (event_id, direction);

CREATE INDEX IF NOT EXISTS idx_message_logs_created
    ON message_logs (created_at);

CREATE INDEX IF NOT EXISTS idx_message_logs_conn
    ON message_logs (connection_id);

-- No seed data. Connections are created on demand via POST /api/connections.
-- The gateway claims them only after a user explicitly registers a phone.
