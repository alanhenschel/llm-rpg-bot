-- Postgres bootstrap. The gateway runs its own migrations on startup; this only ensures
-- useful extensions exist. The database/user themselves are created via the
-- POSTGRES_DB / POSTGRES_USER env vars on the official image.

-- pgcrypto is handy for any future UUID/crypto needs.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
