// Package db implements the PostgreSQL store: connection ownership claiming
// (via SELECT ... FOR UPDATE SKIP LOCKED), heartbeats, session persistence,
// and the message log used by the management analytics.
package db

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// Connection represents a row in whatsapp_connections.
type Connection struct {
	ID          int64
	JID         string
	Label       string
	PodID       string
	Status      string
	SessionData []byte
	LastSeen    *time.Time
}

// Store wraps a pgx connection pool.
type Store struct {
	pool   *pgxpool.Pool
	logger zerolog.Logger
}

// New opens the pool and runs migrations from the provided filesystem.
// The migrations FS is embedded in the main package and passed in here, which
// avoids Go's restriction that //go:embed cannot reference parent directories.
func New(ctx context.Context, dsn string, migrations fs.FS, logger zerolog.Logger) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	s := &Store{pool: pool, logger: logger}
	if migrations != nil {
		if err := s.migrate(ctx, migrations); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context, migrations fs.FS) error {
	var names []string
	err := fs.WalkDir(migrations, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && len(path) > 4 && path[len(path)-4:] == ".sql" {
			names = append(names, path)
		}
		return nil
	})
	if err != nil {
		s.logger.Warn().Err(err).Msg("walk migrations; skipping auto-migrate")
		return nil
	}
	sort.Strings(names) // apply in lexical order (001_, 002_, ...)
	for _, name := range names {
		sqlBytes, err := fs.ReadFile(migrations, name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := s.pool.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		s.logger.Info().Str("migration", name).Msg("migration applied")
	}
	return nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// ClaimConnections atomically claims up to batch connections for this pod using
// SELECT ... FOR UPDATE SKIP LOCKED. Concurrent pods will never grab the same row:
// the second pod skips the locked row instead of blocking.
//
// A row is claimable if it is 'disconnected', OR it is owned by another pod whose
// heartbeat (last_seen) is older than the staleness window (owner pod presumed dead).
func (s *Store) ClaimConnections(ctx context.Context, podID string, batch int, staleness time.Duration) ([]Connection, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	rows, err := tx.Query(ctx, `
		SELECT id, COALESCE(jid, ''), label, COALESCE(pod_id, ''), status, session_data, last_seen
		FROM whatsapp_connections
		WHERE status = 'disconnected'
		   OR (pod_id IS DISTINCT FROM $1 AND (last_seen IS NULL OR last_seen < now() - $2::interval))
		ORDER BY last_seen ASC NULLS FIRST
		LIMIT $3
		FOR UPDATE SKIP LOCKED
	`, podID, staleness.String(), batch)
	if err != nil {
		return nil, fmt.Errorf("select for claim: %w", err)
	}

	var claimed []Connection
	for rows.Next() {
		var c Connection
		if err := rows.Scan(&c.ID, &c.JID, &c.Label, &c.PodID, &c.Status, &c.SessionData, &c.LastSeen); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan claim row: %w", err)
		}
		claimed = append(claimed, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range claimed {
		if _, err := tx.Exec(ctx, `
			UPDATE whatsapp_connections
			SET pod_id = $1, status = 'claimed', last_seen = now(), updated_at = now()
			WHERE id = $2
		`, podID, claimed[i].ID); err != nil {
			return nil, fmt.Errorf("update claim row %d: %w", claimed[i].ID, err)
		}
		claimed[i].PodID = podID
		claimed[i].Status = "claimed"
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return claimed, nil
}

// Heartbeat refreshes last_seen for every connection owned by this pod so other
// pods don't consider them stale.
func (s *Store) Heartbeat(ctx context.Context, podID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE whatsapp_connections
		SET last_seen = now()
		WHERE pod_id = $1 AND status IN ('claimed', 'connected')
	`, podID)
	return err
}

// SetStatus updates the status (and jid if provided) of a connection.
func (s *Store) SetStatus(ctx context.Context, id int64, status, jid string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE whatsapp_connections
		SET status = $2,
		    jid = CASE WHEN $3 <> '' THEN $3 ELSE jid END,
		    last_seen = now(),
		    updated_at = now()
		WHERE id = $1
	`, id, status, jid)
	return err
}

// SaveSession persists the whatsmeow device session blob.
func (s *Store) SaveSession(ctx context.Context, id int64, session []byte) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE whatsapp_connections
		SET session_data = $2, updated_at = now()
		WHERE id = $1
	`, id, session)
	return err
}

// ReleaseAll marks every connection owned by this pod as disconnected. Called on
// graceful shutdown so another pod can pick them up immediately.
func (s *Store) ReleaseAll(ctx context.Context, podID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE whatsapp_connections
		SET status = 'disconnected', pod_id = NULL, updated_at = now()
		WHERE pod_id = $1
	`, podID)
	return err
}

// LogMessage records an inbound/outbound message for analytics. Idempotent on
// (event_id, direction) — a duplicate insert is silently ignored.
func (s *Store) LogMessage(ctx context.Context, eventID string, connID int64, jid, direction, body string, bytes int, traceID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO message_logs (event_id, connection_id, jid, direction, body, bytes, trace_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (event_id, direction) DO NOTHING
	`, eventID, connID, jid, direction, body, bytes, traceID)
	return err
}

// CreateConnection inserts a new slot with the given label and returns the row.
// Status starts as 'disconnected' so the claimer loop (or an immediate Bring call)
// can pick it up.
func (s *Store) CreateConnection(ctx context.Context, label string) (*Connection, error) {
	var c Connection
	err := s.pool.QueryRow(ctx, `
		INSERT INTO whatsapp_connections (label, status)
		VALUES ($1, 'disconnected')
		RETURNING id, COALESCE(jid, ''), label, COALESCE(pod_id, ''), status, session_data, last_seen
	`, label).Scan(&c.ID, &c.JID, &c.Label, &c.PodID, &c.Status, &c.SessionData, &c.LastSeen)
	if err != nil {
		return nil, fmt.Errorf("create connection: %w", err)
	}
	return &c, nil
}

// GetConnection returns a single connection by id.
func (s *Store) GetConnection(ctx context.Context, id int64) (*Connection, error) {
	var c Connection
	err := s.pool.QueryRow(ctx, `
		SELECT id, COALESCE(jid, ''), label, COALESCE(pod_id, ''), status, session_data, last_seen
		FROM whatsapp_connections WHERE id = $1
	`, id).Scan(&c.ID, &c.JID, &c.Label, &c.PodID, &c.Status, &c.SessionData, &c.LastSeen)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// Pool exposes the underlying pool for advanced callers (used sparingly).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }
