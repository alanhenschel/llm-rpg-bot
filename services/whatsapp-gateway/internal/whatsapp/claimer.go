package whatsapp

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/db"
)

// Claimer periodically claims connection slots from the DB and brings them up.
// It also runs the heartbeat that keeps this pod's ownership alive.
type Claimer struct {
	store     *db.Store
	manager   *Manager
	podID     string
	batch     int
	staleness time.Duration
	heartbeat time.Duration
	interval  time.Duration
	logger    zerolog.Logger
}

// NewClaimer wires a claimer.
func NewClaimer(store *db.Store, manager *Manager, podID string, batch int, staleness, heartbeat, interval time.Duration, logger zerolog.Logger) *Claimer {
	return &Claimer{
		store: store, manager: manager, podID: podID,
		batch: batch, staleness: staleness, heartbeat: heartbeat,
		interval: interval, logger: logger,
	}
}

// Run blocks running the claim loop + heartbeat until ctx is cancelled.
func (c *Claimer) Run(ctx context.Context) {
	// Heartbeat ticker (keeps ownership fresh so peers don't steal our slots).
	hb := time.NewTicker(c.heartbeat)
	defer hb.Stop()
	// Claim ticker (looks for new/abandoned slots to take over).
	claim := time.NewTicker(c.interval)
	defer claim.Stop()

	c.claimOnce(ctx) // claim immediately on startup

	for {
		select {
		case <-ctx.Done():
			return
		case <-hb.C:
			if err := c.store.Heartbeat(ctx, c.podID); err != nil {
				c.logger.Error().Err(err).Msg("heartbeat failed")
			}
		case <-claim.C:
			c.claimOnce(ctx)
		}
	}
}

func (c *Claimer) claimOnce(ctx context.Context) {
	claimed, err := c.store.ClaimConnections(ctx, c.podID, c.batch, c.staleness)
	if err != nil {
		c.logger.Error().Err(err).Msg("claim connections failed")
		return
	}
	for _, conn := range claimed {
		if c.manager.Owns(conn.ID) {
			continue // already managing it (re-claimed our own row)
		}
		c.logger.Info().Int64("conn", conn.ID).Str("jid", conn.JID).
			Str("label", conn.Label).Msg("claimed connection slot")
		if err := c.manager.Bring(ctx, conn); err != nil {
			c.logger.Error().Err(err).Int64("conn", conn.ID).Msg("bring connection up")
		}
	}
}
