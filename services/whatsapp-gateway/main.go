// Command whatsapp-gateway manages multiple WhatsApp connections, claims them from
// PostgreSQL with distributed locking, and bridges messages to/from Kafka.
package main

import (
	"context"
	"embed"
	"io/fs"
	"os"
	"os/signal"
	"syscall"
	"time"

	// pgx stdlib driver registration for whatsmeow's sqlstore ("pgx" driver name).
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/config"
	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/db"
	xkafka "github.com/alan/ia-pipeline/whatsapp-gateway/internal/kafka"
	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/server"
	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/telemetry"
	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/whatsapp"
)

// migrationsFS embeds the SQL migrations. Embedding from the main package (where
// migrations/ is a subdirectory) is valid; embedding via ../ is not.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

func main() {
	cfg, err := config.Load()
	logger := telemetry.Init("info")
	if err != nil {
		logger.Fatal().Err(err).Msg("load config")
	}
	logger = telemetry.Init(cfg.LogLevel)
	logger = logger.With().Str("pod_id", cfg.PodID).Logger()
	logger.Info().Msg("whatsapp-gateway starting")

	// Root context cancelled on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- PostgreSQL store ---
	migSub, _ := fs.Sub(migrationsFS, "migrations")
	store, err := db.New(ctx, cfg.DatabaseURL, migSub, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("init db store")
	}
	defer store.Close()

	// --- Kafka producer (idempotent) ---
	producer, err := xkafka.NewProducer(cfg.KafkaBrokers, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("init kafka producer")
	}
	defer producer.Close(context.Background())

	// --- WhatsApp manager ---
	manager, err := whatsapp.New(ctx, whatsapp.Config{
		PodID:        cfg.PodID,
		TopicInbound: cfg.TopicInbound,
		TopicEvents:  cfg.TopicEvents,
		DatabaseURL:  cfg.DatabaseURL,
	}, store, producer, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("init whatsapp manager")
	}

	// --- Outbound consumer: handle send/disconnect commands ---
	handler := func(hctx context.Context, msg xkafka.OutboundMessage, traceID string) error {
		if traceID == "" {
			traceID = msg.TraceID
		}
		switch msg.Command {
		case "disconnect":
			if !manager.Owns(msg.ConnectionID) {
				return nil // another pod owns it; ignore (will be picked up there)
			}
			return manager.Disconnect(hctx, msg.ConnectionID)
		default: // "" or "send"
			if !manager.Owns(msg.ConnectionID) {
				return nil // not ours; let the owning pod handle it
			}
			return manager.SendText(hctx, msg.ConnectionID, msg.JID, msg.Body, traceID, msg.EventID)
		}
	}
	consumer, err := xkafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, cfg.TopicOutbound, handler, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("init kafka consumer")
	}
	defer consumer.Close()

	// --- Claimer: distributed claiming + heartbeat + reconnection ---
	claimer := whatsapp.NewClaimer(
		store, manager, cfg.PodID,
		cfg.ClaimBatchSize, cfg.StalenessWindow, cfg.HeartbeatInterval, cfg.ClaimInterval,
		logger,
	)

	// --- HTTP server ---
	httpSrv := server.New(cfg.HTTPAddr, cfg.PodID, manager, logger)

	// Launch background loops.
	go claimer.Run(ctx)
	go consumer.Run(ctx)
	go func() {
		if err := httpSrv.Start(); err != nil {
			logger.Error().Err(err).Msg("http server stopped")
		}
	}()

	logger.Info().Msg("whatsapp-gateway running")

	// Block until shutdown signal.
	<-ctx.Done()
	logger.Info().Msg("shutdown signal received; cleaning up")

	// Graceful shutdown with a bounded deadline.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	manager.Shutdown()
	if err := store.ReleaseAll(shutdownCtx, cfg.PodID); err != nil {
		logger.Error().Err(err).Msg("release connections on shutdown")
	}
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("http shutdown")
	}
	logger.Info().Msg("whatsapp-gateway stopped")
	os.Exit(0)
}
