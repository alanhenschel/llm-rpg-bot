// Package config loads runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Config holds all gateway configuration.
type Config struct {
	PodID    string // unique identity of this pod; used for connection ownership
	LogLevel string

	DatabaseURL string

	KafkaBrokers       []string
	TopicInbound       string
	TopicOutbound      string
	TopicEvents        string
	ConsumerGroup      string

	HTTPAddr string

	// gRPC address of the LLM bot (hot path).
	// Empty string disables the gRPC client (Kafka-only fallback mode).
	BotGRPCAddr string

	// Connection claiming tuning.
	ClaimBatchSize    int
	HeartbeatInterval time.Duration
	StalenessWindow   time.Duration
	ClaimInterval     time.Duration
}

// Load reads configuration from the environment, applying sane defaults.
func Load() (*Config, error) {
	podID := getenv("POD_ID", "")
	if podID == "" {
		// Stable-ish fallback: hostname + random suffix so two pods never collide.
		host, _ := os.Hostname()
		podID = fmt.Sprintf("%s-%s", host, uuid.NewString()[:8])
	}

	dbURL := getenv("DATABASE_URL", "")
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	cfg := &Config{
		PodID:             podID,
		LogLevel:          getenv("LOG_LEVEL", "info"),
		DatabaseURL:       dbURL,
		KafkaBrokers:      strings.Split(getenv("KAFKA_BROKERS", "kafka:9092"), ","),
		TopicInbound:      getenv("TOPIC_INBOUND", "whatsapp.messages.inbound"),
		TopicOutbound:     getenv("TOPIC_OUTBOUND", "whatsapp.messages.outbound"),
		TopicEvents:       getenv("TOPIC_EVENTS", "whatsapp.events"),
		ConsumerGroup:     getenv("KAFKA_CONSUMER_GROUP", "whatsapp-gateway"),
		HTTPAddr:          getenv("HTTP_ADDR", ":8080"),
		BotGRPCAddr:       getenv("BOT_GRPC_ADDR", ""),
		ClaimBatchSize:    getenvInt("CLAIM_BATCH_SIZE", 10),
		HeartbeatInterval: getenvDuration("HEARTBEAT_INTERVAL", 10*time.Second),
		StalenessWindow:   getenvDuration("STALENESS_WINDOW", 30*time.Second),
		ClaimInterval:     getenvDuration("CLAIM_INTERVAL", 15*time.Second),
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
