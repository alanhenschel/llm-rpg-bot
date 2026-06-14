// Package telemetry provides structured JSON logging and trace-id propagation.
//
// Every log line emitted across the whole pipeline shares the same field schema:
//   service, level, timestamp, trace_id, event_id (where applicable).
// We standardize the keys here so Loki/Grafana queries are identical in Go and Python.
package telemetry

import (
	"context"
	"os"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	serviceName = "whatsapp-gateway"
)

type ctxKey string

const traceIDKey ctxKey = "trace_id"

// Init configures the global zerolog logger to emit JSON to stdout with our schema.
func Init(level string) zerolog.Logger {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.TimestampFieldName = "timestamp"
	zerolog.LevelFieldName = "level"

	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	logger := zerolog.New(os.Stdout).
		Level(lvl).
		With().
		Timestamp().
		Str("service", serviceName).
		Logger()

	return logger
}

// NewTraceID returns a fresh trace id for a new request/message flow.
func NewTraceID() string {
	return uuid.NewString()
}

// WithTraceID stores a trace id in the context for downstream propagation.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// TraceIDFromContext extracts the trace id, generating one if absent.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok && v != "" {
		return v
	}
	return NewTraceID()
}

// LogWithTrace returns a logger event-builder pre-tagged with the trace id.
func LogWithTrace(logger zerolog.Logger, ctx context.Context) zerolog.Logger {
	return logger.With().Str("trace_id", TraceIDFromContext(ctx)).Logger()
}
