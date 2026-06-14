package kafka

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OutboundHandler processes a send command. Returning an error causes the record
// to NOT be committed (so it will be retried).
type OutboundHandler func(ctx context.Context, msg OutboundMessage, traceID string) error

// Consumer reads send commands from the outbound topic.
type Consumer struct {
	client  *kgo.Client
	logger  zerolog.Logger
	handler OutboundHandler
}

// NewConsumer builds a consumer-group client subscribed to the outbound topic.
func NewConsumer(brokers []string, group, topic string, handler OutboundHandler, logger zerolog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		// Manual commit after successful handling → at-least-once; app-level
		// idempotency (deterministic event_id + DB unique index) makes it safe.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client, logger: logger, handler: handler}, nil
}

// Run polls and dispatches until the context is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return
				}
				c.logger.Error().Err(e.Err).Str("topic", e.Topic).Msg("fetch error")
			}
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			traceID := headerValue(rec, "trace_id")
			var msg OutboundMessage
			if err := json.Unmarshal(rec.Value, &msg); err != nil {
				c.logger.Error().Err(err).Msg("unmarshal outbound message; skipping (poison)")
				return // skip poison message but still commit below
			}
			if err := c.handler(ctx, msg, traceID); err != nil {
				c.logger.Error().Err(err).Str("trace_id", traceID).
					Str("event_id", msg.EventID).Msg("handle outbound failed; will retry")
				return // do not commit this offset → retried
			}
		})

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.logger.Error().Err(err).Msg("commit offsets")
		}
	}
}

// Close shuts the consumer down.
func (c *Consumer) Close() { c.client.Close() }

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
