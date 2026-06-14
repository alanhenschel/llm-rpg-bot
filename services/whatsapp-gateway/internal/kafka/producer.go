package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is an idempotent Kafka producer. Idempotence + acks=all guarantees the
// broker dedupes producer retries (exactly-once at the partition level).
type Producer struct {
	client *kgo.Client
	logger zerolog.Logger
}

// NewProducer builds an idempotent producer client.
func NewProducer(brokers []string, logger zerolog.Logger) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// franz-go enables idempotence by default when acks=all and no manual
		// disabling; we set it explicitly for clarity.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka producer: %w", err)
	}
	return &Producer{client: client, logger: logger}, nil
}

// publish marshals v and produces it with the given key and trace header.
func (p *Producer) publish(ctx context.Context, topic, key, traceID string, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "trace_id", Value: []byte(traceID)},
		},
	}
	// Synchronous produce so we surface errors to the caller and preserve ordering
	// semantics per key.
	res := p.client.ProduceSync(ctx, rec)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}

// PublishInbound sends a received WhatsApp message to the inbound topic, keyed by
// JID so a conversation stays on one partition (ordering preserved).
func (p *Producer) PublishInbound(ctx context.Context, topic string, msg InboundMessage) error {
	return p.publish(ctx, topic, msg.JID, msg.TraceID, msg)
}

// PublishEvent sends a connection lifecycle event.
func (p *Producer) PublishEvent(ctx context.Context, topic string, evt ConnectionEvent) error {
	return p.publish(ctx, topic, evt.JID, evt.TraceID, evt)
}

// Close flushes and closes the producer.
func (p *Producer) Close(ctx context.Context) {
	if err := p.client.Flush(ctx); err != nil {
		p.logger.Error().Err(err).Msg("flush producer on close")
	}
	p.client.Close()
}
