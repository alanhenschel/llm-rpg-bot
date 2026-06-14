// Package kafka defines the wire event schema and the producer/consumer wiring.
package kafka

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
)

// eventNamespace is a fixed UUIDv5 namespace so deterministic ids are stable
// across pods, restarts, and services. DO NOT change once in production —
// changing it would break cross-service dedupe.
var eventNamespace = uuid.MustParse("6f1c1d2e-3a4b-5c6d-7e8f-9a0b1c2d3e4f")

// InboundMessage is published to whatsapp.messages.inbound.
type InboundMessage struct {
	EventID      string `json:"event_id"`
	TraceID      string `json:"trace_id"`
	PodID        string `json:"pod_id"`
	ConnectionID int64  `json:"connection_id"`
	JID          string `json:"jid"`      // conversation JID (sender)
	SenderJID    string `json:"sender_jid"`
	Body         string `json:"body"`
	Bytes        int    `json:"bytes"`
	Timestamp    int64  `json:"timestamp"` // unix ms
}

// OutboundMessage is consumed from whatsapp.messages.outbound (send command).
type OutboundMessage struct {
	EventID      string `json:"event_id"`
	TraceID      string `json:"trace_id"`
	ConnectionID int64  `json:"connection_id"`
	JID          string `json:"jid"` // destination
	Body         string `json:"body"`
	// Command lets the management service drive non-message actions.
	// Empty/"send" = send a text message; "disconnect" = disconnect a connection.
	Command string `json:"command,omitempty"`
	Timestamp int64 `json:"timestamp"`
}

// ConnectionEvent is published to whatsapp.events.
type ConnectionEvent struct {
	EventID      string `json:"event_id"`
	TraceID      string `json:"trace_id"`
	PodID        string `json:"pod_id"`
	ConnectionID int64  `json:"connection_id"`
	JID          string `json:"jid"`
	Type         string `json:"type"` // qr | connected | disconnected | logged_out
	Payload      string `json:"payload,omitempty"` // e.g. QR code string
	Timestamp    int64  `json:"timestamp"`
}

// DeterministicEventID derives a stable UUIDv5 from sender + timestamp + body hash.
// The same physical WhatsApp message always maps to the same id regardless of which
// pod or retry produced it → enables cross-service idempotency.
func DeterministicEventID(sender string, timestampMs int64, body string) string {
	bodyHash := sha256.Sum256([]byte(body))
	name := fmt.Sprintf("%s|%d|%s", sender, timestampMs, hex.EncodeToString(bodyHash[:]))
	return uuid.NewSHA1(eventNamespace, []byte(name)).String()
}
