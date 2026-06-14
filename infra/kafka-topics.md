# Kafka Topics & Event Schema

All messages are JSON-encoded UTF-8. Every event carries `event_id`, `timestamp` (unix ms),
and `pod_id` (where produced by the gateway). `trace_id` is propagated via a **Kafka record
header** (key `trace_id`), not the payload, so it survives across services and is greppable
end-to-end in Loki.

Producers on the Go gateway are configured **idempotent** (`acks=all`, idempotence enabled),
so the broker dedupes producer retries at the partition level. Application-level idempotency
is layered on top via the deterministic `event_id` (UUIDv5) + a Redis `SET NX` guard in the
LLM bot and a `(event_id, direction)` unique index in PostgreSQL.

---

## Topic: `whatsapp.messages.inbound`

Raw inbound messages received from WhatsApp by the gateway. **Keyed by JID** so all messages
in one conversation land on the same partition (per-conversation ordering preserved).

Producer: `whatsapp-gateway` · Consumer: `llm-bot`

```json
{
  "event_id": "uuid-v5 (sha1 of sender|timestamp|sha256(body))",
  "trace_id": "uuid-v4 (also in header)",
  "pod_id": "gateway-pod-abc123",
  "connection_id": 1,
  "jid": "5511999999999@s.whatsapp.net",
  "sender_jid": "5511999999999@s.whatsapp.net",
  "body": "Tell me about Skyrim dragon shouts",
  "bytes": 33,
  "timestamp": 1718200000000
}
```

## Topic: `whatsapp.messages.outbound`

Send/control commands consumed by the gateway. **Keyed by JID** (or connection id for control
commands). Produced by both the `llm-bot` (responses) and `management-api` (control commands).

Producers: `llm-bot`, `management-api` · Consumer: `whatsapp-gateway`

```json
{
  "event_id": "uuid",
  "trace_id": "uuid (also in header)",
  "connection_id": 1,
  "jid": "5511999999999@s.whatsapp.net",
  "body": "In Skyrim, Dragon Shouts (Thu'um) ...",
  "command": "send",          // "send" (default) or "disconnect"
  "timestamp": 1718200001000
}
```

The gateway only acts on a command if **it owns** that `connection_id` (otherwise it ignores
the record, letting the owning pod handle it). This keeps send/disconnect routing correct in a
multi-pod deployment without a central router.

## Topic: `whatsapp.events`

Connection lifecycle events (QR codes, connect/disconnect, logout). Used by observability and
could drive future UI notifications.

Producer: `whatsapp-gateway` · Consumers: observability / management (optional)

```json
{
  "event_id": "uuid",
  "trace_id": "uuid (also in header)",
  "pod_id": "gateway-pod-abc123",
  "connection_id": 1,
  "jid": "5511999999999@s.whatsapp.net",
  "type": "qr",               // qr | connected | disconnected | logged_out
  "payload": "2@abc...==",    // e.g. the QR code string for type=qr
  "timestamp": 1718200000000
}
```

---

## Topic creation

Topics are auto-created by Kafka (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` in compose) on first
use. For production you would pre-create them with explicit partition/replication settings:

```bash
kafka-topics --create --topic whatsapp.messages.inbound  --partitions 6 --replication-factor 3
kafka-topics --create --topic whatsapp.messages.outbound --partitions 6 --replication-factor 3
kafka-topics --create --topic whatsapp.events            --partitions 3 --replication-factor 3
```

Partition count gates max consumer parallelism per group. 6 partitions on the message topics
allows up to 6 concurrent `llm-bot` / `whatsapp-gateway` instances per group.
