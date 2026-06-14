# WhatsApp AI Chatbot Pipeline

A production-shaped, event-driven WhatsApp chatbot that answers questions about RPG video games
using a **RAG-grounded local LLM** (Llama 3 via Ollama), scales horizontally via distributed
PostgreSQL locking, and ships with a real-time **management dashboard** and full **log
observability**.

Built as a portfolio project to demonstrate distributed systems design: multi-pod connection
ownership with zero race conditions, idempotent Kafka streaming, a gRPC hot path for low
latency, polyglot microservices (Go + Python + React), and 12-factor observability.

---

## Architecture

```
                    ┌─ gRPC (hot path) ─────────────────────────────────┐
                    │  Bot.Process(msg) → stream ReplyChunk              │
WhatsApp phones     │                                                    │
     │  ▲           │   ┌─────────────────────────────────────────────┐ │
     │  │           │   │                  Kafka                      │ │
     ▼  │           │   │  whatsapp.messages.inbound   (analytics)    │ │
┌──────────────┐    │   │  whatsapp.messages.outbound  (analytics)    │ │
│  WhatsApp    │────┘   │  whatsapp.events             (observability)│ │
│  Gateway     │        └──▲───────────────────────────┬─────────────┘ │
│  (Go)        │           │ inbound event              │ outbound event│
│  whatsmeow   │◄──────────┤                            │               │
│  multi-conn  │  reply    │  ┌────────────────────────▼──────────────┐ │
│  claim+lock  ├───────────┼─►│  LLM Bot (Python)                    │ │
└──┬───────────┘           │  │  gRPC server + RAG + Ollama/llama3   ├─┘
   │ HTTP /connections      │  │  ChromaDB + sentence-transformers     │
   │ message_logs           │  │  Redis idempotency                    │
   ▼           ▼            └──└───────────────────────────────────────┘
┌─────────────────┐  ┌─────────────┐
│ Management API  │─►│ PostgreSQL  │
│ (FastAPI)       │  │ connections │
└────────┬────────┘  │ message_logs│   ┌──────────────────────────┐
         │ REST      │ wa sessions │   │ Observability            │
         ▼           └─────────────┘   │ stdout JSON → Promtail   │
┌─────────────────┐                    │              → Loki      │
│ React Dashboard │                    │              → Grafana   │
│ (Vite + nginx)  │                    └──────────────────────────┘
└─────────────────┘
```

**Message flow:**
1. WhatsApp sends a message → gateway receives it via whatsmeow.
2. Gateway publishes an inbound event to Kafka (`whatsapp.messages.inbound`) for analytics.
3. Gateway calls `Bot.Process(message)` via gRPC — the bot streams reply chunks back.
4. Bot performs RAG retrieval + LLM inference (Ollama), publishes outbound event to Kafka.
5. Gateway assembles the streamed reply and sends it to WhatsApp.

Kafka is the audit/analytics layer. gRPC is the real-time response path. Every log line carries
a `trace_id` (set at the gateway, propagated through gRPC metadata and Kafka headers) so a
conversation is traceable end-to-end in Grafana.

---

## Tech stack

| Layer                 | Technology                                  | Why |
|-----------------------|---------------------------------------------|-----|
| WhatsApp gateway      | **Go**, `go.mau.fi/whatsmeow`               | Cheap goroutines for many concurrent socket connections; whatsmeow is the standard multi-device library. |
| Distributed locking   | **PostgreSQL** `FOR UPDATE SKIP LOCKED`     | Race-free connection claiming with no extra infra. |
| Real-time RPC         | **gRPC** (grpc-go + grpcio)                 | Typed streaming RPC; server-side streaming delivers LLM tokens as they are generated, cutting perceived latency 2-3×. |
| Analytics streaming   | **Kafka** (confluentinc), idempotent producers | Durable ordered event log for analytics, replay, and future fan-out. |
| LLM bot               | **Python**, FastAPI, gRPC server            | Best ML ecosystem; gRPC server replaces Kafka consumer for the hot path. |
| LLM runtime           | **Ollama** + `llama3`                       | Easy Dockerized local inference, no compilation needed. |
| RAG                   | **ChromaDB** + `sentence-transformers`      | Embedded vector store; MiniLM embeddings on CPU. |
| Idempotency           | **Redis** `SET NX`                          | Cross-service dedupe on top of deterministic UUIDv5 event ids. |
| Management API        | **Python**, FastAPI, asyncpg                | Async analytics proxy + phone connection control plane. |
| Frontend              | **React + Vite + TypeScript**, recharts, qrcode.react | Typed SPA dashboard with QR code scanner and analytics charts. |
| Observability         | **Loki + Promtail + Grafana**               | Aggregates JSON stdout logs; query by service/level/trace_id. |
| Session store         | **PostgreSQL** (whatsmeow sqlstore)         | Device keys and WhatsApp sessions persist across gateway restarts. |

---

## Microservices

| Service               | Path                              | Port  | Role |
|-----------------------|-----------------------------------|-------|------|
| `whatsapp-gateway`    | `services/whatsapp-gateway`       | 8081  | Manage WhatsApp connections, distributed locking, gRPC caller, Kafka publisher. |
| `llm-bot`             | `services/llm-bot`                | 8000  | gRPC server: RAG + Ollama inference, Kafka analytics publisher. |
| `management-api`      | `services/management/api`         | 9000  | Connections control plane, analytics API, disconnect commands. |
| `management-frontend` | `services/management/frontend`    | 3000  | Dashboard: phone linking, QR modal, analytics charts. |
| Observability         | `infra/loki`, `infra/grafana`     | 3001  | Grafana on 3001, Loki on 3100. |

---

## Quick start

**Prerequisites:** Docker + Docker Compose, ~8 GB free RAM (Ollama + Llama 3).

```bash
# 1. Clone and configure
git clone <repo>
cp .env.example .env

# 2. Start the full stack (first run downloads Llama 3 — several GB, be patient)
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d

# 3. Check everything is healthy
docker compose ps
```

URLs once running:

| URL | What |
|-----|------|
| http://localhost:3000 | Management dashboard (phone linking + analytics) |
| http://localhost:9000/docs | Management API Swagger |
| http://localhost:8000/docs | LLM bot API Swagger |
| http://localhost:3001 | Grafana (anonymous viewer enabled) |

---

## Linking a WhatsApp phone

1. Open the dashboard at **http://localhost:3000**.
2. Type a label for the phone (e.g. `support-line-1`) and click **Link Phone**.
3. A QR code appears in a modal — scan it with WhatsApp:
   **Settings → Linked Devices → Link a Device**.
4. Once paired, the session is persisted in PostgreSQL. After a gateway restart the phone
   reconnects automatically — no re-scan needed.
5. The connection status updates to `connected` and appears in the connections table.

The raw QR is also rendered in the gateway container terminal via `qrterminal`:

```bash
docker compose logs -f whatsapp-gateway
```

---

## Talking to the bot

Send any WhatsApp message to the linked phone number. The bot answers only about:

- **The Elder Scrolls V: Skyrim**
- **Fallout 4** and **Fallout: New Vegas**
- **The Witcher 3: Wild Hunt**
- **Dark Souls**

Example questions:
- *"What are Dragon Shouts in Skyrim?"*
- *"How does V.A.T.S. work in Fallout 4?"*
- *"Who is Geralt of Rivia?"*

Out-of-scope questions receive a graceful "I only know about these RPGs" reply rather than
hallucinated content.

---

## How it works (key design decisions)

See **[WORKLOG.md](./WORKLOG.md)** for the full decision log.

### Race-free multi-pod connection claiming

Each gateway pod claims phone slots with:

```sql
SELECT ... FROM whatsapp_connections
WHERE status = 'disconnected'
   OR (pod_id != $me AND last_seen < now() - interval '30s')
LIMIT $batch
FOR UPDATE SKIP LOCKED;
```

`FOR UPDATE SKIP LOCKED` guarantees two pods never grab the same row. A 10 s heartbeat
refreshes `last_seen`; a 30 s staleness window lets a healthy pod take over a dead pod's
connections. On graceful shutdown a pod releases its rows immediately.

### gRPC streaming response (low-latency hot path)

The gateway holds a persistent gRPC connection to the bot. When a message arrives:

```
Gateway ──gRPC Process(msg)──► Bot
        ◄──stream ReplyChunk──
```

Server-side streaming delivers LLM tokens to the gateway as Ollama generates them, so the
first part of the reply arrives before full generation completes. Kafka carries the same event
for analytics but is not in the response critical path.

### Idempotent streaming

`event_id = UUIDv5(sender | timestamp | sha256(body))` is deterministic — the same WhatsApp
message always maps to the same id. Three dedup layers:
1. Kafka idempotent producers (`enable.idempotence=true`) — broker dedupes producer retries.
2. Redis `SET NX` in the LLM bot — skips already-answered events.
3. `UNIQUE(event_id, direction)` index in `message_logs`.

### RAG grounding (anti-hallucination)

RPG docs are chunked, embedded with MiniLM, stored in ChromaDB. Each query retrieves top-k
chunks and injects them into a strict system prompt that forbids answering outside the provided
context. Below a similarity floor the bot says "I don't know" rather than inventing content.

### Observability

All services emit JSON to stdout with a shared schema (`service`, `level`, `timestamp`,
`trace_id`, `event_id`). Promtail ships them to Loki. Filter a whole conversation:

```
{service=~".+"} | json | trace_id="<id>"
```

---

## API reference

### Management API (`localhost:9000`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/connections` | Live connection status (from gateway) merged with DB stats and bytes today. |
| `POST` | `/api/connections` | Create a new phone slot and start QR pairing. Body: `{"label": "my-phone"}`. |
| `GET` | `/api/connections/{id}/qr` | Current QR string for a pending connection (proxied from gateway). |
| `POST` | `/api/connections/{id}/disconnect` | Publish a disconnect command via Kafka. |
| `GET` | `/api/analytics/messages` | Message count per hour today, by direction. |
| `GET` | `/api/analytics/bytes` | Per-message byte sizes today. |
| `GET` | `/api/analytics/connections` | Connection uptime/age stats. |

### Gateway API (`localhost:8081`, internal)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/connections` | Live in-memory snapshot of all managed connections. |
| `POST` | `/connections` | Create connection slot + start QR immediately. Body: `{"label": "..."}`. |
| `GET` | `/connections/{id}/qr` | Current QR string for pending connection. 404 when not in QR state. |
| `GET` | `/healthz` | Health check. |

### gRPC (`llm-bot:50051`, internal)

| RPC | Type | Description |
|-----|------|-------------|
| `Bot.Process` | client → server stream | Send one `InboundMessage`, receive `stream ReplyChunk` until `done=true`. |

---

## Project layout

```
.
├── docker-compose.yml              # base stack definition
├── docker-compose.override.yml     # image overrides, port remaps, kafka-init
├── .env / .env.example             # environment variables
├── WORKLOG.md                      # full architectural decision log
├── context.md                      # project context and design intent
├── infra/
│   ├── loki/                       # Loki + Promtail config
│   └── grafana/provisioning/       # datasource + dashboard
└── services/
    ├── whatsapp-gateway/           # Go: whatsmeow, claim lock, gRPC client, Kafka
    │   ├── internal/
    │   │   ├── db/                 # PostgreSQL store (claiming, heartbeat, message logs)
    │   │   ├── kafka/              # idempotent producer, outbound consumer
    │   │   ├── proto/              # generated gRPC stubs (bot.proto)
    │   │   ├── server/             # HTTP API (connections, QR, health)
    │   │   ├── whatsapp/           # Manager, Claimer, QR handling
    │   │   └── telemetry/          # zerolog setup
    │   └── migrations/             # SQL migrations (auto-applied on startup)
    ├── llm-bot/                    # Python: gRPC server, RAG, Ollama, Kafka
    │   ├── app/
    │   │   ├── grpc/               # gRPC servicer (Bot.Process implementation)
    │   │   ├── kafka/              # analytics publisher
    │   │   ├── llm/                # Ollama client with retry + streaming
    │   │   ├── rag/                # ChromaDB retrieval
    │   │   └── proto/              # generated gRPC stubs
    │   └── data/rpg_docs/          # Skyrim, Fallout 4/NV, Witcher 3, Dark Souls
    └── management/
        ├── api/                    # Python FastAPI: analytics, connections proxy
        └── frontend/               # React + Vite + TS: dashboard, QR modal, charts
```

---

## Scaling

### Multiple gateway pods

```bash
docker compose up -d --scale whatsapp-gateway=3
```

Each pod needs a unique `POD_ID` (use the pod hostname in Kubernetes via the downward API).
The claim/heartbeat protocol distributes phone slots automatically and rebalances when a pod
dies.

### Multiple bot instances

The gateway holds a gRPC `ClientConn` to the bot address. For multiple bot replicas, point
the gateway at a load-balancer address (e.g. a Kubernetes headless service with client-side
round-robin). The gRPC round-robin policy handles this transparently.

Increase Kafka partitions to raise analytics consumer parallelism independently.

---

## Database credentials (local dev / DBeaver)

| Field    | Value       |
|----------|-------------|
| Host     | `localhost` |
| Port     | `5432`      |
| Database | `whatsapp`  |
| User     | `app`       |
| Password | `app`       |

JDBC URL: `jdbc:postgresql://localhost:5432/whatsapp`

---

## Known limitations (portfolio honesty)

- ChromaDB embedded is single-node; swap for Qdrant/pgvector for real scale.
- Ollama on CPU is slow (several seconds per reply); use GPU/vLLM in production.
- Only text messages handled; media, groups, and reactions are intentionally out of scope.
- No auth on the management UI (demo only).
- gRPC currently assumes a single bot endpoint; see Scaling section for multi-replica setup.
