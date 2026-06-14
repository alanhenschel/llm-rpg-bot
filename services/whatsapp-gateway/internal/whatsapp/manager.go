// Package whatsapp manages multiple whatsmeow client connections in one process.
//
// Responsibilities:
//   - Claim connection slots from PostgreSQL (delegated to db.Store).
//   - Bring up a whatsmeow client per claimed slot, restoring its session if present.
//   - Emit QR codes (to logs + Kafka events topic) when a slot has no session.
//   - Forward received messages to Kafka (inbound topic) with deterministic event ids.
//   - Send messages on command (driven by the outbound consumer).
//   - Track per-connection status and expose it for the HTTP /connections endpoint.
package whatsapp

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mdp/qrterminal/v3"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/db"
	xgrpc "github.com/alan/ia-pipeline/whatsapp-gateway/internal/grpc"
	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/grpc/botpb"
	xkafka "github.com/alan/ia-pipeline/whatsapp-gateway/internal/kafka"
	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/telemetry"
)

// ConnState is the live, in-memory view of one managed connection.
type ConnState struct {
	ID        int64     `json:"id"`
	JID       string    `json:"jid"`
	Label     string    `json:"label"`
	Status    string    `json:"status"` // connecting | qr | connected | disconnected
	LastSeen  time.Time `json:"last_seen"`
	BytesIn   int64     `json:"bytes_in"`
	BytesOut  int64     `json:"bytes_out"`
	QRCode    string    `json:"qr_code,omitempty"` // current QR string, only while status=qr
	client    *whatsmeow.Client
}

// Manager owns all whatsmeow clients for this pod.
type Manager struct {
	// ctx is the process-level context (lives until SIGTERM). Used for all
	// background goroutines (QR pairing, DB writes after HTTP requests complete).
	// Never use an HTTP request context for long-running operations here.
	ctx       context.Context
	cfg       Config
	store     *db.Store
	producer  *xkafka.Producer
	botClient *xgrpc.BotClient // nil when BOT_GRPC_ADDR is not set
	container *sqlstore.Container
	logger    zerolog.Logger

	mu    sync.RWMutex
	conns map[int64]*ConnState
}

// Config holds manager dependencies/tunables.
type Config struct {
	PodID         string
	TopicInbound  string
	TopicEvents   string
	DatabaseURL   string
}

// New builds a Manager. The sqlstore container is the whatsmeow device store backed
// by the same Postgres instance (separate tables, prefixed whatsmeow_).
func New(ctx context.Context, cfg Config, st *db.Store, producer *xkafka.Producer, botClient *xgrpc.BotClient, logger zerolog.Logger) (*Manager, error) {
	dbLog := waLog.Stdout("whatsmeow-store", "WARN", true)
	container, err := sqlstore.New(ctx, "pgx", cfg.DatabaseURL, dbLog)
	if err != nil {
		return nil, fmt.Errorf("whatsmeow sqlstore: %w", err)
	}
	return &Manager{
		ctx:       ctx,
		cfg:       cfg,
		store:     st,
		producer:  producer,
		botClient: botClient,
		container: container,
		logger:    logger,
		conns:     make(map[int64]*ConnState),
	}, nil
}

// Snapshot returns a copy of current connection states for the HTTP endpoint.
func (m *Manager) Snapshot() []ConnState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ConnState, 0, len(m.conns))
	for _, c := range m.conns {
		cp := *c
		cp.client = nil // don't leak the client
		out = append(out, cp)
	}
	return out
}

// Owns reports whether this pod currently manages the given connection id.
func (m *Manager) Owns(id int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.conns[id]
	return ok
}

// Bring brings up a claimed connection: restore session or start QR pairing.
func (m *Manager) Bring(ctx context.Context, conn db.Connection) error {
	m.mu.Lock()
	if _, exists := m.conns[conn.ID]; exists {
		m.mu.Unlock()
		return nil // already managed
	}
	state := &ConnState{ID: conn.ID, JID: conn.JID, Label: conn.Label, Status: "connecting", LastSeen: time.Now()}
	m.conns[conn.ID] = state
	m.mu.Unlock()

	// Resolve a whatsmeow device store: reuse existing device for this JID if we
	// have one, else create a fresh device that will go through QR pairing.
	var device *store.Device
	var err error
	if conn.JID != "" {
		jid, perr := types.ParseJID(conn.JID)
		if perr == nil {
			device, err = m.container.GetDevice(ctx, jid)
			if err != nil {
				m.logger.Warn().Err(err).Int64("conn", conn.ID).Msg("get device; will create new")
			}
		}
	}
	if device == nil {
		device = m.container.NewDevice()
	}

	client := whatsmeow.NewClient(device, waLog.Stdout("whatsmeow", "WARN", true))
	state.client = client

	client.AddEventHandler(m.eventHandler(ctx, conn.ID))

	if client.Store.ID == nil {
		// No session → need QR pairing.
		return m.startQR(ctx, conn.ID, client)
	}
	// Existing session → reconnect silently.
	if err := client.Connect(); err != nil {
		m.setStatus(conn.ID, "disconnected")
		return fmt.Errorf("reconnect conn %d: %w", conn.ID, err)
	}
	m.setStatus(conn.ID, "connected")
	_ = m.store.SetStatus(ctx, conn.ID, "connected", client.Store.ID.String())
	m.logger.Info().Int64("conn", conn.ID).Str("jid", client.Store.ID.String()).Msg("reconnected existing session")
	return nil
}

// startQR drives the QR pairing flow, emitting QR strings to logs + events topic.
// It always uses m.ctx (process lifetime) — never a per-request context — so that
// QR events and DB writes keep working after the originating HTTP request is done.
func (m *Manager) startQR(_ context.Context, connID int64, client *whatsmeow.Client) error {
	qrChan, err := client.GetQRChannel(m.ctx)
	if err != nil {
		return fmt.Errorf("qr channel conn %d: %w", connID, err)
	}
	if err := client.Connect(); err != nil {
		return fmt.Errorf("connect for qr conn %d: %w", connID, err)
	}
	m.setStatus(connID, "qr")

	go func() {
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				m.logger.Info().Int64("conn", connID).Str("qr", evt.Code).
					Msg("scan this QR code with WhatsApp (Linked Devices)")
				m.setQRCode(connID, evt.Code)
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				m.emitEvent(m.ctx, connID, "qr", evt.Code, "")
			case "success":
				m.logger.Info().Int64("conn", connID).Msg("QR pairing success")
				m.setQRCode(connID, "")
			case "timeout":
				m.logger.Warn().Int64("conn", connID).Msg("QR pairing timed out")
				m.setQRCode(connID, "")
				// Mark disconnected in DB first so the claimer can reclaim.
				_ = m.store.SetStatus(m.ctx, connID, "disconnected", "")
				// Evict from the in-memory map so Owns() returns false.
				// The claimer will find the disconnected row and call Bring
				// again, starting a fresh QR cycle automatically.
				m.evictConn(connID)
			default:
				m.logger.Debug().Int64("conn", connID).Str("event", evt.Event).Msg("qr event")
			}
		}
	}()
	return nil
}

// eventHandler returns the whatsmeow event callback for a connection.
// All DB writes and Kafka publishes use m.ctx (process lifetime) because
// whatsmeow fires these callbacks asynchronously, long after any originating
// HTTP request context is gone.
func (m *Manager) eventHandler(_ context.Context, connID int64) func(any) {
	return func(rawEvt any) {
		switch e := rawEvt.(type) {
		case *events.Message:
			m.onMessage(m.ctx, connID, e)
		case *events.Connected:
			m.setStatus(connID, "connected")
			if c := m.client(connID); c != nil && c.Store.ID != nil {
				jid := c.Store.ID.String()
				_ = m.store.SetStatus(m.ctx, connID, "connected", jid)
				m.persistSession(m.ctx, connID)
				m.emitEvent(m.ctx, connID, "connected", jid, "")
			}
		case *events.Disconnected:
			m.setStatus(connID, "disconnected")
			m.emitEvent(m.ctx, connID, "disconnected", "", "")
		case *events.LoggedOut:
			m.logger.Warn().Int64("conn", connID).Msg("logged out by phone")
			m.setStatus(connID, "disconnected")
			_ = m.store.SetStatus(m.ctx, connID, "disconnected", "")
			m.emitEvent(m.ctx, connID, "logged_out", "", "")
		case *events.PairSuccess:
			m.persistSession(m.ctx, connID)
		}
	}
}

// onMessage handles an inbound text message:
//  1. Logs to DB and publishes to Kafka inbound topic (analytics).
//  2. Calls the LLM bot via gRPC (hot path) and sends the reply over WhatsApp.
func (m *Manager) onMessage(ctx context.Context, connID int64, e *events.Message) {
	body := extractText(e)
	if body == "" {
		return // ignore non-text (media, reactions, etc.) — out of scope
	}
	tsMs := e.Info.Timestamp.UnixMilli()
	sender := e.Info.Sender.String()
	chatJID := e.Info.Chat.String()
	eventID := xkafka.DeterministicEventID(sender, tsMs, body)
	traceID := telemetry.NewTraceID()
	bytes := len(body)

	m.addBytesIn(connID, int64(bytes))

	logEntry := m.logger.With().
		Str("trace_id", traceID).Str("event_id", eventID).
		Int64("conn", connID).Str("jid", chatJID).Logger()

	if err := m.store.LogMessage(ctx, eventID, connID, chatJID, "inbound", body, bytes, traceID); err != nil {
		logEntry.Error().Err(err).Msg("log inbound message")
	}

	// Publish to Kafka for analytics (non-blocking for the response path).
	kafkaMsg := xkafka.InboundMessage{
		EventID:      eventID,
		TraceID:      traceID,
		PodID:        m.cfg.PodID,
		ConnectionID: connID,
		JID:          chatJID,
		SenderJID:    sender,
		Body:         body,
		Bytes:        bytes,
		Timestamp:    tsMs,
	}
	if err := m.producer.PublishInbound(ctx, m.cfg.TopicInbound, kafkaMsg); err != nil {
		logEntry.Error().Err(err).Msg("publish inbound to kafka (analytics)")
	} else {
		logEntry.Info().Int("bytes", bytes).Msg("inbound message published to kafka")
	}

	// gRPC hot path: call the bot directly and send the reply.
	if m.botClient == nil {
		logEntry.Warn().Msg("no bot gRPC client configured; reply skipped")
		return
	}
	reply, err := m.botClient.Process(ctx, &botpb.InboundMessage{
		EventId:      eventID,
		TraceId:      traceID,
		ConnectionId: connID,
		Jid:          chatJID,
		SenderJid:    sender,
		Body:         body,
	})
	if err != nil {
		logEntry.Error().Err(err).Msg("bot.Process gRPC failed")
		return
	}
	if reply == "" {
		logEntry.Warn().Msg("bot returned empty reply; not sending")
		return
	}
	if err := m.SendText(ctx, connID, chatJID, reply, traceID, eventID); err != nil {
		logEntry.Error().Err(err).Msg("send WhatsApp reply")
	}
}

// SendText sends a text message via the connection identified by connID.
func (m *Manager) SendText(ctx context.Context, connID int64, toJID, body, traceID, eventID string) error {
	client := m.client(connID)
	if client == nil {
		return fmt.Errorf("connection %d not managed by this pod", connID)
	}
	jid, err := types.ParseJID(toJID)
	if err != nil {
		return fmt.Errorf("parse jid %q: %w", toJID, err)
	}
	_, err = client.SendMessage(ctx, jid, &waProto.Message{
		Conversation: proto.String(body),
	})
	if err != nil {
		return fmt.Errorf("send message: %w", err)
	}
	bytes := len(body)
	m.addBytesOut(connID, int64(bytes))
	_ = m.store.LogMessage(ctx, eventID, connID, toJID, "outbound", body, bytes, traceID)
	m.logger.Info().Str("trace_id", traceID).Str("event_id", eventID).
		Int64("conn", connID).Str("jid", toJID).Int("bytes", bytes).Msg("outbound message sent")
	return nil
}

// Disconnect tears down a connection (operator-driven via Kafka command).
func (m *Manager) Disconnect(ctx context.Context, connID int64) error {
	client := m.client(connID)
	if client == nil {
		return fmt.Errorf("connection %d not managed by this pod", connID)
	}
	client.Disconnect()
	m.setStatus(connID, "disconnected")
	if err := m.store.SetStatus(ctx, connID, "disconnected", ""); err != nil {
		return err
	}
	m.emitEvent(ctx, connID, "disconnected", "operator", "")
	return nil
}

// CreateAndBring creates a new connection slot in the DB and immediately begins
// QR pairing on this pod. This is the on-demand path triggered by the management UI
// (POST /connections). The slot starts as 'disconnected' and is immediately claimed
// by this pod before Bring is called so the background claimer doesn't race it.
func (m *Manager) CreateAndBring(ctx context.Context, label string) (int64, error) {
	conn, err := m.store.CreateConnection(ctx, label)
	if err != nil {
		return 0, fmt.Errorf("create connection: %w", err)
	}
	// Mark as claimed by this pod so the claim loop doesn't also grab it.
	if err := m.store.SetStatus(ctx, conn.ID, "claimed", ""); err != nil {
		return 0, fmt.Errorf("claim new connection %d: %w", conn.ID, err)
	}
	conn.Status = "claimed"
	conn.PodID = m.cfg.PodID
	if err := m.Bring(ctx, *conn); err != nil {
		return 0, err
	}
	return conn.ID, nil
}

// Shutdown gracefully disconnects all managed clients (sessions stay in DB).
func (m *Manager) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, c := range m.conns {
		if c.client != nil {
			c.client.Disconnect()
		}
		m.logger.Info().Int64("conn", id).Msg("disconnected on shutdown")
	}
}

// --- internal helpers ---

func (m *Manager) persistSession(ctx context.Context, connID int64) {
	client := m.client(connID)
	if client == nil || client.Store.ID == nil {
		return
	}
	jid := client.Store.ID.String()
	_ = m.store.SetStatus(ctx, connID, "connected", jid)
	// Write the JID as a presence marker into session_data so the column reflects
	// that a live whatsmeow session exists. The full session lives in whatsmeow's
	// own sqlstore tables; this is a lightweight signal for operators and tooling.
	_ = m.store.SaveSession(ctx, connID, []byte(jid))
}

func (m *Manager) client(connID int64) *whatsmeow.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if c, ok := m.conns[connID]; ok {
		return c.client
	}
	return nil
}

// evictConn removes a connection from the in-memory map and closes its client.
// Called after QR timeout so the claimer can reclaim the slot and restart QR.
func (m *Manager) evictConn(connID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.conns[connID]; ok {
		if c.client != nil {
			c.client.Disconnect()
		}
		delete(m.conns, connID)
	}
}

func (m *Manager) setStatus(connID int64, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.conns[connID]; ok {
		c.Status = status
		c.LastSeen = time.Now()
	}
}

func (m *Manager) setQRCode(connID int64, code string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.conns[connID]; ok {
		c.QRCode = code
	}
}

// QRCode returns the current QR string for a pending connection, or "" if not in QR state.
func (m *Manager) QRCode(connID int64) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if c, ok := m.conns[connID]; ok {
		return c.QRCode
	}
	return ""
}

func (m *Manager) addBytesIn(connID int64, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.conns[connID]; ok {
		c.BytesIn += n
	}
}

func (m *Manager) addBytesOut(connID int64, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.conns[connID]; ok {
		c.BytesOut += n
	}
}

func (m *Manager) emitEvent(ctx context.Context, connID int64, typ, payload, traceID string) {
	jid := ""
	if c := m.clientState(connID); c != nil {
		jid = c.JID
	}
	now := time.Now().UnixMilli()
	if traceID == "" {
		traceID = telemetry.NewTraceID()
	}
	evt := xkafka.ConnectionEvent{
		EventID:      xkafka.DeterministicEventID(fmt.Sprintf("conn:%d:%s", connID, typ), now, payload),
		TraceID:      traceID,
		PodID:        m.cfg.PodID,
		ConnectionID: connID,
		JID:          jid,
		Type:         typ,
		Payload:      payload,
		Timestamp:    now,
	}
	if err := m.producer.PublishEvent(ctx, m.cfg.TopicEvents, evt); err != nil {
		m.logger.Error().Err(err).Int64("conn", connID).Str("type", typ).Msg("publish event")
	}
}

func (m *Manager) clientState(connID int64) *ConnState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[connID]
}

// extractText pulls plain text out of a whatsmeow message envelope.
func extractText(e *events.Message) string {
	if e.Message == nil {
		return ""
	}
	if c := e.Message.GetConversation(); c != "" {
		return c
	}
	if ext := e.Message.GetExtendedTextMessage(); ext != nil {
		return ext.GetText()
	}
	return ""
}
