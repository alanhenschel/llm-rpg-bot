// Package server exposes the gateway's HTTP API (consumed by the management service).
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/whatsapp"
)

// Server serves the gateway HTTP API.
type Server struct {
	manager *whatsapp.Manager
	podID   string
	logger  zerolog.Logger
	srv     *http.Server
}

// New builds the HTTP server.
func New(addr, podID string, manager *whatsapp.Manager, logger zerolog.Logger) *Server {
	s := &Server{manager: manager, podID: podID, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("/connections", s.handleConnections)
	mux.HandleFunc("/connections/", s.handleConnectionQR) // GET /connections/{id}/qr
	mux.HandleFunc("/healthz", s.handleHealth)
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Start runs the server (blocking) until Shutdown is called.
func (s *Server) Start() error {
	s.logger.Info().Str("addr", s.srv.Addr).Msg("http server listening")
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

type connectionsResponse struct {
	PodID       string               `json:"pod_id"`
	Connections []whatsapp.ConnState `json:"connections"`
	Count       int                  `json:"count"`
}

func (s *Server) handleConnections(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		conns := s.manager.Snapshot()
		writeJSON(w, http.StatusOK, connectionsResponse{
			PodID:       s.podID,
			Connections: conns,
			Count:       len(conns),
		})
	case http.MethodPost:
		var body struct {
			Label string `json:"label"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Label == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "label required"})
			return
		}
		id, err := s.manager.CreateAndBring(r.Context(), body.Label)
		if err != nil {
			s.logger.Error().Err(err).Msg("create connection")
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"id": id, "label": body.Label})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleConnectionQR serves GET /connections/{id}/qr
// Returns the live QR string for a pending connection so the dashboard can render it.
func (s *Server) handleConnectionQR(w http.ResponseWriter, r *http.Request) {
	// path: /connections/{id}/qr
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[2] != "qr" {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	qr := s.manager.QRCode(id)
	if qr == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no qr available"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"qr": qr})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "pod_id": s.podID})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
