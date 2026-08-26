package transport

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"webhookbroker/domain"
)

type Service interface {
	ReceiveEvent(ctx context.Context, eventID, issuer string, payload []byte) error
	RegisterWebhook(ctx context.Context, hookURL string, filters domain.FilterParams) (*domain.Webhook, error)
}

type HTTPHandler struct {
	service Service
}

func NewHTTPHandler(s Service) *HTTPHandler {
	return &HTTPHandler{service: s}
}

type webhookRequest struct {
	HookURL string              `json:"hook_url"`
	Filters domain.FilterParams `json:"filters,omitempty"`
}

type eventRequest struct {
	EventID string          `json:"id"`
	Issuer  string          `json:"issuer"`
	Data    json.RawMessage `json:"data"`
}

func (h *HTTPHandler) RegisterWebhook(w http.ResponseWriter, r *http.Request) {
	var req webhookRequest

	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("Failed to decode webhook", "error", err)
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)

		return
	}
	defer r.Body.Close()

	if req.HookURL == "" {
		http.Error(w, "hook_url is required", http.StatusBadRequest)
		return
	}

	webhook, err := h.service.RegisterWebhook(r.Context(), req.HookURL, req.Filters)
	if err != nil {
		slog.Error("Failed to register webhook", "error", err)
		http.Error(w, "Failed to register webhook", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(webhook)
}

func (h *HTTPHandler) ReceiveEvent(w http.ResponseWriter, r *http.Request) {
	var req eventRequest

	r.Body = http.MaxBytesReader(w, r.Body, 520*1024)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("Failed to decode event", "error", err)
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)

		return
	}
	defer r.Body.Close()

	if req.EventID == "" || req.Issuer == "" {
		http.Error(w, "id, issuer, data are required", http.StatusBadRequest)
		return
	}

	err := h.service.ReceiveEvent(r.Context(), req.EventID, req.Issuer, req.Data)
	if err != nil {
		slog.Error("Failed to ingest event", "error", err)
		http.Error(w, "Failed to ingest event", http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "accepted"}`))
}
