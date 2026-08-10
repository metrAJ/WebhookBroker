package api

import (
	"context"
	"webhookbroker/internal/domain"
)

type Repository interface {
	CreateWebhook(ctx context.Context, hookURL string) (*domain.Webhook, error)
	IngestEvent(ctx context.Context, eventID, issuer string, payloadJSON []byte) error
}

type Service struct {
	repo Repository
}

func NewService(r Repository) *Service {
	return &Service{repo: r}
}

func (s *Service) RegisterWebhook(ctx context.Context, hookURL string) (*domain.Webhook, error) {
	return s.repo.CreateWebhook(ctx, hookURL)
}

func (s *Service) ReceiveEvent(ctx context.Context, eventID, issuer string, payload []byte) error {
	return s.repo.IngestEvent(ctx, eventID, issuer, payload)
}