package api

import (
	"context"
	"fmt"
	"net/url"
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
	hook := webhook{HookURL: hookURL}
	if err := hook.Validate(); err != nil {
		return nil, err
	}

	return s.repo.CreateWebhook(ctx, hook.HookURL)
}

func (s *Service) ReceiveEvent(ctx context.Context, eventID, issuer string, payload []byte) error {
	event := event{EventID: eventID, Issuer: issuer, Data: payload}
	if err := event.Validate(); err != nil {
		return err
	}
	return s.repo.IngestEvent(ctx, eventID, issuer, payload)
}

type webhook struct {
	HookURL string
}

func (c webhook) Validate() error {
	u, err := url.ParseRequestURI(c.HookURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("api.go webhook.Validate() invalid webhook URL: %s", c.HookURL)
	}

	return nil
}

type event struct {
	EventID string
	Issuer  string
	Data    []byte
}

func (c event) Validate() error {
	if len(c.Data) > 512*1024 {
		return fmt.Errorf("api.go event.Validate() Data is too large: %s", c.EventID)
	}

	return nil
}
