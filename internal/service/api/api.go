package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"webhookbroker/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	CreateWebhook(ctx context.Context, hookURL string) (*domain.Webhook, error)
	IngestEvent(ctx context.Context, eventID, issuer string, payloadJSON []byte) error
}

type Service struct {
	db     *pgxpool.Pool
	repoFn func(tx pgx.Tx) Repository
}

func NewService(db *pgxpool.Pool, repoFn func(tx pgx.Tx) Repository) *Service {
	return &Service{
		db:     db,
		repoFn: repoFn,
	}
}

func (s *Service) RegisterWebhook(ctx context.Context, hookURL string) (*domain.Webhook, error) {
	hook := webhook{HookURL: hookURL}
	if err := hook.Validate(); err != nil {
		return nil, err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	repo := s.repoFn(tx)

	webhook, err := repo.CreateWebhook(ctx, hook.HookURL)
	if err != nil {
		return nil, err
	}

	slog.Info("Successfully registered new webhook", slog.Int("webhook_id", webhook.ID))

	return webhook, tx.Commit(ctx)
}

func (s *Service) ReceiveEvent(ctx context.Context, eventID, issuer string, payload []byte) error {
	event := event{EventID: eventID, Issuer: issuer, Data: payload}
	if err := event.Validate(); err != nil {
		return err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	repo := s.repoFn(tx)

	if err := repo.IngestEvent(ctx, eventID, issuer, payload); err != nil {
		return err
	}

	return tx.Commit(ctx)
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
