package api

import (
	"context"
	"log/slog"
	"webhookbroker/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	CreateWebhook(ctx context.Context, hookURL string, filterConfig domain.FilterConfig) (*domain.Webhook, error)
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

func (s *Service) RegisterWebhook(ctx context.Context, hookURL string, dbFilters domain.FilterConfig) (*domain.Webhook, error) {
	hook := webhook{
		HookURL: hookURL,
		Filters: dbFilters,
	}

	if err := hook.Validate(); err != nil {
		return nil, err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	repo := s.repoFn(tx)

	webhook, err := repo.CreateWebhook(ctx, hook.HookURL, hook.Filters)
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
