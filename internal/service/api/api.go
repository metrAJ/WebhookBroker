package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"webhookbroker/domain"

	"github.com/google/uuid"
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

// Map domain DTO to DB model
func mapToDBConfig(params domain.FilterParams) domain.FilterConfig {
	return domain.FilterConfig{
		Divisor: params.DivisibleBy,
		Issuer:  params.Issuer,
	}
}

func (s *Service) RegisterWebhook(ctx context.Context, hookURL string, params domain.FilterParams) (*domain.Webhook, error) {
	dbFilters := mapToDBConfig(params)
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

type webhook struct {
	HookURL string
	Filters domain.FilterConfig
}

func (c webhook) Validate() error {
	u, err := url.ParseRequestURI(c.HookURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("api.go webhook.Validate(): invalid webhook URL: %s", c.HookURL)
	}

	if c.Filters.Divisor != nil && *c.Filters.Divisor == 0 {
		return fmt.Errorf("api.go webhook.Validate(): invalid filter (divisibleByN cannot be zero)")
	}

	return nil
}

type event struct {
	EventID string
	Issuer  string
	Data    []byte
}

func (c event) Validate() error {
	if _, err := uuid.Parse(c.EventID); err != nil {
		return fmt.Errorf("invalid event id: must be a valid UUIDv4")
	}

	if len(c.Data) > 512*1024 {
		return fmt.Errorf("api.go event.Validate() Data is too large: %s", c.EventID)
	}

	return nil
}
