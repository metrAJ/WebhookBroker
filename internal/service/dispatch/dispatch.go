package dispatch

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"
	"webhookbroker/internal/config"
	"webhookbroker/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const agent = "WebhookBroker"

type Repository interface {
	ClaimNextTasks(ctx context.Context, limit, leaseSec int) ([]domain.DeliveryTask, error)
	FetchNextTask(ctx context.Context) (*domain.DeliveryTask, error)
	MarkSuccess(ctx context.Context, webhookID int, outboxID int64) error
	MarkFailure(ctx context.Context, webhookID int, nextRetry time.Time) error
	DisableWebhook(ctx context.Context, webhookID int) error
}

type Service struct {
	cf         config.DispatcherConfig
	db         *pgxpool.Pool
	repoFn     func(tx pgx.Tx) Repository
	httpClient *http.Client
}

func NewService(cf config.DispatcherConfig, db *pgxpool.Pool, repoFn func(tx pgx.Tx) Repository) *Service {
	return &Service{
		cf:         cf,
		db:         db,
		repoFn:     repoFn,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

func (s *Service) processTask(ctx context.Context, task domain.DeliveryTask) {
	err := s.sendHTTP(ctx, task.HookURL, task.Payload)

	dbCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	tx, txErr := s.db.Begin(dbCtx)
	if txErr != nil {
		slog.Error("Failed to begin transaction for state update", slog.String("error", txErr.Error()))
		return
	}
	defer tx.Rollback(dbCtx)

	repo := s.repoFn(tx)

	if err != nil {
		slog.Warn("Webhook delivery failed", slog.Int("webhook_id", task.WebhookID), slog.Int("attempt", task.CurrentRetry), slog.String("error", err.Error()))

		if task.CurrentRetry > s.cf.MaxRetries {
			if dbErr := repo.DisableWebhook(dbCtx, task.WebhookID); dbErr != nil {
				slog.Error("Failed to disable webhook", slog.String("error", dbErr.Error()))
				return
			}
		} else {
			nextRetry := time.Now().Add(s.calculateBackoff(task.CurrentRetry))
			if dbErr := repo.MarkFailure(dbCtx, task.WebhookID, nextRetry); dbErr != nil {
				slog.Error("Failed to mark failure", slog.String("error", dbErr.Error()))
				return
			}
		}
	} else {
		if dbErr := repo.MarkSuccess(dbCtx, task.WebhookID, task.OutboxDeliveryID); dbErr != nil {
			slog.Error("Failed to mark success", slog.String("error", dbErr.Error()))
			return
		}
	}

	if commitErr := tx.Commit(dbCtx); commitErr != nil {
		slog.Error("Failed to commit state update", slog.String("error", commitErr.Error()))
	}
}

func (s *Service) sendHTTP(ctx context.Context, url string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("API-Agent", agent)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP request failed with status code: %d", resp.StatusCode)
	}

	return nil
}

func (s *Service) calculateBackoff(currentRetry int) time.Duration {
	multiplier := math.Pow(2, float64(currentRetry))
	return time.Duration(multiplier) * s.cf.BaseSecWait
}

func (s *Service) claimTasks(ctx context.Context, limit int) ([]domain.DeliveryTask, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	repo := s.repoFn(tx)

	tasks, err := repo.ClaimNextTasks(ctx, limit, s.cf.TaskLeaseSec)
	if err != nil {
		return nil, err
	}

	return tasks, tx.Commit(ctx)
}
