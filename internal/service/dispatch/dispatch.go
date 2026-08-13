package dispatch

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"
	"webhookbroker/internal/config"
	"webhookbroker/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
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

func (s *Service) Run(ctx context.Context) {
	slog.Info("Starting dispatcher worker")

	for {
		select {
		case <-ctx.Done():
			slog.Info("Dispatcher shutting down gracefully")
			return
		default:
			// Nonblocking check + not nesting second part because of The Line of Sight Rule
		}

		processed, err := s.processTask(ctx)
		if err != nil {
			slog.Error("dispatcher processTask() error:", slog.String("error", err.Error()))
			time.Sleep(5 * time.Second)

			continue
		}

		if !processed {
			time.Sleep(2 * time.Second)
		}
	}
}

func (s *Service) processTask(ctx context.Context) (bool, error) {
	// Transaction is controlled by service
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, err
	}

	defer tx.Rollback(ctx)

	// Init repo bount to transaction
	repo := s.repoFn(tx)

	task, err := repo.FetchNextTask(ctx)
	if err != nil {
		return false, err
	}

	if task == nil {
		return false, nil
	}

	if httpErr := s.sendHTTP(ctx, task.HookURL, task.Payload); httpErr != nil {
		slog.Warn("Webhook delivery failed", slog.Int("webhook_id", task.WebhookID), slog.Int("attempt", task.CurrentRetry+1), slog.String("error", httpErr.Error()))

		if task.CurrentRetry >= s.cf.MaxRetries {
			repo.DisableWebhook(ctx, task.WebhookID)
		} else {
			nextRetry := time.Now().Add(s.calculateBackoff(task.CurrentRetry))
			repo.MarkFailure(ctx, task.WebhookID, nextRetry)
		}

		return true, tx.Commit(ctx)
	}

	slog.Info("Webhook delivered successfully", slog.Int("webhook_id", task.WebhookID))

	repo.MarkSuccess(ctx, task.WebhookID, task.OutboxDeliveryID)

	return true, tx.Commit(ctx)
}

func (s *Service) sendHTTP(ctx context.Context, url string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("API-Agent", "WebhookBroker")

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

type Manager struct {
	service *Service
}

func NewManager(svc *Service) *Manager {
	return &Manager{
		service: svc,
	}
}

func (m *Manager) Start(ctx context.Context) {
	slog.Info("Starting dispatcher pool", slog.Int("workers", m.service.cf.WorkerCount))

	var wg sync.WaitGroup
	for i := range m.service.cf.WorkerCount {
		wg.Go(func() {
			slog.Debug("Worker starting", slog.Int("worker_id", i))
			m.service.Run(ctx)
		})
	}

	wg.Wait()
	slog.Info("All dispatcher workers shut down fracefully")
}
