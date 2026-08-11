package dispatch

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"
	"webhookbroker/internal/domain"
)

const baseSecWait = 10

type Acknowledger interface {
	Ack(ctx context.Context) error
	Nack(ctx context.Context, nextRetryTime time.Time) error
	Fatal(ctx context.Context) error
}

type Repository interface {
	FetchNextTask(ctx context.Context) (*domain.DeliveryTask, Acknowledger, error)
}

type Service struct {
	repo       Repository
	httpClient *http.Client
}

const MaxRetries = 9

func NewService(r Repository) *Service {
	return &Service{
		repo: r,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
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
	task, ack, err := s.repo.FetchNextTask(ctx)
	if err != nil {
		return false, err
	}

	if task == nil {
		return false, nil
	}

	if httpErr := s.sendHTTP(ctx, task.HookURL, task.Payload); httpErr != nil {
		slog.Warn("Webhook delivery failed", slog.Int("webhook_id", task.WebhookID), slog.Int("attempt", task.CurrentRetry+1), slog.String("error", httpErr.Error()))

		if task.CurrentRetry >= MaxRetries {
			slog.Error("Max retries reached. Disabling webhook.", slog.Int("webhook_id", task.WebhookID))
			ack.Fatal(ctx)

			return true, nil
		}

		nextRetryTime := time.Now().Add(s.calculateBackoff(task.CurrentRetry))
		ack.Nack(ctx, nextRetryTime)

		return true, nil
	}

	slog.Info("Webhook delivered successfully", slog.Int("webhook_id", task.WebhookID))
	ack.Ack(ctx)

	return true, nil
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
	return time.Duration(multiplier) * baseSecWait * time.Second
}
