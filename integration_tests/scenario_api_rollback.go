package integration_tests

import (
	"context"
	"fmt"
	"time"
	"webhookbroker/domain"
	"webhookbroker/internal/repo"
	"webhookbroker/internal/service/api"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type faultInjectingRepo struct {
	realRepo api.Repository
}

func (f *faultInjectingRepo) CreateWebhook(ctx context.Context, hookURL string, _ domain.FilterConfig) (*domain.Webhook, error) {
	return f.realRepo.CreateWebhook(ctx, hookURL, domain.FilterConfig{})
}

func (f *faultInjectingRepo) IngestEvent(ctx context.Context, eventID, issuer string, payload []byte) error {
	if err := f.realRepo.IngestEvent(ctx, eventID, issuer, payload); err != nil {
		return err
	}

	return fmt.Errorf("simulated catastrophic DB failure during ingestion")
}

func RunScenarioAPIRollback(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	repoFactory := func(tx pgx.Tx) api.Repository {
		return &faultInjectingRepo{
			realRepo: repo.NewPostgresRepo(tx),
		}
	}
	svc := api.NewService(pool, repoFactory)

	eventID := uuid.New().String()
	if err := svc.ReceiveEvent(ctx, eventID, "test", []byte(`{"data":"fail"}`)); err == nil {
		return fmt.Errorf("expected ReceiveEvent to fail due to simulated crash, but it succeeded")
	}

	var count int

	err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1`, eventID).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to query events table: %w", err)
	}

	if count > 0 {
		return fmt.Errorf("transaction rollback failed! Found %d orphaned events in the database", count)
	}

	return nil
}
