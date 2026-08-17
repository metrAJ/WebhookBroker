package integration_tests

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedWebhook(ctx context.Context, pool *pgxpool.Pool, hookURL string) (int, error) {
	var webhookID int

	err := pool.QueryRow(ctx, `
		INSERT INTO webhooks (hook_url, is_active, last_processed_outbox_id, current_retry) 
		VALUES ($1, true, 0, 0) RETURNING id
	`, hookURL).Scan(&webhookID)

	if err != nil {
		return 0, fmt.Errorf("factory failed to seed webhook: %w", err)
	}

	return webhookID, nil
}

func seedEvent(ctx context.Context, pool *pgxpool.Pool, webhookID int, payload string) error {
	eventID := uuid.New().String()

	var eventIndex int64

	err := pool.QueryRow(ctx, `
		INSERT INTO events (event_id, issuer, data) 
		VALUES ($1, 'integration_test', $2) RETURNING index
	`, eventID, payload).Scan(&eventIndex)

	if err != nil {
		return fmt.Errorf("factory failed to seed event: %w", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO outbox_deliveries (event_index, webhook_id) 
		VALUES ($1, $2)
	`, eventIndex, webhookID)

	if err != nil {
		return fmt.Errorf("factory failed to seed outbox delivery: %w", err)
	}

	return nil
}
