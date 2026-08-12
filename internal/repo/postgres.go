package repo

import (
	"context"
	"errors"
	"fmt"
	"time"
	"webhookbroker/internal/domain"

	"github.com/jackc/pgx/v5"
)

type PostgresRepo struct {
	tx pgx.Tx
}

func NewPostgresRepo(tx pgx.Tx) *PostgresRepo {
	return &PostgresRepo{tx: tx}
}

func (r *PostgresRepo) CreateWebhook(ctx context.Context, hookURL string) (*domain.Webhook, error) {
	query := ` 
	INSERT INTO webhooks (hook_url, is_active, last_processed_outbox_id, current_retry)
	VALUES ($1, true, 0, 0)
	RETURNING id, hook_url, is_active, last_processed_outbox_id, current_retry, created_at
	`
	webhook := &domain.Webhook{}

	err := r.tx.QueryRow(ctx, query, hookURL).Scan(
		&webhook.ID,
		&webhook.HookURL,
		&webhook.IsActive,
		&webhook.LastProcessedOutboxID,
		&webhook.CurrentRetry,
		&webhook.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create webhook: %w", err)
	}

	return webhook, nil
}

func (r *PostgresRepo) IngestEvent(ctx context.Context, eventID, issuer string, payloadJSON []byte) error {
	// Save the event and get autoincremented index
	var eventIndex int64

	err := r.tx.QueryRow(ctx, `
		INSERT INTO events (event_id, issuer, data) 
		VALUES ($1, $2, $3) 
		RETURNING index
	`, eventID, issuer, string(payloadJSON)).Scan(&eventIndex)
	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	// Bulk insert into outbox_deliveries for all active webhooks
	_, err = r.tx.Exec(ctx, `
		INSERT INTO outbox_deliveries (event_index, webhook_id)
		SELECT $1, id 
		FROM webhooks 
		WHERE is_active = true
	`, eventIndex)
	if err != nil {
		return fmt.Errorf("failed to populate outbox deliveries: %w", err)
	}

	return nil
}

// 1. Look for active webhook, where next_retry_time is NULL or <= current time
// 2. Joing with outbox on webhook id with bigger inremental task id
// 3. Joing with events to get payload
// 4. Lock the webhook row with FOR UPDATE OF w + SKIP LOCKED for concurrent workers
func (r *PostgresRepo) FetchNextTask(ctx context.Context) (*domain.DeliveryTask, error) {
	query := `
		SELECT 
			w.id, 
			w.hook_url,
			w.current_retry, 
			o.id, 
			e.data
		FROM webhooks w
		JOIN LATERAL (
			SELECT id, event_index
			FROM outbox_deliveries
			WHERE webhook_id = w.id 
			  AND id > w.last_processed_outbox_id
			ORDER BY id ASC
			LIMIT 1
		) o ON true
		JOIN events e ON e.index = o.event_index
		WHERE w.is_active = true
		  AND (w.next_retry_time IS NULL OR w.next_retry_time <= NOW())
		LIMIT 1
		FOR UPDATE OF w SKIP LOCKED;
	`

	task := &domain.DeliveryTask{}

	err := r.tx.QueryRow(ctx, query).Scan(&task.WebhookID, &task.HookURL, &task.CurrentRetry, &task.OutboxDeliveryID, &task.Payload)
	// Handle the empty query
	if err != nil {
		// Track healthy error: ErrNoRows
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to fetch next task: %w", err)
	}

	return task, nil
}

func (r *PostgresRepo) DisableWebhook(ctx context.Context, webhookID int) error {
	_, err := r.tx.Exec(ctx, `
		UPDATE webhooks 
		SET is_active = false 
		WHERE id = $1
	`, webhookID)
	if err != nil {
		return fmt.Errorf("failed to disable webhook: %w", err)
	}

	return nil
}

func (r *PostgresRepo) MarkFailure(ctx context.Context, webhookID int, nextRetry time.Time) error {
	_, err := r.tx.Exec(ctx, `
		UPDATE webhooks 
		SET current_retry = current_retry + 1, 
		    next_retry_time = $1
		WHERE id = $2
	`, nextRetry, webhookID)
	if err != nil {
		return fmt.Errorf("failed to mark webhook failure: %w", err)
	}

	return nil
}

func (r *PostgresRepo) MarkSuccess(ctx context.Context, webhookID int, outboxID int64) error {
	_, err := r.tx.Exec(ctx, `
		UPDATE webhooks 
		SET current_retry = 0, 
		    next_retry_time = NULL, 
		    last_processed_outbox_id = $1
		WHERE id = $2
	`, outboxID, webhookID)
	if err != nil {
		return fmt.Errorf("failed to update webhook success state: %w", err)
	}

	return nil
}
