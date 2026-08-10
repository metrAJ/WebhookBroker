package repo

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type Acknowledger struct {
	tx               pgx.Tx
	webhookID        int
	outboxDeliveryID int64
}

func (a *Acknowledger) Ack(ctx context.Context) error {
	defer a.tx.Rollback(ctx)
	_, err := a.tx.Exec(ctx, `
		UPDATE webhooks 
		SET last_processed_outbox_id = $1, current_retry = 0, next_retry_time = NULL 
		WHERE id = $2
	`, a.outboxDeliveryID, a.webhookID)
	if err != nil {
		return err
	}
	return a.tx.Commit(ctx)
}

func (a *Acknowledger) Nack(ctx context.Context, nextRetryTime time.Time) error {
	defer a.tx.Rollback(ctx)
	_, err := a.tx.Exec(ctx, `
		UPDATE webhooks SET current_retry = current_retry + 1, next_retry_time = $1 WHERE id = $2
	`, nextRetryTime, a.webhookID)
	if err != nil {
		return err
	}
	return a.tx.Commit(ctx)
}

func (a *Acknowledger) Fatal(ctx context.Context) error {
	defer a.tx.Rollback(ctx)
	_, err := a.tx.Exec(ctx,
		`UPDATE webhooks SET is_active = false WHERE id = $1
	`, a.webhookID)
	if err != nil {
		return err
	}
	return a.tx.Commit(ctx)
}
