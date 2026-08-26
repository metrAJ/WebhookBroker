package filter

import (
	"context"
	"webhookbroker/domain"
	"webhookbroker/pkg/filters"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	FetchUnprocessedEvents(ctx context.Context, limit int) ([]domain.Event, error)
	GetActiveWebhooks(ctx context.Context) ([]domain.Webhook, error)
	DispatchToOutbox(ctx context.Context, matches []domain.OutboxDelivery, highestIndex int64) error
}

type Service struct {
	db     *pgxpool.Pool
	repoFn func(tx pgx.Tx) Repository
}

func NewService(db *pgxpool.Pool, repoFn func(tx pgx.Tx) Repository) *Service {
	return &Service{db: db, repoFn: repoFn}
}

// To Not Fetch and reassemble whole filter chains for every event
type compiledWebhook struct {
	hook  domain.Webhook
	chain filters.FilterChain
}

func (s *Service) ProcessBatch(ctx context.Context, batchSize int) (int, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	repo := s.repoFn(tx)

	events, err := repo.FetchUnprocessedEvents(ctx, batchSize)
	if err != nil || len(events) == 0 {
		return 0, err
	}

	webhooks, err := repo.GetActiveWebhooks(ctx)
	if err != nil {
		return 0, err
	}

	compiledHooks := make([]compiledWebhook, 0, len(webhooks))
	for _, w := range webhooks {
		compiledHooks = append(compiledHooks, compiledWebhook{
			hook:  w,
			chain: filters.BuildChain(w.Filters),
		})
	}

	matches := make([]domain.OutboxDelivery, 0, len(events))
	var highestIndex int64

	for _, event := range events {
		highestIndex = event.Index

		for _, compiled := range compiledHooks {
			// Prevent sending old events to new hooks
			if event.CreatedAt.Before(compiled.hook.CreatedAt) {
				continue
			}

			if compiled.chain.MatchesAll(event) {
				matches = append(matches, domain.OutboxDelivery{
					EventIndex: event.Index,
					WebhookID:  compiled.hook.ID,
				})
			}
		}
	}

	if err := repo.DispatchToOutbox(ctx, matches, highestIndex); err != nil {
		return 0, err
	}

	return len(events), tx.Commit(ctx)
}
