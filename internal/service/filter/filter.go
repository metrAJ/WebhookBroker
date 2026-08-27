package filter

import (
	"context"
	"log/slog"
	"webhookbroker/domain"
	"webhookbroker/pkg/filters"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	FetchUnprocessedEvents(ctx context.Context, limit int, workerID string) ([]domain.Event, error)
	GetActiveWebhooks(ctx context.Context) ([]domain.Webhook, error)
	DispatchToOutbox(ctx context.Context, matches []domain.OutboxDelivery, highestIndex int64, workerID string) error
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

func (s *Service) ProcessBatch(ctx context.Context, batchSize int, workerID string) (int, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	repo := s.repoFn(tx)

	events, webhooks, err := s.fetchData(ctx, repo, batchSize, workerID)
	if err != nil || len(events) == 0 {
		return 0, err
	}

	slog.Info("Fetched events", slog.Int("amount", len(events)))

	compiledHooks := s.compileWebhooks(webhooks)
	matches, highestIndex := s.evaluateEvents(events, compiledHooks)

	if err := repo.DispatchToOutbox(ctx, matches, highestIndex, workerID); err != nil {
		return 0, err
	}

	return len(events), tx.Commit(ctx)
}

func (s *Service) fetchData(ctx context.Context, repo Repository, limit int, workerID string) ([]domain.Event, []domain.Webhook, error) {
	events, err := repo.FetchUnprocessedEvents(ctx, limit, workerID)
	if err != nil || len(events) == 0 {
		return nil, nil, err
	}

	webhooks, err := repo.GetActiveWebhooks(ctx)
	if err != nil {
		return nil, nil, err
	}

	return events, webhooks, nil
}

func (s *Service) compileWebhooks(webhooks []domain.Webhook) []compiledWebhook {
	compiled := make([]compiledWebhook, 0, len(webhooks))
	for _, w := range webhooks {
		// Construct the slice of interfaces based on the domain config
		var hookFilters []filters.EventFilter

		if w.Filters.Divisor != nil {
			hookFilters = append(hookFilters, &filters.DivisibleFilter{Divisor: *w.Filters.Divisor})
		}
		if w.Filters.Issuer != nil {
			hookFilters = append(hookFilters, &filters.IssuerFilter{Expected: *w.Filters.Issuer})
		}
		if w.Filters.StartsWith != nil {
			hookFilters = append(hookFilters, &filters.StartsWithFilter{Prefix: *w.Filters.StartsWith})
		}

		compiled = append(compiled, compiledWebhook{
			hook:  w,
			chain: filters.BuildChain(hookFilters...),
		})
	}

	return compiled
}

func (s *Service) evaluateEvents(events []domain.Event, hooks []compiledWebhook) ([]domain.OutboxDelivery, int64) {
	matches := make([]domain.OutboxDelivery, 0, len(events))

	var highestIndex int64

	for _, event := range events {
		highestIndex = event.Index
		for _, compiled := range hooks {
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

	return matches, highestIndex
}
