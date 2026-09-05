package integration_tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"
	"webhookbroker/internal/config"
	"webhookbroker/internal/repo"
	"webhookbroker/internal/service/dispatch"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func RunScenarioRetry(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	serverHit := make(chan bool, 1)
	// Fake server to check incremention
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		serverHit <- true

		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	testCfg := config.DispatcherConfig{
		MaxRetries:   3,
		BaseSecWait:  5 * time.Millisecond,
		WorkerCount:  1,
		TaskLeaseSec: 1,
	}
	repoFactory := func(tx pgx.Tx) dispatch.Repository {
		return repo.NewPostgresRepo(tx)
	}
	svc := dispatch.NewService(testCfg, pool, repoFactory)
	mgr := dispatch.NewManager(svc)

	webhookID, err := seedWebhook(ctx, pool, server.URL)
	if err != nil {
		return err
	}

	if err := seedEvent(ctx, pool, webhookID, "data"); err != nil {
		return err
	}

	go mgr.Start(ctx)

	select {
	case <-serverHit:
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for the retry server to be hit")
	}

	var (
		currentRetry  int
		nextRetryTime *time.Time
	)

	time.Sleep(100 * time.Millisecond)

	err = pool.QueryRow(ctx, `
		SELECT current_retry, next_retry_time
		FROM webhooks
		WHERE id = $1
	`, webhookID).Scan(&currentRetry, &nextRetryTime)
	if err != nil {
		return fmt.Errorf("failed to query final webhook state: %w", err)
	}

	if currentRetry != 1 {
		return fmt.Errorf("expected current_retry to be 1, got %d", currentRetry)
	}

	if nextRetryTime == nil {
		return fmt.Errorf("expected next_retry_time to be set, but it was NULL")
	}

	return nil
}
