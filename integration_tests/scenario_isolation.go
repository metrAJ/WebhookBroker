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

func RunScenarioIsolation(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	badHit := make(chan bool, 1)
	good1Hit := make(chan bool, 1)
	good2Hit := make(chan bool, 1)

	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		badHit <- true

		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer badServer.Close()

	goodServer1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		good1Hit <- true

		w.WriteHeader(http.StatusOK)
	}))
	defer goodServer1.Close()

	goodServer2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		good2Hit <- true

		w.WriteHeader(http.StatusOK)
	}))
	defer goodServer2.Close()

	testCfg := config.DispatcherConfig{
		MaxRetries:   3,
		BaseSecWait:  1 * time.Millisecond,
		WorkerCount:  3,
		TaskLeaseSec: 1,
	}

	repoFactory := func(tx pgx.Tx) dispatch.Repository {
		return repo.NewPostgresRepo(tx)
	}
	svc := dispatch.NewService(testCfg, pool, repoFactory)
	mgr := dispatch.NewManager(svc)

	badHookID, err := seedWebhook(ctx, pool, badServer.URL)
	if err != nil {
		return err
	}

	goodHookID1, err := seedWebhook(ctx, pool, goodServer1.URL)
	if err != nil {
		return err
	}

	goodHookID2, err := seedWebhook(ctx, pool, goodServer2.URL)
	if err != nil {
		return err
	}

	if err := seedEvent(ctx, pool, badHookID, `{"status":"bad"}`); err != nil {
		return err
	}

	if err := seedEvent(ctx, pool, goodHookID1, `{"status":"good1"}`); err != nil {
		return err
	}

	if err := seedEvent(ctx, pool, goodHookID2, `{"status":"good2"}`); err != nil {
		return err
	}

	go mgr.Start(ctx)

	for i := 0; i < 3; i++ {
		select {
		case <-badHit:
		case <-good1Hit:
		case <-good2Hit:
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for HTTP requests")
		}
	}

	time.Sleep(100 * time.Millisecond)

	var badHookRetry int
	if err := pool.QueryRow(ctx, `SELECT current_retry FROM webhooks WHERE id = $1`, badHookID).Scan(&badHookRetry); err != nil {
		return fmt.Errorf("failed querying bad webhook: %w", err)
	}

	if badHookRetry != 1 {
		return fmt.Errorf("expected bad webhook to have 1 retry, got %d", badHookRetry)
	}

	var (
		goodHook1Retry  int
		goodHook1Cursor int64
	)
	if err := pool.QueryRow(ctx, `SELECT current_retry, last_processed_outbox_id FROM webhooks WHERE id = $1`, goodHookID1).Scan(&goodHook1Retry, &goodHook1Cursor); err != nil {
		return fmt.Errorf("failed to query final webhook state: %w", err)
	}

	if goodHook1Retry != 0 {
		return fmt.Errorf("expected good webhook 1 to have 0 retries, got %d", goodHook1Retry)
	}

	if goodHook1Cursor == 0 {
		return fmt.Errorf("expected good webhook 1 cursor to advance, but it stayed at 0")
	}

	return nil
}
