package integration_tests

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"time"
	"webhookbroker/internal/config"
	"webhookbroker/internal/repo"
	"webhookbroker/internal/service/dispatch"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func RunScenarioOrder(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payloadChan := make(chan string, 3)
	// Start fake server and record what it receives
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		payloadChan <- string(body)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

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

	webhookID, err := seedWebhook(ctx, pool, server.URL)
	if err != nil {
		return fmt.Errorf("failed to arrange webhook: %w", err)
	}

	if err := seedEvent(ctx, pool, webhookID, `{"sequence":"first"}`); err != nil {
		return fmt.Errorf("failed to arrange first event: %w", err)
	}

	if err := seedEvent(ctx, pool, webhookID, `{"sequence":"second"}`); err != nil {
		return fmt.Errorf("failed to arrange second event: %w", err)
	}

	if err := seedEvent(ctx, pool, webhookID, `{"sequence":"third"}`); err != nil {
		return fmt.Errorf("failed to arrange third event: %w", err)
	}

	go mgr.Start(ctx)

	var receivedPayloads []string

	for i := 0; i < 3; i++ {
		select {
		case p := <-payloadChan:
			receivedPayloads = append(receivedPayloads, p)
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for payloads")
		}
	}

	if receivedPayloads[0] != `{"sequence":"first"}` {
		return fmt.Errorf("expected first payload to be 'first', got: %s", receivedPayloads[0])
	}

	if receivedPayloads[1] != `{"sequence":"second"}` {
		return fmt.Errorf("expected second payload to be 'second', got: %s", receivedPayloads[1])
	}

	return nil
}
