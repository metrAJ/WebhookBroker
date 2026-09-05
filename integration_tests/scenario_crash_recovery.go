package integration_tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"
	"webhookbroker/internal/config"
	"webhookbroker/internal/repo"
	"webhookbroker/internal/service/dispatch"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type crashRecoveryRepo struct {
	dispatch.Repository
	crashed *atomic.Bool
}

func (c *crashRecoveryRepo) MarkSuccess(ctx context.Context, webhookID int, outboxID int64) error {
	if c.crashed.CompareAndSwap(false, true) {
		return fmt.Errorf("simulated fatal worker crash before state could be saved")
	}

	return c.Repository.MarkSuccess(ctx, webhookID, outboxID)
}

func RunScenarioCrashRecovery(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		deliveryCount atomic.Int32
		hasCrashed    atomic.Bool
	)

	serverHit := make(chan bool, 2)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		deliveryCount.Add(1)

		serverHit <- true

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	testCfg := config.DispatcherConfig{
		MaxRetries:   3,
		BaseSecWait:  1 * time.Millisecond,
		WorkerCount:  1,
		TaskLeaseSec: 1,
	}

	repoFactory := func(tx pgx.Tx) dispatch.Repository {
		realRepo := repo.NewPostgresRepo(tx)

		return &crashRecoveryRepo{
			Repository: realRepo,
			crashed:    &hasCrashed,
		}
	}

	svc := dispatch.NewService(testCfg, pool, repoFactory)
	mgr := dispatch.NewManager(svc)

	webhookID, err := seedWebhook(ctx, pool, server.URL)
	if err != nil {
		return err
	}

	if err := seedEvent(ctx, pool, webhookID, `{"data":"survive-crash"}`); err != nil {
		return err
	}

	go mgr.Start(ctx)

	for i := range 2 {
		select {
		case <-serverHit:
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for delivery %d; crash recovery failed", i+1)
		}
	}

	if deliveryCount.Load() != 2 {
		return fmt.Errorf("expected exactly 2 deliveries (1 initial + 1 recovery), got %d", deliveryCount.Load())
	}

	time.Sleep(100 * time.Millisecond)

	var cursor int64

	err = pool.QueryRow(ctx, `SELECT last_processed_outbox_id FROM webhooks WHERE id = $1`, webhookID).Scan(&cursor)
	if err != nil {
		return err
	}

	if cursor == 0 {
		return fmt.Errorf("expected outbox cursor to advance after recovery, but it remained 0")
	}

	return nil
}
