package integration_tests

import (
	"context"
	"testing"
	"webhookbroker/internal/config"
	"webhookbroker/internal/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

func setupTestDB(t *testing.T) *pgxpool.Pool {
	ctx := context.Background()

	dbcf := config.DBConfig{
		DSN:      "postgres://testuser:testpassword@localhost:5433/testdb?sslmode=disable",
		MaxConns: 10,
		MinConns: 5,
	}

	pool, err := db.InitDB(ctx, dbcf)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	_, err = pool.Exec(ctx, `
		TRUNCATE TABLE outbox_deliveries, events, webhooks RESTART IDENTITY CASCADE;
	`)
	if err != nil {
		t.Fatalf("Failed to clean database: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

func TestEnvironmentIsReady(t *testing.T) {
	pool := setupTestDB(t)

	var result int

	err := pool.QueryRow(context.Background(), "SELECT 1").Scan(&result)

	if err != nil || result != 1 {
		t.Fatalf("Database connection check failed")
	}
}
