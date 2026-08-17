package integration_tests

import (
	"context"
	"fmt"
	"webhookbroker/internal/config"
	"webhookbroker/internal/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

func SetupTestDB() (*pgxpool.Pool, error) {
	ctx := context.Background()

	dbcf := config.DBConfig{
		DSN:      "postgres://testuser:testpassword@localhost:5434/testdb?sslmode=disable",
		MaxConns: 10,
		MinConns: 5,
	}

	pool, err := db.InitDB(ctx, dbcf)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func CleanDB(pool *pgxpool.Pool) error {
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		TRUNCATE TABLE outbox_deliveries, events, webhooks RESTART IDENTITY CASCADE;
	`)

	if err != nil {
		return fmt.Errorf("failed to clean database tables: %w", err)
	}

	return nil
}
