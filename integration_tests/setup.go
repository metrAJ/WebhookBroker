package integration_tests

import (
	"context"
	"os"
	"webhookbroker/internal/config"
	"webhookbroker/internal/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

func SetupTestDB(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := "postgres://testuser:testpassword@localhost:5434/testdb?sslmode=disable"
	if envDsn := os.Getenv("DB_URL"); envDsn != "" {
		dsn = envDsn
	}

	dbcf := config.DBConfig{
		DSN:      dsn,
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

	return err
}
