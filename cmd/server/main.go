package main

import (
	"context"
	"log/slog"
	"os"
	"webhookbroker/internal/db"
	"webhookbroker/internal/logger"
)

func main() {
	logger.Setup("debug")

	ctx := context.Background()

	dsn := "postgres://user:password@localhost:5433/broker?sslmode=disable"

	pool, err := db.InitDB(ctx, dsn)
	if err != nil {
		slog.Error("Database initialization failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

}
