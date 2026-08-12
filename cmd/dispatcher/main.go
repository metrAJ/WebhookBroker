package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"webhookbroker/internal/db"
	"webhookbroker/internal/repo"
	"webhookbroker/internal/service/dispatch"

	"github.com/jackc/pgx/v5"
)

const (
	dsn         = "postgres://user:password@localhost:5433/broker?sslmode=disable"
	workerCount = 20
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbPool, err := db.InitDB(ctx, dsn)
	if err != nil {
		slog.Error("Database initialization failed", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	repoFactory := func(tx pgx.Tx) dispatch.Repository {
		return repo.NewPostgresRepo(tx)
	}

	dispatchService := dispatch.NewService(dbPool, repoFactory)

	manager := dispatch.NewManager(dispatchService)

	slog.Info("Starting Dispatcher", slog.Int("workers", workerCount))

	manager.Start(ctx, workerCount)

	slog.Info("Dispatcher gracefully shut down")
}
