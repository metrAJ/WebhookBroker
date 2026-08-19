package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"webhookbroker/internal/config"
	"webhookbroker/internal/db"
	"webhookbroker/internal/repo"
	"webhookbroker/internal/service/dispatch"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	dbPool, err := db.InitDB(ctx, cfg.DB)
	if err != nil {
		slog.Error("Database initialization failed", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	repoFactory := func(tx pgx.Tx) dispatch.Repository {
		return repo.NewPostgresRepo(tx)
	}

	dispatchService := dispatch.NewService(cfg.Dispatcher, dbPool, repoFactory)

	manager := dispatch.NewManager(dispatchService)
	manager.Start(ctx)

	slog.Info("Dispatcher gracefully shut down")
}
