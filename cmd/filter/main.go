package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"webhookbroker/internal/config"
	"webhookbroker/internal/db"
	"webhookbroker/internal/repo"
	"webhookbroker/internal/service/filter"

	"github.com/jackc/pgx/v5"
)

const (
	filterIntervalSec = 2
	batchSzie         = 100
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	repoFactory := func(tx pgx.Tx) filter.Repository {
		return repo.NewPostgresRepo(tx)
	}

	svc := filter.NewService(dbPool, repoFactory)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.Info("Received shutdown signal.")
		cancel()
	}()

	worker := filter.NewWorker(svc, filterIntervalSec*time.Second)
	worker.Start(ctx, batchSzie)
}
