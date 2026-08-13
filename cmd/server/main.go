package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"webhookbroker/internal/config"
	"webhookbroker/internal/db"
	"webhookbroker/internal/logger"
	"webhookbroker/internal/repo"
	"webhookbroker/internal/service/api"
	"webhookbroker/internal/service/api/transport"

	"github.com/jackc/pgx/v5"
)

func main() {
	logger.Setup("debug")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cf := config.Load()

	dbPool, err := db.InitDB(ctx, cf.DB)
	if err != nil {
		slog.Error("Database initialization failed", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	repoFactory := func(tx pgx.Tx) api.Repository {
		return repo.NewPostgresRepo(tx)
	}
	apiService := api.NewService(dbPool, repoFactory)
	handler := transport.NewHTTPHandler(apiService)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks", handler.RegisterWebhook)
	mux.HandleFunc("POST /events", handler.ReceiveEvent)

	srv := &http.Server{
		Addr:    ":" + cf.API.Port,
		Handler: mux,
	}

	go func() {
		slog.Info("Starting API Server", slog.String("port", cf.API.Port))

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server error", slog.String("error", err.Error()))
		}
	}()

	<-ctx.Done()
	slog.Info("Shutting down API server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv.Shutdown(shutdownCtx)
}
