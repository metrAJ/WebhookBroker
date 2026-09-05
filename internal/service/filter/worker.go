package filter

import (
	"context"
	"log/slog"
	"time"
)

type Worker struct {
	svc      *Service
	interval time.Duration
	workerID string
}

func NewWorker(svc *Service, interval time.Duration, workerID string) *Worker {
	return &Worker{
		svc:      svc,
		interval: interval,
		workerID: workerID,
	}
}

func (w *Worker) Start(ctx context.Context, batchSize int) {
	slog.Info("Starting event dispatcher worker", slog.String("interval", w.interval.String()))

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Worker stopped gracefully.")
			return
		case <-ticker.C:
			w.svc.ProcessBatch(ctx, batchSize, w.workerID)
		}
	}
}
