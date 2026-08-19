package dispatch

import (
	"context"
	"log/slog"
	"sync"
	"time"
	"webhookbroker/internal/domain"
)

type Manager struct {
	service *Service
	workers []chan domain.DeliveryTask
}

func NewManager(svc *Service) *Manager {
	return &Manager{
		service: svc,
	}
}

func (m *Manager) Start(ctx context.Context) {
	slog.Info("Starting dispatcher pool", slog.Int("workers", m.service.cf.WorkerCount))
	m.workers = make([]chan domain.DeliveryTask, m.service.cf.WorkerCount)

	var wg sync.WaitGroup

	for i := range m.service.cf.WorkerCount {
		ch := make(chan domain.DeliveryTask, 10)
		m.workers[i] = ch

		wg.Go(func() {
			m.workerInit(ctx, i, ch)
		})
	}

	m.pollingLoop(ctx)

	for _, ch := range m.workers {
		close(ch)
	}

	wg.Wait()
	slog.Info("All dispatcher workers shut down gracefully")
}

func (m *Manager) workerInit(ctx context.Context, id int, taskChan <-chan domain.DeliveryTask) {
	slog.Debug("Worker started", slog.Int("worker_id", id))

	for task := range taskChan {
		m.service.processTask(ctx, task)
	}
}

func (m *Manager) pollingLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Manager polling loop shutting down")
			return
		case <-ticker.C:
			tasks, err := m.service.claimTasks(ctx, m.service.cf.WorkerCount)
			if err != nil {
				slog.Error("Manager failed to claim tasks", slog.String("error", err.Error()))
				continue
			}

			for _, task := range tasks {
				workerIdx := task.WebhookID % m.service.cf.WorkerCount
				m.workers[workerIdx] <- task
			}
		}
	}
}
