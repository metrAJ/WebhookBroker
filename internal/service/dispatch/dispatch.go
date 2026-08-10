package dispatch

import (
	"context"
	"webhookbroker/internal/domain"
)

type Repository interface {
	FetchNextTask(ctx context.Context) (*domain.DeliveryTask, error)
}

type Service struct {
	repo Repository
}

func NewService(r Repository) *Service {
	return &Service{repo: r}
}

func (s *Service) Run(ctx context.Context) {
}

func (s *Service) processTask(ctx context.Context) {
}
