package service

import (
	"context"
	"telemetry-pipeline/internal/model"
	"telemetry-pipeline/internal/repository"
	"time"
)

type GPUService struct {
	repo *repository.GPURepository
}

func NewGPUService(repo *repository.GPURepository) *GPUService {
	return &GPUService{repo: repo}
}

func (s *GPUService) ListGPUs(ctx context.Context, limit, offset int) ([]model.GPU, error) {
	return s.repo.ListGPUs(ctx, limit, offset)
}

func (s *GPUService) GetGPUTelemetry(ctx context.Context, id int, start, end *time.Time, limit, offset int) ([]model.Telemetry, int, error) {
	return s.repo.GetGPUTelemetry(ctx, id, start, end, limit, offset)
}

func (s *GPUService) GetGPUMetrics(ctx context.Context, id int, metricName string, start, end *time.Time, limit, offset int) ([]model.Telemetry, int, error) {
	return s.repo.GetGPUMetrics(ctx, id, metricName, start, end, limit, offset)
}
