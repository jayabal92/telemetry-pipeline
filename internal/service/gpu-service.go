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

func (s *GPUService) ListGPUs(ctx context.Context, limit, offset int) ([]model.GPU, int, error) {
	return s.repo.ListGPUs(ctx, limit, offset)
}

func (s *GPUService) GetGPUTelemetry(ctx context.Context, device string, hostname string, start, end *time.Time, limit, offset int) ([]model.Telemetry, int, error) {
	return s.repo.GetGPUTelemetry(ctx, device, hostname, start, end, limit, offset)
}

func (s *GPUService) GetGPUMetrics(ctx context.Context, device string, hostname string, metricName model.MetricName, agg model.Aggregation, start, end *time.Time, limit, offset int) ([]model.MetricsResponse, error) {
	return s.repo.GetGPUMetrics(ctx, device, hostname, metricName, agg, start, end, limit, offset)
}
