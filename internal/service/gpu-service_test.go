package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"telemetry-pipeline/internal/model"
	"telemetry-pipeline/internal/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mock Repository ---
type MockGPURepository struct {
	mock.Mock
}

func (m *MockGPURepository) ListGPUs(ctx context.Context, limit, offset int) ([]model.GPU, int, error) {
	args := m.Called(ctx, limit, offset)
	return args.Get(0).([]model.GPU), args.Int(1), args.Error(2)
}

func (m *MockGPURepository) GetGPUTelemetry(ctx context.Context, device, hostname string, start, end *time.Time, limit, offset int) ([]model.Telemetry, int, error) {
	args := m.Called(ctx, device, hostname, start, end, limit, offset)
	return args.Get(0).([]model.Telemetry), args.Int(1), args.Error(2)
}

func (m *MockGPURepository) GetGPUMetrics(ctx context.Context, device, hostname string, metricName model.MetricName, agg model.Aggregation, start, end *time.Time, limit, offset int) ([]model.MetricsResponse, error) {
	args := m.Called(ctx, device, hostname, metricName, agg, start, end, limit, offset)
	return args.Get(0).([]model.MetricsResponse), args.Error(1)
}

// --- Unit Tests ---
func TestGPUService_ListGPUs(t *testing.T) {
	mockRepo := new(MockGPURepository)
	svc := service.NewGPUService(mockRepo)

	expected := []model.GPU{{Device: "nvidia0", Hostname: "node1"}}
	mockRepo.On("ListGPUs", mock.Anything, 10, 0).Return(expected, 1, nil)

	res, total, err := svc.ListGPUs(context.Background(), 10, 0)

	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, expected, res)
	mockRepo.AssertExpectations(t)
}

func TestGPUService_GetGPUTelemetry(t *testing.T) {
	mockRepo := new(MockGPURepository)
	svc := service.NewGPUService(mockRepo)

	start := time.Now().Add(-time.Hour)
	end := time.Now()

	expected := []model.Telemetry{{Device: "nvidia0", Hostname: "node1", Value: 75.0}}
	mockRepo.On("GetGPUTelemetry", mock.Anything, "nvidia0", "node1", &start, &end, 10, 0).
		Return(expected, 1, nil)

	res, total, err := svc.GetGPUTelemetry(context.Background(), "nvidia0", "node1", &start, &end, 10, 0)

	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, expected, res)
	mockRepo.AssertExpectations(t)
}

func TestGPUService_GetGPUMetrics(t *testing.T) {
	mockRepo := new(MockGPURepository)
	svc := service.NewGPUService(mockRepo)

	start := time.Now().Add(-time.Hour)
	end := time.Now()

	expected := []model.MetricsResponse{{
		Device: "nvidia0", Hostname: "node1",
		MetricName: "utilization", MetricValue: 88.5,
	}}
	mockRepo.On("GetGPUMetrics", mock.Anything, "nvidia0", "node1", model.MetricName("utilization"), model.Aggregation("avg"), &start, &end, 5, 0).
		Return(expected, nil)

	res, err := svc.GetGPUMetrics(context.Background(), "nvidia0", "node1", "utilization", "avg", &start, &end, 5, 0)

	assert.NoError(t, err)
	assert.Equal(t, expected, res)
	mockRepo.AssertExpectations(t)
}

func TestGPUService_ErrorPropagation(t *testing.T) {
	mockRepo := new(MockGPURepository)
	svc := service.NewGPUService(mockRepo)

	mockRepo.On("ListGPUs", mock.Anything, 1, 0).
		Return([]model.GPU{}, 0, errors.New("db error"))

	_, _, err := svc.ListGPUs(context.Background(), 1, 0)

	assert.Error(t, err)
	assert.EqualError(t, err, "db error")
	mockRepo.AssertExpectations(t)
}
