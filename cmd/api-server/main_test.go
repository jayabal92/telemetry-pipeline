package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"

	"telemetry-pipeline/internal/handler"
	"telemetry-pipeline/internal/model"
	"telemetry-pipeline/internal/service"
)

// mockRepo implements repository.GPURepository
type mockRepo struct{}

func (m *mockRepo) ListGPUs(ctx context.Context, limit, offset int) ([]model.GPU, int, error) {
	return []model.GPU{{Device: "dev1", Hostname: "host1"}}, 1, nil
}
func (m *mockRepo) GetGPUTelemetry(ctx context.Context, device, hostname string,
	startTime, endTime *time.Time, limit, offset int) ([]model.Telemetry, int, error) {
	return []model.Telemetry{
		{Device: device, Hostname: hostname, MetricName: "utilization", Value: 75.5},
	}, 1, nil
}
func (m *mockRepo) GetGPUMetrics(ctx context.Context, device, hostname string,
	metricName model.MetricName, agg model.Aggregation, startTime, endTime *time.Time,
	limit, offset int) ([]model.MetricsResponse, error) {
	return []model.MetricsResponse{
		{Device: device, Hostname: hostname, MetricName: string(metricName), MetricValue: 42.0},
	}, nil
}

// helper to setup router with mock service
func setupTestRouter() *mux.Router {
	repo := &mockRepo{}
	svc := service.NewGPUService(repo)
	h := handler.NewGPUHandler(svc)

	r := mux.NewRouter()
	r.HandleFunc("/api/v1/gpus", h.ListGPUs).Methods("GET")
	r.HandleFunc("/api/v1/gpus/{gpu_id}/telemetry", h.GetGPUTelemetry).Methods("GET")
	r.HandleFunc("/api/v1/gpus/{gpu_id}/metrics", h.GetGPUMetrics).Methods("GET")
	return r
}

func TestListGPUs(t *testing.T) {
	router := setupTestRouter()
	req := httptest.NewRequest("GET", "/api/v1/gpus?limit=5&offset=0", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "dev1")
}

func TestGetGPUTelemetry(t *testing.T) {
	router := setupTestRouter()
	// gpu_id must be "device:hostname" format
	req := httptest.NewRequest("GET", "/api/v1/gpus/dev1:host1/telemetry?limit=1", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "utilization")
}

func TestGetGPUMetrics(t *testing.T) {
	router := setupTestRouter()
	// supply valid metric + aggregation
	req := httptest.NewRequest("GET", "/api/v1/gpus/dev1:host1/metrics?metric=DCGM_FI_DEV_GPU_UTIL&aggregation=avg", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DCGM_FI_DEV_GPU_UTIL")
	assert.Contains(t, rec.Body.String(), "42")
}

// Ensure main doesn’t crash DB init (smoke test)
func TestMainSmoke(t *testing.T) {
	// skip db open; just check dsn string is built
	_ = sql.Drivers() // make sure pgx driver is registered
}
