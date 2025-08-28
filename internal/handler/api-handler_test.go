package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"telemetry-pipeline/internal/handler"
	"telemetry-pipeline/internal/model"
)

// --- Mock GPUService ---
type MockGPUService struct {
	mock.Mock
}

func (m *MockGPUService) ListGPUs(ctx context.Context, limit, offset int) ([]model.GPU, int, error) {
	args := m.Called(ctx, limit, offset)
	return args.Get(0).([]model.GPU), args.Int(1), args.Error(2)
}

func (m *MockGPUService) GetGPUTelemetry(ctx context.Context, device string, hostname string, start, end *time.Time, limit, offset int) ([]model.Telemetry, int, error) {
	args := m.Called(ctx, device, hostname, start, end, limit, offset)
	return args.Get(0).([]model.Telemetry), args.Int(1), args.Error(2)
}

func (m *MockGPUService) GetGPUMetrics(ctx context.Context, device string, hostname string, metricName model.MetricName, agg model.Aggregation, start, end *time.Time, limit, offset int) ([]model.MetricsResponse, error) {
	args := m.Called(ctx, device, hostname, metricName, agg, start, end, limit, offset)
	return args.Get(0).([]model.MetricsResponse), args.Error(1)
}

// --- Tests ---

func TestListGPUs_OK(t *testing.T) {
	mockSvc := new(MockGPUService)
	h := handler.NewGPUHandler(mockSvc)

	expected := []model.GPU{{Device: "nvidia0", Hostname: "node1"}}
	mockSvc.On("ListGPUs", mock.Anything, 10, 0).Return(expected, 1, nil)

	req := httptest.NewRequest("GET", "/gpus", nil)
	w := httptest.NewRecorder()
	h.ListGPUs(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body model.GPUResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	assert.Equal(t, 1, body.Total)
	assert.Equal(t, expected, body.Results)

	mockSvc.AssertExpectations(t)
}

func TestListGPUs_Error(t *testing.T) {
	mockSvc := new(MockGPUService)
	h := handler.NewGPUHandler(mockSvc)

	mockSvc.On("ListGPUs", mock.Anything, 10, 0).
		Return([]model.GPU{}, 0, errors.New("db error"))

	req := httptest.NewRequest("GET", "/gpus", nil)
	w := httptest.NewRecorder()
	h.ListGPUs(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetGPUTelemetry_OK(t *testing.T) {
	mockSvc := new(MockGPUService)
	h := handler.NewGPUHandler(mockSvc)

	tele := []model.Telemetry{{Device: "dev", Hostname: "host", Value: 99.9}}
	mockSvc.On("GetGPUTelemetry", mock.Anything, "dev", "host",
		mock.Anything, mock.Anything, 10, 0).
		Return(tele, 1, nil)

	req := httptest.NewRequest("GET", "/gpus/dev:host/telemetry", nil)
	req = mux.SetURLVars(req, map[string]string{"gpu_id": "dev:host"})
	w := httptest.NewRecorder()
	h.GetGPUTelemetry(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body model.TelemetryResponse
	_ = json.NewDecoder(w.Body).Decode(&body)
	assert.Equal(t, 1, body.Total)
	assert.Equal(t, tele, body.Results)
}

func TestGetGPUTelemetry_InvalidUUID(t *testing.T) {
	mockSvc := new(MockGPUService)
	h := handler.NewGPUHandler(mockSvc)

	req := httptest.NewRequest("GET", "/gpus/invalid/telemetry", nil)
	req = mux.SetURLVars(req, map[string]string{"gpu_id": "invalid"})
	w := httptest.NewRecorder()
	h.GetGPUTelemetry(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetGPUMetrics_OK(t *testing.T) {
	mockSvc := new(MockGPUService)
	h := handler.NewGPUHandler(mockSvc)

	expected := []model.MetricsResponse{{
		Device: "dev", Hostname: "host",
		MetricName: "DCGM_FI_DEV_DEC_UTIL", MetricValue: 75.5,
	}}
	mockSvc.On("GetGPUMetrics", mock.Anything, "dev", "host",
		model.MetricName("DCGM_FI_DEV_DEC_UTIL"), model.Aggregation("avg"),
		mock.Anything, mock.Anything, 10, 0).
		Return(expected, nil)

	req := httptest.NewRequest("GET", "/gpus/dev:host/metrics?metric=DCGM_FI_DEV_DEC_UTIL&aggregation=avg", nil)
	req = mux.SetURLVars(req, map[string]string{"gpu_id": "dev:host"})
	w := httptest.NewRecorder()
	h.GetGPUMetrics(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body []model.MetricsResponse
	_ = json.NewDecoder(w.Body).Decode(&body)
	assert.Equal(t, expected, body)
}

func TestGetGPUMetrics_InvalidParams(t *testing.T) {
	mockSvc := new(MockGPUService)
	h := handler.NewGPUHandler(mockSvc)

	// invalid metric
	req := httptest.NewRequest("GET", "/gpus/dev:host/metrics?metric=wrong&aggregation=avg", nil)
	req = mux.SetURLVars(req, map[string]string{"gpu_id": "dev:host"})
	w := httptest.NewRecorder()
	h.GetGPUMetrics(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	// invalid aggregation
	req = httptest.NewRequest("GET", "/gpus/dev:host/metrics?metric=DCGM_FI_DEV_DEC_UTIL&aggregation=wrong", nil)
	req = mux.SetURLVars(req, map[string]string{"gpu_id": "dev:host"})
	w = httptest.NewRecorder()
	h.GetGPUMetrics(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetGPUMetrics_ErrorPropagation(t *testing.T) {
	mockSvc := new(MockGPUService)
	h := handler.NewGPUHandler(mockSvc)

	mockSvc.On("GetGPUMetrics", mock.Anything, "dev", "host",
		model.MetricName("DCGM_FI_DEV_DEC_UTIL"), model.Aggregation("avg"),
		mock.Anything, mock.Anything, 10, 0).
		Return([]model.MetricsResponse{}, errors.New("query error"))

	req := httptest.NewRequest("GET", "/gpus/dev:host/metrics?metric=DCGM_FI_DEV_DEC_UTIL&aggregation=avg", bytes.NewReader(nil))
	req = mux.SetURLVars(req, map[string]string{"gpu_id": "dev:host"})
	w := httptest.NewRecorder()
	h.GetGPUMetrics(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
