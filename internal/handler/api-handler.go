package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"telemetry-pipeline/internal/model"
)

type GPUServiceInterface interface {
	ListGPUs(ctx context.Context, limit, offset int) ([]model.GPU, int, error)
	GetGPUTelemetry(ctx context.Context, device string, hostname string, start, end *time.Time, limit, offset int) ([]model.Telemetry, int, error)
	GetGPUMetrics(ctx context.Context, device string, hostname string, metricName model.MetricName, agg model.Aggregation, start, end *time.Time, limit, offset int) ([]model.MetricsResponse, error)
}

type GPUHandler struct {
	service GPUServiceInterface
}

func NewGPUHandler(service GPUServiceInterface) *GPUHandler {
	return &GPUHandler{service: service}
}

// type GPUHandler struct {
// 	service *service.GPUService
// }

// func NewGPUHandler(service *service.GPUService) *GPUHandler {
// 	return &GPUHandler{service: service}
// }

// @Summary List GPUs
// @Description Returns list of unique GPU IDs available in telemetry data.
// @Tags Telemetry
// @Produce json
// @Param limit query int false "Limit number of records" default(10)
// @Param offset query int false "Offset for pagination" default(0)
// @Success 200 {object} model.GPUResponse
// @Failure 400 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /gpus [get]
func (h *GPUHandler) ListGPUs(w http.ResponseWriter, r *http.Request) {
	log.Printf("Got List GPU request ...")
	// Pagination params
	limit, offset := getLimitOffset(r)
	gpus, total, err := h.service.ListGPUs(r.Context(), limit, offset)
	if err != nil {
		log.Printf("Error in List GPU request: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	resp := model.GPUResponse{
		Total:   total,
		Limit:   limit,
		Offset:  offset,
		Count:   len(gpus),
		Results: gpus,
	}
	writeJSON(w, resp)
}

// @Summary Get telemetry for a GPU
// @Description Returns telemetry data for a given GPU ID.
// @Tags Telemetry
// @Produce json
// @Param gpu_id path string true "GPU ID"
// @Param start_time query string false "Start time (sample: 2025-08-28T08:46:58Z)"
// @Param end_time query string false "End time (sample: 2025-08-28T09:46:58Z)"
// @Param limit query int false "Limit number of records" default(10)
// @Param offset query int false "Offset for pagination" default(0)
// @Success 200 {object} model.TelemetryResponse
// @Router /gpus/{gpu_id}/telemetry [get]
func (h *GPUHandler) GetGPUTelemetry(w http.ResponseWriter, r *http.Request) {
	log.Printf("Got GPU Telemetry request....")
	vars := mux.Vars(r)
	rawGpuId := vars["gpu_id"]
	log.Printf("Got GPU Metrics request.... gpuid: %s", rawGpuId)

	ok, device, hostname := model.IsValidUUID(rawGpuId)
	if !ok {
		http.Error(w, fmt.Sprintf("invalid gpu id: %s", rawGpuId), http.StatusBadRequest)
		return
	}

	// Pagination params
	limit, offset := getLimitOffset(r)

	startTime, endTime, err := getStartEndTime(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	telemetry, total, err := h.service.GetGPUTelemetry(r.Context(), device, hostname, startTime, endTime, limit, offset)
	if err != nil {
		log.Printf("Error in GPU telemetry request: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	resp := model.TelemetryResponse{
		Total:   total,
		Limit:   limit,
		Offset:  offset,
		Count:   len(telemetry),
		Results: telemetry,
	}
	writeJSON(w, resp)
}

// @Summary Get metrics for a GPU
// @Description Returns metrics data for a given GPU ID and metrics name.
// @Tags Telemetry
// @Produce json
// @Param gpu_id path string true "GPU ID"
// @Param metric query model.MetricName true "Metric name"
// @Param aggregation query model.Aggregation true "Aggregation function"
// @Param start_time query string false "Start time (sample: 2025-08-28T08:46:58Z)"
// @Param end_time query string false "End time (sample: 2025-08-28T09:46:58Z)"
// @Param limit query int false "Limit number of records" default(10)
// @Param offset query int false "Offset for pagination" default(0)
// @Success 200 {object} model.TelemetryResponse
// @Router /gpus/{gpu_id}/metrics [get]
func (h *GPUHandler) GetGPUMetrics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rawGpuId := vars["gpu_id"]
	log.Printf("Got GPU Metrics request.... gpuid: %s", rawGpuId)

	ok, gpuId, hostname := model.IsValidUUID(rawGpuId)
	if !ok {
		http.Error(w, fmt.Sprintf("invalid gpu id: %s", rawGpuId), http.StatusBadRequest)
		return
	}

	q := r.URL.Query()
	metricName := model.MetricName(q.Get("metric"))
	aggregation := model.Aggregation(q.Get("aggregation"))
	if !metricName.IsValid() {
		http.Error(w, fmt.Sprintf("invalid metric: %s", metricName), http.StatusBadRequest)
		return
	}
	if !aggregation.IsValid() {
		http.Error(w, fmt.Sprintf("invalid aggregation: %s", aggregation), http.StatusBadRequest)
		return
	}

	// Pagination params
	limit, offset := getLimitOffset(r)

	startTime, endTime, err := getStartEndTime(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	metrics, err := h.service.GetGPUMetrics(r.Context(), gpuId, hostname, metricName, aggregation, startTime, endTime, limit, offset)
	if err != nil {
		log.Printf("Error in GPU metrics request: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	writeJSON(w, metrics)
}

// Utility
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func getLimitOffset(r *http.Request) (int, int) {
	// Pagination params
	limit := 10
	offset := 0

	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
			limit = l
		}
	}
	if oStr := r.URL.Query().Get("offset"); oStr != "" {
		if o, err := strconv.Atoi(oStr); err == nil && o >= 0 {
			offset = o
		}
	}
	return limit, offset
}

func getStartEndTime(r *http.Request) (*time.Time, *time.Time, error) {
	// Time filters
	var startTime, endTime *time.Time

	if startStr := r.URL.Query().Get("start_time"); startStr != "" {
		t, err := time.Parse(time.RFC3339, startStr)
		if err != nil {
			log.Printf("invalid start_time format: startTime: %s %v", startStr, err)
			return nil, nil, fmt.Errorf("invalid start_time format (expected RFC3339)")
		}
		startTime = &t
	}

	if endStr := r.URL.Query().Get("end_time"); endStr != "" {
		t, err := time.Parse(time.RFC3339, endStr)
		if err != nil {
			log.Printf("invalid end_time format: startTime: %s %v", endStr, err)
			return nil, nil, fmt.Errorf("invalid end_time format (expected RFC3339)")
		}
		endTime = &t
	}
	return startTime, endTime, nil
}
