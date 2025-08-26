package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"telemetry-pipeline/internal/service"
)

type GPUHandler struct {
	service *service.GPUService
}

func NewGPUHandler(service *service.GPUService) *GPUHandler {
	return &GPUHandler{service: service}
}

// GET /api/v1/gpus
func (h *GPUHandler) ListGPUs(w http.ResponseWriter, r *http.Request) {
	log.Printf("Got List GPU request ...")
	// Pagination params
	limit, offset := getLimitOffset(r)
	gpus, err := h.service.ListGPUs(r.Context(), limit, offset)
	if err != nil {
		log.Printf("Error in List GPU request: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, gpus)
}

// GET /api/v1/gpus/{id}/telemetry?start_time=...&end_time=...&limit=...&offset=...
func (h *GPUHandler) GetGPUTelemetry(w http.ResponseWriter, r *http.Request) {
	log.Printf("Got GPU Telemetry request....")
	vars := mux.Vars(r)
	idStr := vars["id"]
	gpuID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid GPU id", http.StatusBadRequest)
		return
	}

	// Pagination params
	limit, offset := getLimitOffset(r)

	startTime, endTime, err := getStartEndTime(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	telemetry, total, err := h.service.GetGPUTelemetry(r.Context(), gpuID, startTime, endTime, limit, offset)
	if err != nil {
		log.Printf("Error in GPU telemetry request: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	resp := map[string]interface{}{
		"total":     total,
		"limit":     limit,
		"offset":    offset,
		"count":     len(telemetry),
		"telemetry": telemetry,
	}
	writeJSON(w, resp)
}

// GET /api/v1/gpus/{id}/metrics/{metric_name}?start_time=...&end_time=...&limit=...&offset=...
func (h *GPUHandler) GetGPUMetrics(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	idStr := vars["id"]
	metricName := vars["metric_name"]
	log.Printf("Got GPU Metrics request.... metrics: %s gpuid: %s", metricName, idStr)

	gpuID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid GPU id", http.StatusBadRequest)
		return
	}

	// Pagination params
	limit, offset := getLimitOffset(r)

	startTime, endTime, err := getStartEndTime(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	telemetry, total, err := h.service.GetGPUMetrics(r.Context(), gpuID, metricName, startTime, endTime, limit, offset)
	if err != nil {
		log.Printf("Error in GPU metrics request: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	resp := map[string]interface{}{
		"total":     total,
		"limit":     limit,
		"offset":    offset,
		"count":     len(telemetry),
		"telemetry": telemetry,
	}
	writeJSON(w, resp)
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
