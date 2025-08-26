package model

import "time"

// Telemetry represents a single GPU telemetry record
// @Description Telemetry data collected from GPUs
type Telemetry struct {
	ID         int64             `json:"id"`
	Timestamp  time.Time         `json:"timestamp"`
	MetricName string            `json:"metric_name"`
	GPUId      int               `json:"gpu_id"`
	Device     string            `json:"device"`
	UUID       string            `json:"uuid"`
	ModelName  string            `json:"modelName"`
	Hostname   string            `json:"Hostname"`
	Container  string            `json:"container"`
	Pod        string            `json:"pod"`
	Namespace  string            `json:"namespace"`
	Value      float64           `json:"value"`
	LabelsRaw  map[string]string `json:"labels_raw"`
	CreatedAt  time.Time         `json:"created_at"`
}

// GPU represents a single GPU record
// @Description GPUs device from where telemetry collected
type GPU struct {
	GPUId     int    `json:"gpu_id"`
	UUID      string `json:"uuid"`
	Device    string `json:"device"`
	ModelName string `json:"model_name"`
	Hostname  string `json:"hostname"`
}

// PaginatedResponse is a reusable generic struct for paginated API responses.
type PaginatedResponse[T any] struct {
	Total   int `json:"total"`
	Limit   int `json:"limit"`
	Offset  int `json:"offset"`
	Count   int `json:"count"`
	Results []T `json:"results"`
}

// Alias for Swagger to work with Telemetry
type TelemetryResponse = PaginatedResponse[Telemetry]

// Alias for Swagger to work with Telemetry
type GPUResponse = PaginatedResponse[GPU]
