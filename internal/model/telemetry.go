package model

import "time"

type Telemetry struct {
	Timestamp  time.Time         `json:"timestamp"`
	MetricName string            `json:"metric_name"`
	GPUId      int               `json:"gpu_id"`
	Device     string            `json:"device"`
	UUID       string            `json:"uuid"`
	ModelName  string            `json:"modelName"`
	Hostname   string            `json:"hostname"`
	Container  string            `json:"container"`
	Pod        string            `json:"pod"`
	Namespace  string            `json:"namespace"`
	Value      float64           `json:"value"`
	LabelsRaw  map[string]string `json:"labels_raw"`
}
