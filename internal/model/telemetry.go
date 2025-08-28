package model

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Telemetry represents a single GPU telemetry record
// @Description Telemetry data collected from GPUs
type Telemetry struct {
	ID         int64     `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	MetricName string    `json:"metric_name"`
	GPUId      int       `json:"gpu_id"`
	Device     string    `json:"device"`
	UUID       string    `json:"uuid"`
	ModelName  string    `json:"modelName"`
	Hostname   string    `json:"Hostname"`
	Container  string    `json:"container"`
	Pod        string    `json:"pod"`
	Namespace  string    `json:"namespace"`
	Value      float64   `json:"value"`
	LabelsRaw  JSONBMap  `json:"labels_raw"`
	CreatedAt  time.Time `json:"created_at"`
}

// GPU represents a single GPU record
// @Description GPUs device from where telemetry collected
type GPU struct {
	GPUId    int    `json:"gpu_id,omitempty"`
	UUID     string `json:"uuid,omitempty"`
	Device   string `json:"device,omitempty"`
	Hostname string `json:"hostname,omitempty"`
}

func (g *GPU) SetUUID() {
	g.UUID = ConstructUUID(g.Device, g.Hostname)
}

func (g *GPU) getUUID() string {
	return g.UUID
}

func ConstructUUID(device, hostname string) string {
	return fmt.Sprintf("%s:%s", device, hostname)
}

func IsValidUUID(uuid string) (bool, string, string) {
	seg := strings.Split(uuid, ":")
	if len(seg) != 2 {
		return false, "", ""
	}
	return true, seg[0], seg[1]
}

// PaginatedResponse is a reusable generic struct for paginated API responses.
type PaginatedResponse[T any] struct {
	Total   int `json:"total"`
	Limit   int `json:"limit"`
	Offset  int `json:"offset"`
	Count   int `json:"count"`
	Results []T `json:"results"`
}

type MetricsResponse struct {
	UUID        string  `json:"uuid,omitempty"`
	Device      string  `json:"device,omitempty"`
	Hostname    string  `json:"hostname,omitempty"`
	MetricName  string  `json:"metric_name,omitempty"`
	MetricValue float64 `json:"metric_value"`
}

// Aggregation enum
// @enum string
type Aggregation string

const (
	AggregationMin Aggregation = "min"
	AggregationMax Aggregation = "max"
	AggregationAvg Aggregation = "avg"
	AggregationSum Aggregation = "sum"
)

func (a Aggregation) IsValid() bool {
	switch a {
	case AggregationMin, AggregationMax, AggregationAvg, AggregationSum:
		return true
	}
	return false
}

// MetricName enum
// @enum string
type MetricName string

const (
	MetricDecUtil     MetricName = "DCGM_FI_DEV_DEC_UTIL"
	MetricEncUtil     MetricName = "DCGM_FI_DEV_ENC_UTIL"
	MetricFbFree      MetricName = "DCGM_FI_DEV_FB_FREE"
	MetricFbUsed      MetricName = "DCGM_FI_DEV_FB_USED"
	MetricGpuTemp     MetricName = "DCGM_FI_DEV_GPU_TEMP"
	MetricGpuUtil     MetricName = "DCGM_FI_DEV_GPU_UTIL"
	MetricMemClock    MetricName = "DCGM_FI_DEV_MEM_CLOCK"
	MetricMemCopyUtil MetricName = "DCGM_FI_DEV_MEM_COPY_UTIL"
	MetricPowerUsage  MetricName = "DCGM_FI_DEV_POWER_USAGE"
	MetricSmClock     MetricName = "DCGM_FI_DEV_SM_CLOCK"
)

func (m MetricName) IsValid() bool {
	switch m {
	case MetricEncUtil, MetricDecUtil, MetricFbFree, MetricFbUsed,
		MetricGpuTemp, MetricGpuUtil, MetricMemClock, MetricMemCopyUtil,
		MetricPowerUsage, MetricSmClock:
		return true
	}
	return false
}

// Alias for Swagger to work with Telemetry
type TelemetryResponse = PaginatedResponse[Telemetry]

// Alias for Swagger to work with Telemetry
type GPUResponse = PaginatedResponse[GPU]

type JSONBMap map[string]string

func (m *JSONBMap) Scan(value interface{}) error {
	if value == nil {
		*m = make(map[string]string)
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("JSONBMap: cannot convert %T to []byte", value)
	}
	return json.Unmarshal(b, m)
}

func (m JSONBMap) Value() (driver.Value, error) {
	return json.Marshal(m)
}
