package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"telemetry-pipeline/internal/model"
	"time"
)

type GPURepositoryImpl struct {
	db *sql.DB
}

// GPURepository defines the interface for telemetry storage operations.
type GPURepository interface {
	ListGPUs(ctx context.Context, limit, offset int) ([]model.GPU, int, error)
	GetGPUTelemetry(ctx context.Context, device, hostname string, startTime, endTime *time.Time, limit, offset int) ([]model.Telemetry, int, error)
	GetGPUMetrics(ctx context.Context, device string, hostname string, metricName model.MetricName, agg model.Aggregation, startTime, endTime *time.Time, limit, offset int) ([]model.MetricsResponse, error)
}

func NewGPURepository(db *sql.DB) GPURepository {
	return GPURepositoryImpl{db: db}
}

func (r GPURepositoryImpl) ListGPUs(ctx context.Context, limit, offset int) ([]model.GPU, int, error) {
	args := []interface{}{limit, offset}
	query := `
	SELECT DISTINCT device, hostname,
		COUNT(*) OVER() AS total_count
	FROM gpu_telemetry
		ORDER BY device, hostname
		LIMIT $1  OFFSET $2`

	log.Printf("ListGPUs query: %s", query)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	totalCount := 0
	var gpus []model.GPU
	for rows.Next() {
		var g model.GPU
		if err := rows.Scan(&g.Device, &g.Hostname, &totalCount); err != nil {
			return nil, 0, err
		}
		g.SetUUID()
		gpus = append(gpus, g)
	}
	return gpus, totalCount, nil
}

// GPU telemetry with pagination and optional time range
func (r GPURepositoryImpl) GetGPUTelemetry(
	ctx context.Context,
	device, hostname string,
	startTime, endTime *time.Time,
	limit, offset int,
) ([]model.Telemetry, int, error) {

	query := `
		SELECT id, ts, metric_name, gpu_id, device, uuid, model_name, hostname,
		       container, pod, namespace, metric_value, labels_raw, created_at,
		       COUNT(*) OVER() AS total_count
		FROM gpu_telemetry
		WHERE device = $1 and hostname = $2
	`
	args := []interface{}{device, hostname}
	argIndex := 3

	if startTime != nil {
		query += " AND ts >= $" + itoa(argIndex)
		args = append(args, *startTime)
		argIndex++
	}

	if endTime != nil {
		query += " AND ts <= $" + itoa(argIndex)
		args = append(args, *endTime)
		argIndex++
	}

	query += `
		ORDER BY ts, device, hostname DESC
		LIMIT $` + itoa(argIndex) + ` OFFSET $` + itoa(argIndex+1)

	args = append(args, limit, offset)

	log.Printf("GetGPUTelemetry query: %s", query)
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var telemetry []model.Telemetry
	totalCount := 0

	for rows.Next() {
		var t model.Telemetry
		if err := rows.Scan(
			&t.ID,
			&t.Timestamp,
			&t.MetricName,
			&t.GPUId,
			&t.Device,
			&t.UUID,
			&t.ModelName,
			&t.Hostname,
			&t.Container,
			&t.Pod,
			&t.Namespace,
			&t.Value,
			&t.LabelsRaw,
			&t.CreatedAt,
			&totalCount,
		); err != nil {
			return nil, 0, err
		}
		telemetry = append(telemetry, t)
	}

	return telemetry, totalCount, nil
}

// GPU telemetry with pagination and optional time range
func (r GPURepositoryImpl) GetGPUMetrics(
	ctx context.Context,
	device string,
	hostname string,
	metricName model.MetricName,
	agg model.Aggregation,
	startTime, endTime *time.Time,
	limit, offset int,
) ([]model.MetricsResponse, error) {

	args := []interface{}{device, hostname, metricName}
	argIndex := 4
	query := fmt.Sprintf("SELECT device, hostname, metric_name, %s(metric_value) from gpu_telemetry", agg)
	query += `
		WHERE device = $1 and hostname = $2 and metric_name = $3
	`
	if startTime != nil {
		query += " AND ts >= $" + itoa(argIndex)
		args = append(args, *startTime)
		argIndex++
	}

	if endTime != nil {
		query += " AND ts <= $" + itoa(argIndex)
		args = append(args, *endTime)
		argIndex++
	}

	query += `
		GROUP BY device, hostname, metric_name;`

	log.Printf("GetGPUMetrics query: %s", query)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []model.MetricsResponse

	for rows.Next() {
		var m model.MetricsResponse
		if err := rows.Scan(
			&m.Device,
			&m.Hostname,
			&m.MetricName,
			&m.MetricValue,
		); err != nil {
			return nil, err
		}
		m.UUID = model.ConstructUUID(m.Device, m.Hostname)
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// helper to convert int to string
func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
