package repository

import (
	"context"
	"database/sql"
	"fmt"
	"telemetry-pipeline/internal/model"
	"time"
)

type GPURepository struct {
	db *sql.DB
}

func NewGPURepository(db *sql.DB) *GPURepository {
	return &GPURepository{db: db}
}

func (r *GPURepository) ListGPUs(ctx context.Context, limit, offset int) ([]model.GPU, int, error) {
	args := []interface{}{limit, offset}
	query := `
	SELECT DISTINCT gpu_id, device, hostname,
		COUNT(*) OVER() AS total_count
	FROM gpu_telemetry
		ORDER BY gpu_id
		LIMIT $1  OFFSET $2`

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	totalCount := 0
	var gpus []model.GPU
	for rows.Next() {
		var g model.GPU
		if err := rows.Scan(&g.GPUId, &g.Device, &g.Hostname, &totalCount); err != nil {
			return nil, 0, err
		}
		gpus = append(gpus, g)
	}
	return gpus, totalCount, nil
}

// GPU telemetry with pagination and optional time range
func (r *GPURepository) GetGPUTelemetry(
	ctx context.Context,
	gpuID int,
	startTime, endTime *time.Time,
	limit, offset int,
) ([]model.Telemetry, int, error) {

	query := `
		SELECT id, ts, metric_name, gpu_id, device, uuid, model_name, hostname,
		       container, pod, namespace, metric_value, labels_raw, created_at,
		       COUNT(*) OVER() AS total_count
		FROM gpu_telemetry
		WHERE gpu_id = $1
	`
	args := []interface{}{gpuID}
	argIndex := 2

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
		ORDER BY ts DESC
		LIMIT $` + itoa(argIndex) + ` OFFSET $` + itoa(argIndex+1)

	args = append(args, limit, offset)

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
func (r *GPURepository) GetGPUMetrics(
	ctx context.Context,
	gpuID int,
	metricName string,
	startTime, endTime *time.Time,
	limit, offset int,
) ([]model.Telemetry, int, error) {

	query := `
		SELECT id, ts, metric_name, gpu_id, device, uuid, model_name, hostname,
		       container, pod, namespace, metric_value, labels_raw, created_at,
		       COUNT(*) OVER() AS total_count
		FROM gpu_telemetry
		WHERE gpu_id = $1 and metric_name = $2
	`
	args := []interface{}{gpuID, metricName}
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
		ORDER BY ts DESC
		LIMIT $` + itoa(argIndex) + ` OFFSET $` + itoa(argIndex+1)

	args = append(args, limit, offset)

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

// helper to convert int to string
func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
