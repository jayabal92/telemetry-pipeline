package repository_test

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	"telemetry-pipeline/internal/model"
	"telemetry-pipeline/internal/repository"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func setupMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, repository.GPURepository) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	repo := repository.NewGPURepository(db)
	return db, mock, repo
}

func TestListGPUs_Success(t *testing.T) {
	db, mock, repo := setupMockDB(t)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"device", "hostname", "total_count"}).
		AddRow("nvidia0", "node1", 1)

	mock.ExpectQuery(regexp.QuoteMeta(`
	SELECT DISTINCT device, hostname,
		COUNT(*) OVER() AS total_count
	FROM gpu_telemetry
		ORDER BY device, hostname
		LIMIT $1  OFFSET $2`)).
		WithArgs(10, 0).
		WillReturnRows(rows)

	res, total, err := repo.ListGPUs(context.Background(), 10, 0)

	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, res, 1)
	assert.Equal(t, "nvidia0", res[0].Device)
	assert.Equal(t, "node1", res[0].Hostname)
	assert.NotEmpty(t, res[0].UUID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetGPUTelemetry_Success(t *testing.T) {
	db, mock, repo := setupMockDB(t)
	defer db.Close()

	now := time.Now()

	rows := sqlmock.NewRows([]string{
		"id", "ts", "metric_name", "gpu_id", "device", "uuid", "model_name",
		"hostname", "container", "pod", "namespace", "metric_value",
		"labels_raw", "created_at", "total_count",
	}).AddRow(
		1, now, "gpu_utilization", 0, "nvidia0", "uuid-1", "Tesla-T4",
		"node1", "cont1", "pod1", "ns1", 99.5,
		[]byte(`{"k":"v"}`), now, 1,
	)

	mock.ExpectQuery("SELECT id, ts, metric_name").
		WithArgs("nvidia0", "node1", 10, 0).
		WillReturnRows(rows)

	res, total, err := repo.GetGPUTelemetry(context.Background(), "nvidia0", "node1", nil, nil, 10, 0)

	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, res, 1)
	assert.Equal(t, "gpu_utilization", res[0].MetricName)
	assert.Equal(t, 99.5, res[0].Value)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetGPUMetrics_Success(t *testing.T) {
	db, mock, repo := setupMockDB(t)
	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"device", "hostname", "metric_name", "metric_value",
	}).AddRow("nvidia0", "node1", "gpu_utilization", 75.0)

	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT device, hostname, metric_name, avg(metric_value) from gpu_telemetry")).
		WithArgs("nvidia0", "node1", model.MetricName("gpu_utilization")).
		WillReturnRows(rows)

	res, err := repo.GetGPUMetrics(
		context.Background(),
		"nvidia0", "node1",
		model.MetricName("gpu_utilization"),
		model.Aggregation("avg"),
		nil, nil, 10, 0,
	)

	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, "gpu_utilization", res[0].MetricName)
	assert.Equal(t, 75.0, res[0].MetricValue)
	assert.NotEmpty(t, res[0].UUID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListGPUs_QueryError(t *testing.T) {
	db, mock, repo := setupMockDB(t)
	defer db.Close()

	mock.ExpectQuery("SELECT DISTINCT device").WillReturnError(assert.AnError)

	_, _, err := repo.ListGPUs(context.Background(), 10, 0)
	assert.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetGPUTelemetry_WithTimeRange(t *testing.T) {
	db, mock, repo := setupMockDB(t)
	defer db.Close()

	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	rows := sqlmock.NewRows([]string{
		"id", "ts", "metric_name", "gpu_id", "device", "uuid", "model_name",
		"hostname", "container", "pod", "namespace", "metric_value",
		"labels_raw", "created_at", "total_count",
	}).AddRow(
		2, start, "memory_utilization", 1, "nvidia1", "uuid-2", "Tesla-V100",
		"node2", "cont2", "pod2", "ns2", 88.0,
		[]byte(`{"env":"prod"}`), end, 1,
	)

	// Expect query with ts >= $3 and ts <= $4
	mock.ExpectQuery("SELECT id, ts, metric_name").
		WithArgs("nvidia1", "node2", start, end, 10, 0).
		WillReturnRows(rows)

	res, total, err := repo.GetGPUTelemetry(
		context.Background(),
		"nvidia1", "node2",
		&start, &end,
		10, 0,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, res, 1)
	assert.Equal(t, "memory_utilization", res[0].MetricName)
	assert.Equal(t, 88.0, res[0].Value)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetGPUMetrics_WithTimeRange(t *testing.T) {
	db, mock, repo := setupMockDB(t)
	defer db.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := time.Now()

	rows := sqlmock.NewRows([]string{
		"device", "hostname", "metric_name", "metric_value",
	}).AddRow("nvidia1", "node2", "memory_utilization", 65.5)

	// Expect query with ts >= $4 and ts <= $5
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT device, hostname, metric_name, max(metric_value) from gpu_telemetry")).
		WithArgs("nvidia1", "node2", model.MetricName("memory_utilization"), start, end).
		WillReturnRows(rows)

	res, err := repo.GetGPUMetrics(
		context.Background(),
		"nvidia1", "node2",
		model.MetricName("memory_utilization"),
		model.Aggregation("max"),
		&start, &end,
		10, 0,
	)

	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, "memory_utilization", res[0].MetricName)
	assert.Equal(t, 65.5, res[0].MetricValue)
	assert.NoError(t, mock.ExpectationsWereMet())
}
