package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"

	"telemetry-pipeline/internal/model"
)

// ---- Mock pgx.Conn ----
type mockConn struct {
	copyErr  error
	execErr  error
	queryErr error
	offset   int64
}

func (m *mockConn) CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, rows pgx.CopyFromSource) (int64, error) {
	if m.copyErr != nil {
		return 0, m.copyErr
	}
	return int64(len(columns)), nil
}

func (m *mockConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if m.execErr != nil {
		return pgconn.CommandTag{}, m.execErr
	}
	return pgconn.CommandTag{}, nil
}

func (m *mockConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return mockRow{offset: m.offset, err: m.queryErr}
}

type mockRow struct {
	offset int64
	err    error
}

func (r mockRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	ptr := dest[0].(*int64)
	*ptr = r.offset
	return nil
}

// ---- UTs ----

func TestCopyInsert_Positive(t *testing.T) {
	mc := &mockConn{}
	batch := []model.Telemetry{{
		Timestamp:  time.Now(),
		MetricName: "utilization",
		GPUId:      1,
		Device:     "dev",
		UUID:       "uuid1",
		ModelName:  "RTX",
		Hostname:   "host1",
		Value:      75.5,
		LabelsRaw:  map[string]string{"k1": "v1"},
	}}

	// simulate success
	err := copyInsert(context.Background(), mc, batch)
	assert.NoError(t, err)
}

func TestCopyInsert_Negative(t *testing.T) {
	mc := &mockConn{copyErr: errors.New("copy failed")}
	batch := []model.Telemetry{{MetricName: "utilization"}}

	err := copyInsert(context.Background(), mc, batch)
	assert.Error(t, err)
}

func TestCommitOffset_Positive(t *testing.T) {
	mc := &mockConn{}
	commitOffset(context.Background(), mc, "g1", "topic", 0, 123)
	// No panic expected
}

func TestCommitOffset_Negative(t *testing.T) {
	mc := &mockConn{execErr: errors.New("exec failed")}
	commitOffset(context.Background(), mc, "g1", "topic", 0, 123)
	// Should log error but not panic
}

func TestLoadOffset_Positive(t *testing.T) {
	mc := &mockConn{offset: 42}
	got := loadOffset(context.Background(), mc, "g1", "topic", 0)
	assert.Equal(t, int64(42), got)
}

func TestLoadOffset_Negative(t *testing.T) {
	mc := &mockConn{queryErr: errors.New("no rows")}
	got := loadOffset(context.Background(), mc, "g1", "topic", 0)
	assert.Equal(t, int64(0), got)
}
