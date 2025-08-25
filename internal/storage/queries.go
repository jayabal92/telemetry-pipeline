package storage

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type TelemetryRow struct {
	GPUID       string
	TS          time.Time
	SMUtil      *int
	MemUtil     *int
	Temperature *int
	Hostname    *string
	Model       *string
}

func (s *Store) InsertTelemetryBatch(ctx context.Context, rows []TelemetryRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(`INSERT INTO gpu_metrics (gpu_id, ts, sm_util, mem_util, temperature, hostname, model)
VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			r.GPUID, r.TS, r.SMUtil, r.MemUtil, r.Temperature, r.Hostname, r.Model,
		)
	}
	br := s.DB.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < len(rows); i++ {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ListGPUs(ctx context.Context) ([]string, error) {
	rows, err := s.DB.Query(ctx, `SELECT DISTINCT gpu_id FROM gpu_metrics ORDER BY gpu_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) GetTelemetry(ctx context.Context, gpuID string, start, end *time.Time, limit int) ([]TelemetryRow, error) {
	q := `SELECT gpu_id, ts, sm_util, mem_util, temperature, hostname, model
FROM gpu_metrics WHERE gpu_id=$1`
	args := []any{gpuID}
	if start != nil {
		q += ` AND ts >= $` + itoa(len(args)+1)
		args = append(args, *start)
	}
	if end != nil {
		q += ` AND ts <= $` + itoa(len(args)+1)
		args = append(args, *end)
	}
	q += ` ORDER BY ts DESC`
	if limit > 0 {
		q += ` LIMIT ` + itoa(limit)
	}

	rows, err := s.DB.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TelemetryRow{}
	for rows.Next() {
		var r TelemetryRow
		if err := rows.Scan(&r.GPUID, &r.TS, &r.SMUtil, &r.MemUtil, &r.Temperature, &r.Hostname, &r.Model); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func itoa(i int) string { return fmtInt(i) }

// tiny inline to avoid importing strconv everywhere
func fmtInt(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var b [20]byte
	bp := len(b)
	for i > 0 {
		bp--
		b[bp] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		bp--
		b[bp] = '-'
	}
	return string(b[bp:])
}
