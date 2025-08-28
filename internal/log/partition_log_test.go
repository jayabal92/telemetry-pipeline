package log

import (
	"encoding/json"
	"testing"

	"go.uber.org/zap"
)

// type dummyMsg struct {
// 	Key   string
// 	Value string
// }

func TestPartitionLogAppendBatchAndReadFrom_Rollover(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	segBytes := int64(128) // small to force rollover
	pl, err := OpenPartition(dir, segBytes, logger)
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	// defer pl.Close()

	// Build multiple messages to exceed a single segment
	var records [][]byte
	var msgs []dummyMsg
	for i := 0; i < 20; i++ {
		m := dummyMsg{Key: "k", Value: "val"}
		b, _ := json.Marshal(m)
		records = append(records, b)
		msgs = append(msgs, m)
	}

	first, last, err := pl.AppendBatch(records)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if first != 0 {
		t.Fatalf("expected first offset 0, got %d", first)
	}
	if last < int64(len(records))-1 {
		t.Fatalf("expected last >= %d, got %d", len(records)-1, last)
	}

	// Read back all records
	read, hw, err := pl.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if int64(len(read)) != (last - first + 1) {
		t.Fatalf("expected read count %d got %d", last-first+1, len(read))
	}
	if hw != pl.HighWatermark() {
		t.Fatalf("expected hw=%d got %d", pl.HighWatermark(), hw)
	}

	if err := pl.Close(); err != nil { // 🔑 add this
		t.Fatal(err)
	}
	// Ensure that after reopening partition, nextOffset/HighWatermark restored
	// Sync files: flush current segment/index
	for _, seg := range pl.segments {
		_ = seg.Flush()
		_ = seg.Sync()
	}
	for _, idx := range pl.indexes {
		_ = idx.Flush()
		_ = idx.Sync()
	}

	// Re-open by calling OpenPartition again (simulate restart)
	pl2, err := OpenPartition(dir, segBytes, logger)
	if err != nil {
		t.Fatalf("OpenPartition(reopen): %v", err)
	}
	defer pl2.Close()
	if pl2.NextOffset() != pl.NextOffset() {
		t.Fatalf("nextOffset mismatch after reopen: %d vs %d", pl2.NextOffset(), pl.NextOffset())
	}
	if pl2.HighWatermark() != pl.HighWatermark() {
		t.Fatalf("highWatermark mismatch after reopen: %d vs %d", pl2.HighWatermark(), pl.HighWatermark())
	}
}

func TestPartitionLogReadFromPastHighWatermarkReturnsEOF(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	pl, err := OpenPartition(dir, 1024*1024, logger)
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}

	records := [][]byte{
		[]byte(`{"k":"v"}`),
	}
	_, last, err := pl.AppendBatch(records)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	// Try to read starting after last
	_, hw, err := pl.ReadFrom(last+1, 10)
	if err == nil {
		// According to implementation, it returns io.EOF if offset>hw
		t.Fatalf("expected EOF when offset > hw, but err==nil, hw=%d", hw)
	}
}
