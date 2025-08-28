package log

import (
	"encoding/json"
	"io"
	"sync"
	"testing"

	"go.uber.org/zap"
)

type dummyMsg struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func mkBatch(n int) [][]byte {
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		b, _ := json.Marshal(dummyMsg{Key: "k", Value: "v"})
		out = append(out, b)
	}
	return out
}

func TestAppendRead_Basic(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	pl, err := OpenPartition(dir, 1<<20, logger)
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	defer pl.Close()

	batch := mkBatch(10)
	first, last, err := pl.AppendBatch(batch)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if got := last - first + 1; got != int64(len(batch)) {
		t.Fatalf("range mismatch, got=%d want=%d", got, len(batch))
	}
	recs, hw, err := pl.ReadFrom(first, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if int64(len(recs)) != (last - first + 1) {
		t.Fatalf("read count mismatch: got=%d want=%d", len(recs), last-first+1)
	}
	if hw != last {
		t.Fatalf("hw mismatch: got=%d want=%d", hw, last)
	}
}

func TestAppendRead_Rollover(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	// small segment to force 5 rollovers easily
	pl, err := OpenPartition(dir, 128, logger)
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	defer pl.Close()

	total := 100
	_, last, err := pl.AppendBatch(mkBatch(total))
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	got, hw, err := pl.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(got) != total {
		t.Fatalf("expected %d, got %d", total, len(got))
	}
	if hw != last {
		t.Fatalf("hw mismatch: got=%d want=%d", hw, last)
	}
}

func TestRead_MaxLimit(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	pl, err := OpenPartition(dir, 1<<20, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	_, last, err := pl.AppendBatch(mkBatch(50))
	if err != nil {
		t.Fatal(err)
	}
	recs, hw, err := pl.ReadFrom(10, 7)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(recs) != 7 {
		t.Fatalf("expected 7, got %d", len(recs))
	}
	if hw != last {
		t.Fatalf("hw mismatch")
	}
}

func TestRead_FromMiddle(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	pl, err := OpenPartition(dir, 256, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	_, last, err := pl.AppendBatch(mkBatch(30))
	if err != nil {
		t.Fatal(err)
	}
	recs, _, err := pl.ReadFrom(15, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(recs) != 15 {
		t.Fatalf("want 15, got %d", len(recs))
	}
	_ = last
}

func TestRead_OffsetBeyondHW_EOF(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	pl, err := OpenPartition(dir, 1<<20, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	first, last, err := pl.AppendBatch(mkBatch(5))
	if err != nil {
		t.Fatal(err)
	}
	if first != 0 || last != 4 {
		t.Fatalf("unexpected range [%d,%d]", first, last)
	}
	// ask beyond HW -> io.EOF
	_, hw, err := pl.ReadFrom(999, 0)
	if err == nil || err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if hw != last {
		t.Fatalf("hw mismatch: %d vs %d", hw, last)
	}
}

func TestReopen_RestoresOffsets(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	pl, err := OpenPartition(dir, 128, logger)
	if err != nil {
		t.Fatal(err)
	}
	_, last, err := pl.AppendBatch(mkBatch(40))
	if err != nil {
		t.Fatal(err)
	}
	// Close to persist
	if err := pl.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen
	pl2, err := OpenPartition(dir, 128, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer pl2.Close()

	if pl2.NextOffset() != last+1 {
		t.Fatalf("nextOffset mismatch: got=%d want=%d", pl2.NextOffset(), last+1)
	}
	if pl2.HighWatermark() != last {
		t.Fatalf("hw mismatch: got=%d want=%d", pl2.HighWatermark(), last)
	}
	// sanity: read all
	recs, hw, err := pl2.ReadFrom(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 40 {
		t.Fatalf("expected 40, got %d", len(recs))
	}
	if hw != last {
		t.Fatalf("hw mismatch after reopen")
	}
}

func TestAppendBatch_EmptyRejected(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	pl, err := OpenPartition(dir, 1<<20, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	if _, _, err := pl.AppendBatch(nil); err == nil {
		t.Fatalf("expected error for empty batch")
	}
}

func TestConcurrent_ProduceConsume(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	pl, err := OpenPartition(dir, 1024, logger) // small-ish to cause some rollovers
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	// 3 concurrent producers, 10 batches each, 20 records per batch
	var wg sync.WaitGroup
	prodErr := make(chan error, 3)
	total := 3 * 10 * 20

	wg.Add(3)
	for p := 0; p < 3; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				if _, _, err := pl.AppendBatch(mkBatch(20)); err != nil {
					prodErr <- err
					return
				}
			}
			prodErr <- nil
		}()
	}

	// Consumer polls until it sees all messages
	var seen int
	for seen < total {
		recs, _, err := pl.ReadFrom(0, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadFrom err: %v", err)
		}
		if len(recs) < seen {
			t.Fatalf("monotonicity broken: had %d now %d", seen, len(recs))
		}
		seen = len(recs)
	}

	wg.Wait()
	for i := 0; i < 3; i++ {
		if err := <-prodErr; err != nil {
			t.Fatalf("producer error: %v", err)
		}
	}
	if int64(seen) != pl.HighWatermark()+1 {
		t.Fatalf("seen=%d hw+1=%d", seen, pl.HighWatermark()+1)
	}
}
