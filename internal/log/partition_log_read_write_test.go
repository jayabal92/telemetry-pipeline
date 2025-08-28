package log

import (
	"io"
	"sync"
	"testing"

	"go.uber.org/zap"
)

func TestAppendRead_RolloverAndLastOffset(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	pl, err := OpenPartition(dir, 128, logger) // small to force rollovers
	if err != nil {
		t.Fatalf("OpenPartition: %v", err)
	}
	defer pl.Close()

	// append 200 records to create many segments
	const N = 200
	first, last, err := pl.AppendBatch(mkBatch(N))
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if first != 0 {
		t.Fatalf("first should be 0 got %d", first)
	}
	if last != int64(N-1) {
		t.Fatalf("last expected %d got %d", N-1, last)
	}

	// read from a mid offset that crosses many segments
	recs, hw, err := pl.ReadFrom(100, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadFrom err: %v", err)
	}
	if int64(len(recs)) != last-100+1 {
		t.Fatalf("expected %d recs got %d", last-100+1, len(recs))
	}
	if hw != last {
		t.Fatalf("hw expected %d got %d", last, hw)
	}

	// read from last offset
	recs, hw, err = pl.ReadFrom(last, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadFrom err for last: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 rec at last offset got %d", len(recs))
	}
	if hw != last {
		t.Fatalf("hw mismatch at last")
	}

	// reopen and verify offsets restored
	if err := pl.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	pl2, err := OpenPartition(dir, 128, logger)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer pl2.Close()
	if pl2.NextOffset() != last+1 {
		t.Fatalf("nextOffset mismatch after reopen: %d vs %d", pl2.NextOffset(), last+1)
	}
	if pl2.HighWatermark() != last {
		t.Fatalf("hw mismatch after reopen: %d vs %d", pl2.HighWatermark(), last)
	}
}

func TestConcurrentProduceConsume(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dir := t.TempDir()
	pl, err := OpenPartition(dir, 4096, logger)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pl.Close()

	const producers = 4
	const batches = 20
	const perBatch = 25
	var wg sync.WaitGroup
	errCh := make(chan error, producers)

	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < batches; i++ {
				if _, _, err := pl.AppendBatch(mkBatch(perBatch)); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}
	// consumer
	total := producers * batches * perBatch
	seen := 0
	for seen < total {
		recs, _, err := pl.ReadFrom(0, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadFrom err: %v", err)
		}
		if len(recs) < seen {
			t.Fatalf("records decreased: was %d now %d", seen, len(recs))
		}
		seen = len(recs)
	}
	wg.Wait()
	for i := 0; i < producers; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("producer err: %v", err)
		}
	}
}
