package log

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestIndexAppendIteratorLastOffsetCount(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := OpenIndex(path)
	if err != nil {
		t.Fatalf("OpenIndex: %v", err)
	}
	defer idx.Close()

	// Append several offsets
	for off, pos := int64(10), int64(100); off < 15; off, pos = off+1, pos+10 {
		if err := idx.Append(off, pos); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	if err := idx.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Count
	cnt, err := idx.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != 5 {
		t.Fatalf("expected count 5, got %d", cnt)
	}

	// LastOffset
	last, ok, err := idx.LastOffset()
	if err != nil {
		t.Fatalf("LastOffset: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if last != 14 {
		t.Fatalf("expected last offset 14, got %d", last)
	}

	// Iterator: verify sequence
	it := idx.Iterator()
	var i int64 = 10
	for {
		off, pos, err := it()
		if err != nil {
			break
		}
		if off != i {
			t.Fatalf("expected off %d, got %d", i, off)
		}
		// Basic check that pos is non-negative
		if pos < 0 {
			t.Fatalf("invalid pos %d", pos)
		}
		i++
	}
}

func TestIndexFileStructure(t *testing.T) {
	// Validate index file contains big-endian int64 pairs
	dir := t.TempDir()
	path := filepath.Join(dir, "raw.idx")

	idx, err := OpenIndex(path)
	if err != nil {
		t.Fatalf("OpenIndex: %v", err)
	}
	defer idx.Close()

	if err := idx.Append(42, 99); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := idx.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Check bytes on disk
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(b) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(b))
	}
	off := int64(binary.BigEndian.Uint64(b[0:8]))
	pos := int64(binary.BigEndian.Uint64(b[8:16]))
	if off != 42 || pos != 99 {
		t.Fatalf("unexpected values: off=%d pos=%d", off, pos)
	}
}
