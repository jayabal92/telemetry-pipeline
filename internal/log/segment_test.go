package log

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"
)

func TestSegmentAppendReadAndEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg0.seg")

	// small max bytes so we can trigger EOF easily
	maxBytes := int64(64)
	s, err := OpenSegment(path, 0, maxBytes)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer s.Close()

	records := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("this is a longer record"),
	}

	// Append records until we get EOF or finish
	var offsets []int64
	for _, r := range records {
		off, err := s.Append(r)
		if err != nil {
			if err == io.EOF {
				// expected if record doesn't fit
				break
			}
			t.Fatalf("Append failed: %v", err)
		}
		offsets = append(offsets, off)
	}

	// Flush to ensure on-disk
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Read back each record using ReadAt via walking the file
	var pos int64 = 0
	for i := 0; i < len(offsets); i++ {
		rec, n, err := s.ReadAt(pos)
		if err != nil {
			t.Fatalf("ReadAt failed at pos %d: %v", pos, err)
		}
		if !bytes.Equal(rec, records[i]) {
			t.Fatalf("record mismatch: expected %q got %q", string(records[i]), string(rec))
		}
		pos += n
	}
}
