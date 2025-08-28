package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Segment struct {
	File       *os.File
	Writer     *bufio.Writer
	BaseOffset int64
	SizeBytes  int64
	MaxBytes   int64
	numRecords int64
}

func OpenSegment(path string, base int64, maxBytes int64) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	s := &Segment{
		File:       f,
		Writer:     bufio.NewWriterSize(f, 1<<20),
		BaseOffset: base,
		MaxBytes:   maxBytes,
	}
	fi, _ := f.Stat()
	s.SizeBytes = fi.Size()
	return s, nil
}

func (s *Segment) Append(record []byte) (int64, error) {
	// frame: 4-byte length + payload
	if int64(len(record))+s.SizeBytes+4 > s.MaxBytes {
		return -1, io.EOF
	}
	offset := s.BaseOffset + s.numRecords
	if err := binary.Write(s.Writer, binary.BigEndian, uint32(len(record))); err != nil {
		return -1, err
	}
	n, err := s.Writer.Write(record)
	if err != nil {
		return -1, err
	}
	s.SizeBytes += int64(4 + n)
	s.numRecords++
	return offset, nil
}

// Truncate shrinks the segment file to 'size' bytes and updates in-memory size.
// Assumes s.Writer wraps s.File; flushes the buffer before truncating.
func (s *Segment) Truncate(size int64) error {
	if s.Writer != nil {
		if err := s.Writer.Flush(); err != nil {
			return err
		}
	}
	// Seek + Truncate to the size requested
	if s.File != nil {
		if _, err := s.File.Seek(size, io.SeekStart); err != nil {
			return err
		}
		if err := s.File.Truncate(size); err != nil {
			return err
		}
	}
	s.SizeBytes = size
	// numRecords will be recomputed from index on next use; leave as-is
	return nil
}

func (s *Segment) NumRecords() int64 { return s.numRecords }

func (s *Segment) Flush() error { return s.Writer.Flush() }
func (s *Segment) Sync() error  { _ = s.Writer.Flush(); return s.File.Sync() }
func (s *Segment) Close() error { _ = s.Writer.Flush(); return s.File.Close() }

func (s *Segment) ReadAt(pos int64) ([]byte, int64, error) {
	// Guard against positions beyond current on-disk size
	if pos+4 > s.SizeBytes { // need at least the 4-byte length
		return nil, 0, io.EOF
	}
	if _, err := s.File.Seek(pos, io.SeekStart); err != nil {
		return nil, 0, err
	}
	var l uint32
	if err := binary.Read(s.File, binary.BigEndian, &l); err != nil {
		return nil, 0, err
	}
	if pos+4+int64(l) > s.SizeBytes {
		// length says we need more bytes than are present → treat as EOF/truncated
		return nil, 0, io.EOF
	}
	buf := make([]byte, int(l))
	n, err := io.ReadFull(s.File, buf)
	if err != nil {
		return nil, 0, err
	}
	return buf, int64(4 + n), nil
}

func (s *Segment) String() string {
	return fmt.Sprintf("segment(base=%d,size=%d,records=%d)", s.BaseOffset, s.SizeBytes, s.numRecords)
}
