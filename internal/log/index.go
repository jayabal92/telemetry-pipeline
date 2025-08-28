package log

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

const indexEntryBytes = 16 // int64(offset) + int64(pos)

type Index struct {
	File   *os.File
	Writer *bufio.Writer
}

func OpenIndex(path string) (*Index, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &Index{File: f, Writer: bufio.NewWriter(f)}, nil
}

func (i *Index) Append(offset int64, pos int64) error {
	if err := binary.Write(i.Writer, binary.BigEndian, offset); err != nil {
		return err
	}
	if err := binary.Write(i.Writer, binary.BigEndian, pos); err != nil {
		return err
	}
	return nil
}

// TruncateAtOffset drops all entries with offset >= cutoff.
// It scans using the existing iterator to count how many entries to keep,
// then truncates the file at keepCount*16.
func (i *Index) TruncateAtOffset(cutoff int64) error {
	if i.Writer != nil {
		if err := i.Writer.Flush(); err != nil {
			return err
		}
	}
	var keep int64
	it := i.Iterator()
	for {
		off, _, err := it()
		if err != nil {
			break
		}
		if off >= cutoff {
			break
		}
		keep++
	}
	newSize := keep * indexEntryBytes
	if i.File != nil {
		if _, err := i.File.Seek(newSize, io.SeekStart); err != nil {
			return err
		}
		if err := i.File.Truncate(newSize); err != nil {
			return err
		}
	}
	return nil
}

func (i *Index) Flush() error { return i.Writer.Flush() }
func (i *Index) Sync() error  { _ = i.Writer.Flush(); return i.File.Sync() }
func (i *Index) Close() error { _ = i.Writer.Flush(); return i.File.Close() }

// Count returns number of complete index entries (from file size, after a flush).
func (i *Index) Count() (int64, error) {
	if err := i.Writer.Flush(); err != nil { // ensure buffered writes visible
		return 0, err
	}
	fi, err := i.File.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size() / indexEntryBytes, nil
}

// LastOffset returns the highest offset in the index (after a flush). ok=false if empty.
func (i *Index) LastOffset() (last int64, ok bool, err error) {
	if err := i.Writer.Flush(); err != nil {
		return 0, false, err
	}
	fi, err := i.File.Stat()
	if err != nil {
		return 0, false, err
	}
	size := fi.Size()
	if size < indexEntryBytes {
		return 0, false, nil
	}
	if _, err := i.File.Seek(size-indexEntryBytes, io.SeekStart); err != nil {
		return 0, false, err
	}
	var off int64
	if err := binary.Read(i.File, binary.BigEndian, &off); err != nil {
		return 0, false, err
	}
	return off, true, nil
}

// Iterator yields (offset, pos, error). It tolerates EOF and returns io.EOF when done.
func (i *Index) Iterator() func() (int64, int64, error) {
	// make sure buffered data is visible to the reader
	if err := i.Writer.Flush(); err != nil {
		return func() (int64, int64, error) { return 0, 0, err }
	}
	if _, err := i.File.Seek(0, io.SeekStart); err != nil {
		return func() (int64, int64, error) { return 0, 0, err }
	}
	rd := bufio.NewReader(i.File)
	return func() (int64, int64, error) {
		var off, pos int64
		if err := binary.Read(rd, binary.BigEndian, &off); err != nil {
			// Any read error at this point should be treated as end of iteration.
			// (If there is a partial trailing entry, we also stop cleanly here.)
			return 0, 0, io.EOF
		}
		if err := binary.Read(rd, binary.BigEndian, &pos); err != nil {
			return 0, 0, io.EOF
		}
		return off, pos, nil
	}
}
