package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"go.uber.org/zap"
)

type PartitionLog struct {
	logger        *zap.Logger
	Dir           string
	SegmentBytes  int64
	mu            sync.RWMutex
	segments      []*Segment
	indexes       []*Index
	baseOffsets   []int64
	nextOffset    int64 // next offset to assign
	highWatermark int64 // last committed offset
}

type ReadRecord struct {
	Off  int64
	Data []byte
}

func OpenPartition(dir string, segmentBytes int64, logger *zap.Logger) (*PartitionLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	pl := &PartitionLog{Dir: dir, SegmentBytes: segmentBytes, highWatermark: -1, logger: logger}
	if err := pl.load(); err != nil {
		return nil, err
	}
	if len(pl.segments) == 0 {
		if err := pl.roll(0); err != nil {
			return nil, err
		}
	}
	// Run lightweight recovery
	if err := pl.RecoverLastSegment(); err != nil {
		return nil, err
	}
	return pl, nil
}

func (p *PartitionLog) NextOffset() int64 {
	return p.nextOffset
}

// HighWatermark returns the last committed offset (nextOffset - 1).
func (p *PartitionLog) HighWatermark() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.highWatermark
}

// func (p *PartitionLog) load() error {
// 	files, err := filepath.Glob(filepath.Join(p.Dir, "*.seg"))
// 	if err != nil {
// 		return err
// 	}
// 	sort.Strings(files)

// 	for _, f := range files {
// 		var base int64
// 		fmt.Sscanf(filepath.Base(f), "%d.seg", &base)

// 		seg, err := OpenSegment(f, base, p.SegmentBytes)
// 		if err != nil {
// 			return err
// 		}
// 		idxPath := filepath.Join(p.Dir, fmt.Sprintf("%d.idx", base))
// 		idx, err := OpenIndex(idxPath)
// 		if err != nil {
// 			seg.Close()
// 			return err
// 		}

// 		// restore numRecords from index (Count uses a flush to ensure visibility)
// 		count, err := idx.Count()
// 		if err != nil {
// 			seg.Close()
// 			idx.Close()
// 			return err
// 		}
// 		seg.numRecords = count

// 		p.segments = append(p.segments, seg)
// 		p.indexes = append(p.indexes, idx)
// 		p.baseOffsets = append(p.baseOffsets, base)
// 	}

// 	// Recover highWatermark and nextOffset from the indexes.
// 	// Choose the maximum offset across all index files (most robust).
// 	if len(p.segments) > 0 {
// 		var maxOff int64 = -1
// 		for _, idx := range p.indexes {
// 			if off, ok, err := idx.LastOffset(); err == nil && ok {
// 				if off > maxOff {
// 					maxOff = off
// 				}
// 			}
// 		}
// 		if maxOff >= 0 {
// 			p.highWatermark = maxOff
// 			p.nextOffset = maxOff + 1
// 		} else {
// 			// No index entries found in any index: start from last segment's base
// 			lastSeg := p.segments[len(p.segments)-1]
// 			p.highWatermark = -1
// 			p.nextOffset = lastSeg.BaseOffset
// 		}
// 	} else {
// 		// brand new log
// 		p.highWatermark = -1
// 		p.nextOffset = 0
// 	}
// 	fmt.Printf("OpenPartition: discovered nextOffset=%d highWatermark=%d segments=%d\n",
// 		p.nextOffset, p.highWatermark, len(p.segments))

// 	return nil
// }

func (p *PartitionLog) load() error {
	// Find segment files (named <base>.seg) and sort by numeric base offset.
	files, err := filepath.Glob(filepath.Join(p.Dir, "*.seg"))
	if err != nil {
		return err
	}

	type fileEntry struct {
		path string
		base int64
	}
	var entries []fileEntry

	// reset existing slices
	p.segments = nil
	p.indexes = nil
	p.baseOffsets = nil

	for _, f := range files {
		var base int64
		// parse base from filename; ignore files that don't match
		if _, err := fmt.Sscanf(filepath.Base(f), "%d.seg", &base); err != nil {
			continue
		}
		entries = append(entries, fileEntry{path: f, base: base})
	}

	// sort by numeric base ascending
	sort.Slice(entries, func(i, j int) bool { return entries[i].base < entries[j].base })

	// open segments & indexes in numeric order
	for _, e := range entries {
		seg, err := OpenSegment(e.path, e.base, p.SegmentBytes)
		if err != nil {
			return err
		}
		idxPath := filepath.Join(p.Dir, fmt.Sprintf("%d.idx", e.base))
		idx, err := OpenIndex(idxPath)
		if err != nil {
			_ = seg.Close()
			return err
		}

		count, err := idx.Count()
		if err != nil {
			_ = seg.Close()
			_ = idx.Close()
			return err
		}
		seg.numRecords = count

		p.segments = append(p.segments, seg)
		p.indexes = append(p.indexes, idx)
		p.baseOffsets = append(p.baseOffsets, e.base)
	}

	// Validate index/segment pairs
	truncated := false
	if len(p.segments) > 0 {
		for si := 0; si < len(p.segments); si++ {
			seg := p.segments[si]
			idx := p.indexes[si]

			if err := idx.Flush(); err != nil {
				return err
			}

			it := idx.Iterator()
			var lastGoodOff int64 = -1
			var lastGoodEndPos int64 = 0

			for {
				off, pos, err := it()
				if err != nil {
					break // finished this index
				}
				rec, nbytes, rerr := seg.ReadAt(pos)
				if rerr != nil {
					p.logger.Warn("validation: index points past segment data; truncating",
						zap.Int("segmentIndex", si),
						zap.Int64("indexOffset", off),
						zap.Int64("lastGoodOffset", lastGoodOff),
						zap.Int64("lastGoodEndPos", lastGoodEndPos),
						zap.Error(rerr))
					if terr := p.truncateAt(si, lastGoodOff, lastGoodEndPos); terr != nil {
						return terr
					}
					truncated = true
					break
				}
				lastGoodOff = off
				lastGoodEndPos = pos + int64(4+len(rec))
				_ = nbytes // just to silence unused warning
			}
			if truncated {
				break // stop validating after truncation
			}
		}
	}

	// Recompute highWatermark and nextOffset
	if len(p.segments) > 0 {
		var maxOff int64 = -1
		for _, idx := range p.indexes {
			off, ok, err := idx.LastOffset()
			if err != nil {
				return err
			}
			if ok && off > maxOff {
				maxOff = off
			}
		}
		if maxOff >= 0 {
			p.highWatermark = maxOff
			p.nextOffset = maxOff + 1
		} else {
			lastSeg := p.segments[len(p.segments)-1]
			p.highWatermark = -1
			p.nextOffset = lastSeg.BaseOffset
		}
	} else {
		p.highWatermark = -1
		p.nextOffset = 0
	}

	p.logger.Info("OpenPartition loaded",
		zap.Int64("nextOffset", p.nextOffset),
		zap.Int64("highWatermark", p.highWatermark),
		zap.Int("segments", len(p.segments)),
	)

	return nil
}

func (p *PartitionLog) roll(base int64) error {
	// Keep previous segments/indexes open so readers can continue to access them.
	// Create/open a new segment & index and append them to slices.
	segPath := filepath.Join(p.Dir, fmt.Sprintf("%d.seg", base))
	idxPath := filepath.Join(p.Dir, fmt.Sprintf("%d.idx", base))
	seg, err := OpenSegment(segPath, base, p.SegmentBytes)
	if err != nil {
		return err
	}
	idx, err := OpenIndex(idxPath)
	if err != nil {
		seg.Close()
		return err
	}
	p.segments = append(p.segments, seg)
	p.indexes = append(p.indexes, idx)
	p.baseOffsets = append(p.baseOffsets, base)
	return nil
}

// func (p *PartitionLog) AppendBatch(records [][]byte) (firstOffset int64, lastOffset int64, err error) {
// 	p.mu.Lock()
// 	defer p.mu.Unlock()

// 	if len(p.segments) == 0 || len(p.indexes) == 0 {
// 		if err := p.roll(p.nextOffset); err != nil {
// 			return 0, 0, err
// 		}
// 	}

// 	seg := p.segments[len(p.segments)-1]
// 	idx := p.indexes[len(p.indexes)-1]

// 	for i, rec := range records {
// 		offset := p.nextOffset

// 		// Get position BEFORE write (in case of roll, recompute later)
// 		startPos := seg.SizeBytes

// 		// Try append to current segment
// 		_, err := seg.Append(rec)
// 		if err != nil {
// 			if err == io.EOF {
// 				// current segment full: flush current seg/index, roll and append to new segment.
// 				base := p.nextOffset
// 				if err := seg.Flush(); err != nil {
// 					return 0, 0, err
// 				}
// 				if err := idx.Flush(); err != nil {
// 					return 0, 0, err
// 				}
// 				// Create new segment/index (do not close previous here)
// 				if err := p.roll(base); err != nil {
// 					return 0, 0, err
// 				}

// 				// Switch to the newly created segment/index
// 				seg = p.segments[len(p.segments)-1]
// 				idx = p.indexes[len(p.indexes)-1]

// 				// recompute startPos for the new segment
// 				startPos = seg.SizeBytes

// 				// Now append into new segment
// 				_, err = seg.Append(rec)
// 				if err != nil {
// 					return 0, 0, err
// 				}
// 			} else {
// 				return 0, 0, err
// 			}
// 		}

// 		if i == 0 {
// 			firstOffset = offset
// 		}
// 		lastOffset = offset

// 		// Append index entry for the current index at the correct startPos.
// 		if err := idx.Append(offset, startPos); err != nil {
// 			return 0, 0, err
// 		}

// 		// Update meta
// 		p.nextOffset++
// 		p.highWatermark = lastOffset
// 	}

// 	// Flush and sync ALL indexes and segments to ensure durability.
// 	for _, seg := range p.segments {
// 		if err := seg.Flush(); err != nil {
// 			return 0, 0, err
// 		}
// 		if err := seg.Sync(); err != nil {
// 			return 0, 0, err
// 		}
// 	}
// 	for _, idx := range p.indexes {
// 		if err := idx.Flush(); err != nil {
// 			return 0, 0, err
// 		}
// 		if err := idx.Sync(); err != nil {
// 			return 0, 0, err
// 		}
// 	}

// 	return firstOffset, lastOffset, nil
// }

// // ReadFrom reads records starting at `offset`, up to `max` messages.
// // Always returns the real high watermark, even if no records found.
// func (p *PartitionLog) ReadFrom(offset int64, max int) ([]ReadRecord, int64, error) {
// 	p.mu.RLock()
// 	defer p.mu.RUnlock()

// 	var res []ReadRecord
// 	hw := p.highWatermark

// 	if offset > hw {
// 		return res, hw, io.EOF
// 	}

// 	for si, seg := range p.segments {
// 		idx := p.indexes[si]
// 		// Ensure buffered index entries are visible to the reader.
// 		// _ = idx.Flush()
// 		it := idx.Iterator()
// 		for {
// 			off, pos, err := it()
// 			if err != nil {
// 				if errors.Is(err, io.EOF) {
// 					// stop reading this segment: possible partial write at the tail
// 					fmt.Println("EOF error:", err, off, pos)
// 					break
// 				}
// 				fmt.Println("iterator error:", err)
// 				break // finished this index
// 			}
// 			if off < offset {
// 				continue
// 			}
// 			rec, _, err := seg.ReadAt(pos)
// 			if err != nil {
// 				// skip truncated/invalid entries rather than failing the whole read
// 				fmt.Println("partition read error:", err)
// 				break
// 			}
// 			res = append(res, ReadRecord{Off: off, Data: rec})
// 			if max > 0 && len(res) >= max {
// 				return res, hw, nil
// 			}
// 		}
// 	}

// 	return res, hw, nil
// }

// AppendBatch appends a batch of records atomically from the reader's POV.
// Visibility rule:
//  1. Write all records to their target segment(s) and capture positions.
//  2. Flush the written segment(s) to durable storage.
//  3. Append corresponding index entries and flush the index(es).
//
// Thus, readers will never observe an index entry that points to bytes
// that are not visible on disk.
//
// Returns [firstOffset, lastOffset].
func (p *PartitionLog) AppendBatch(records [][]byte) (int64, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(records) == 0 {
		return 0, 0, fmt.Errorf("AppendBatch: empty batch")
	}
	if len(p.segments) == 0 {
		if err := p.roll(0); err != nil {
			return 0, 0, err
		}
	}

	type recLoc struct {
		idx    *Index
		seg    *Segment
		off    int64
		pos    int64
		segIdx int // for debugging
	}

	firstOffset := p.nextOffset
	var lastOffset int64
	var written []recLoc

	curSeg := p.segments[len(p.segments)-1]
	curIdx := p.indexes[len(p.indexes)-1]

	for _, rec := range records {
		// retryCurrent:
		// record offset to assign
		off := p.nextOffset

		// starting position BEFORE write
		startPos := curSeg.SizeBytes

		// attempt append
		_, err := curSeg.Append(rec)
		if err != nil {
			// io.EOF from Segment.Append means segment capacity reached; roll
			if err == io.EOF {
				// flush/sync current before rolling
				if e := curSeg.Flush(); e != nil {
					return 0, 0, e
				}
				if e := curSeg.Sync(); e != nil {
					return 0, 0, e
				}
				if e := curIdx.Flush(); e != nil {
					return 0, 0, e
				}
				if e := curIdx.Sync(); e != nil {
					return 0, 0, e
				}
				// New segment base is nextOffset (the offset we were about to write)
				if e := p.roll(p.nextOffset); e != nil {
					return 0, 0, e
				}
				curSeg = p.segments[len(p.segments)-1]
				curIdx = p.indexes[len(p.indexes)-1]
				// retry on the fresh segment
				startPos = curSeg.SizeBytes
				_, err = curSeg.Append(rec)
				if err != nil {
					return 0, 0, err
				}
			} else {
				return 0, 0, err
			}
		}

		// capture location (index append will happen after we flush the segment)
		written = append(written, recLoc{
			idx:    curIdx,
			seg:    curSeg,
			off:    off,
			pos:    startPos,
			segIdx: len(p.segments) - 1,
		})

		// bump offsets
		lastOffset = off
		p.nextOffset++
	}

	// 1) Flush+Sync all segments that got new bytes in this batch
	seenSeg := make(map[*Segment]struct{}, 2)
	for _, w := range written {
		if _, ok := seenSeg[w.seg]; ok {
			continue
		}
		if err := w.seg.Flush(); err != nil {
			return 0, 0, err
		}
		if err := w.seg.Sync(); err != nil {
			return 0, 0, err
		}
		seenSeg[w.seg] = struct{}{}
	}

	// 2) Append all index entries
	for _, w := range written {
		if err := w.idx.Append(w.off, w.pos); err != nil {
			return 0, 0, err
		}
	}

	// 3) Flush+Sync indexes that received entries
	seenIdx := make(map[*Index]struct{}, 2)
	for _, w := range written {
		if _, ok := seenIdx[w.idx]; ok {
			continue
		}
		if err := w.idx.Flush(); err != nil {
			return 0, 0, err
		}
		if err := w.idx.Sync(); err != nil {
			return 0, 0, err
		}
		seenIdx[w.idx] = struct{}{}
	}

	// Update high watermark to the last offset of the batch
	p.highWatermark = lastOffset

	return firstOffset, lastOffset, nil
}

// AppendBatch appends a batch of records.
// Guarantees:
//   - segment bytes for the batch are flushed before corresponding index entries are made visible.
//   - supports mid-batch rollovers.
//   - updates nextOffset and highWatermark.
func (p *PartitionLog) AppendBatch_bk(records [][]byte) (firstOffset int64, lastOffset int64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(records) == 0 {
		return 0, 0, fmt.Errorf("AppendBatch: empty batch")
	}
	if len(p.segments) == 0 {
		if err := p.roll(p.nextOffset); err != nil {
			return 0, 0, err
		}
	}

	curSeg := p.segments[len(p.segments)-1]
	curIdx := p.indexes[len(p.indexes)-1]

	firstOffset = p.nextOffset
	for _, rec := range records {
		startPos := curSeg.SizeBytes
		_, err := curSeg.Append(rec)
		if err == io.EOF {
			// rollover
			_ = curSeg.Flush()
			_ = curIdx.Flush()
			if err := p.roll(p.nextOffset); err != nil {
				return 0, 0, err
			}
			curSeg = p.segments[len(p.segments)-1]
			curIdx = p.indexes[len(p.indexes)-1]
			startPos = curSeg.SizeBytes
			if _, err = curSeg.Append(rec); err != nil {
				return 0, 0, err
			}
		} else if err != nil {
			return 0, 0, err
		}

		// index every record
		if err := curIdx.Append(p.nextOffset, startPos); err != nil {
			return 0, 0, err
		}

		lastOffset = p.nextOffset
		p.nextOffset++
	}

	_ = curSeg.Flush()
	_ = curIdx.Flush()
	p.highWatermark = lastOffset
	return firstOffset, lastOffset, nil
}

// ReadFrom reads records starting at 'offset' up to 'max' (0 = no limit).
// Returns the records, the current high watermark, and an error.
// Contract:
//   - If offset > high watermark, returns (nil, hw, io.EOF).
//   - Otherwise returns available records up to max across segments.
//   - Never surfaces spurious EOFs due to visibility races; it flushes the
//     active segment before scanning indexes.
//
// Concurrency: compatible with AppendBatch; holds a read-lock.
func (p *PartitionLog) ReadFrom_bk(offset int64, max int) ([]ReadRecord, int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	hw := p.highWatermark
	if offset < 0 {
		offset = 0
	}
	if offset > hw {
		return nil, hw, io.EOF
	}
	if len(p.segments) == 0 {
		return nil, hw, nil
	}

	// Start scanning from the segment that could contain 'offset'
	startIdx := 0
	for i := len(p.baseOffsets) - 1; i >= 0; i-- {
		if p.baseOffsets[i] <= offset {
			startIdx = i
			break
		}
	}

	// var res [][]byte
	var res []ReadRecord
	for si := startIdx; si < len(p.segments); si++ {
		seg := p.segments[si]
		idx := p.indexes[si]

		// Ensure bytes are visible before scanning the index
		_ = seg.Flush()
		_ = idx.Flush()

		it := idx.Iterator()
		for {
			off, pos, err := it()
			if err != nil {
				// iterator finished (including io.EOF)
				break
			}
			if off < offset {
				continue
			}
			// Bounds: do not read beyond current HW
			if off > hw {
				// This should not happen if HW is set post-index flush,
				// but guard anyway.
				break
			}
			rec, _, err := seg.ReadAt(pos)
			if err != nil {
				// If reading the active tail and the entry is not fully durable,
				// stop scanning this segment (older segments must be consistent).
				// Don't fail whole ReadFrom on a single bad entry.
				break
			}
			res = append(res, ReadRecord{Off: off, Data: rec})
			if max > 0 && len(res) >= max {
				return res, hw, nil
			}
		}
	}

	return res, hw, nil
}

// ReadFrom reads records starting at offset up to max (0 = no limit).
// It uses index files to find positions for offsets, ensuring correctness even
// across segment boundaries. If a segment index points to a truncated/partial
// record, the function returns whatever it could read; caller sees the HW.
func (p *PartitionLog) ReadFrom(offset int64, max int) ([]ReadRecord, int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	hw := p.highWatermark
	if offset < 0 {
		offset = 0
	}
	if offset > hw {
		p.logger.Debug("readfrom: offset beyond HW",
			zap.Int64("offset", offset),
			zap.Int64("hw", p.highWatermark))
		return nil, hw, io.EOF
	}

	var res []ReadRecord
	remaining := max

	// find the segment that may contain offset
	startSeg := 0
	for i := len(p.baseOffsets) - 1; i >= 0; i-- {
		if p.baseOffsets[i] <= offset {
			startSeg = i
			break
		}
	}

	for si := startSeg; si < len(p.segments); si++ {
		seg := p.segments[si]
		idx := p.indexes[si]

		// Ensure buffered bytes are visible to readers
		_ = seg.Flush()
		_ = idx.Flush()

		it := idx.Iterator()
		for {
			off, pos, err := it()
			if err != nil {
				// iterator finished
				break
			}
			if off < offset {
				continue
			}
			// do not read beyond HW
			if off > hw {
				return res, hw, nil
			}
			rec, _, rerr := seg.ReadAt(pos)
			if rerr != nil {
				// truncated or partial record — stop here and return
				p.logger.Warn("segment read at index pos failed", zap.Int64("off", off), zap.Any("pos", pos), zap.Error(rerr))
				return res, hw, nil
			}
			res = append(res, ReadRecord{Off: off, Data: rec})
			if remaining > 0 {
				remaining--
				if remaining == 0 {
					return res, hw, nil
				}
			}
		}
	}

	return res, hw, nil
}

// RecoverLastSegment only scans the active (last) segment for partial/corrupted
// records. All older segments are assumed durable and left untouched.
func (p *PartitionLog) RecoverLastSegment() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.segments) == 0 {
		p.nextOffset = 0
		p.highWatermark = -1
		return nil
	}

	lastIdx := len(p.segments) - 1
	seg := p.segments[lastIdx]
	idx := p.indexes[lastIdx]

	_ = idx.Flush()

	var lastGoodOff int64 = -1
	var lastGoodEndPos int64 = 0

	it := idx.Iterator()
	for {
		off, pos, err := it()
		if err != nil {
			break
		}

		rec, _, rerr := seg.ReadAt(pos)
		if rerr == io.EOF {
			// Partial write at this offset → truncate here.
			return p.truncateAt(lastIdx, lastGoodOff, lastGoodEndPos)
		}
		if rerr != nil {
			// Corrupted record → truncate here too.
			return p.truncateAt(lastIdx, lastGoodOff, lastGoodEndPos)
		}

		// Valid record
		lastGoodOff = off
		lastGoodEndPos = pos + int64(4+len(rec))
	}

	// If everything valid, recompute watermarks
	if lastGoodOff >= 0 {
		p.highWatermark = lastGoodOff
		p.nextOffset = lastGoodOff + 1
	} else {
		// Empty log
		p.highWatermark = -1
		p.nextOffset = 0
	}

	return nil
}

// truncateAt truncates the log at the given segment index and byte position,
// drops index entries at/after cutoffOffset, and removes all later segments/indexes.
func (p *PartitionLog) truncateAt(segIdx int, lastGoodOff int64, segCutPos int64) error {
	// If we never saw a good record in this run (lastGoodOff == -1),
	// truncate everything from the first segment.
	if lastGoodOff < 0 {
		for i := range p.segments {
			_ = p.segments[i].Truncate(0)
			_ = p.indexes[i].TruncateAtOffset(0)
		}
		// Drop all but the first (keep files, empty).
		if len(p.segments) > 1 {
			p.segments = p.segments[:1]
			p.indexes = p.indexes[:1]
		}
		p.highWatermark = -1
		p.nextOffset = 0
		return nil
	}

	// 1) Truncate the segment that contains lastGoodOff to exactly end-of-last-good.
	if err := p.segments[segIdx].Truncate(segCutPos); err != nil {
		return err
	}

	// 2) Truncate its index to drop entries >= (lastGoodOff+1).
	cutoff := lastGoodOff + 1
	if err := p.indexes[segIdx].TruncateAtOffset(cutoff); err != nil {
		return err
	}

	// 3) Remove (or empty) all later segments/indexes.
	for i := segIdx + 1; i < len(p.segments); i++ {
		_ = p.segments[i].Truncate(0)
		_ = p.indexes[i].TruncateAtOffset(0)
	}
	// Keep the files but discard them from the in-memory set to preserve a single active tail,
	// or, if you prefer, leave them and mark them empty. Here we drop references:
	p.segments = p.segments[:segIdx+1]
	p.indexes = p.indexes[:segIdx+1]

	// 4) Fix counters.
	p.highWatermark = lastGoodOff
	p.nextOffset = lastGoodOff + 1
	return nil
}

func (p *PartitionLog) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for _, seg := range p.segments {
		if e := seg.Flush(); e != nil && firstErr == nil {
			firstErr = e
		}
		if e := seg.Sync(); e != nil && firstErr == nil {
			firstErr = e
		}
		if e := seg.Close(); e != nil && firstErr == nil {
			firstErr = e
		}
	}
	for _, idx := range p.indexes {
		if e := idx.Flush(); e != nil && firstErr == nil {
			firstErr = e
		}
		if e := idx.Sync(); e != nil && firstErr == nil {
			firstErr = e
		}
		if e := idx.Close(); e != nil && firstErr == nil {
			firstErr = e
		}
	}
	return firstErr
}
