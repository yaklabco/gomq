package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	// schemaVersion is written as a uint32 at byte 0 of each segment file.
	schemaVersion uint32 = 1
	// schemaSize is the number of bytes occupied by the schema version header.
	schemaSize = 4
	// ackEntrySize is the byte size of each position entry in an ack file.
	ackEntrySize = 4
)

// MessageStore manages message persistence across memory-mapped segment files.
// Messages are written to append-only segments; deletions (acknowledgements) are
// tracked in separate ack files per segment. Fully-acked segments are cleaned up.
type MessageStore struct {
	mu       sync.Mutex
	dir      string
	segSize  int64
	segments map[uint32]*MFile
	acks     map[uint32]*MFile
	deleted  map[uint32][]uint32 // sorted positions per segment
	msgCount map[uint32]uint32
	wfileID  uint32
	rfileID  uint32
	wfile    *MFile
	rfile    *MFile
	rpos     int64
	size     uint32
	bytesize uint64
	closed   bool
}

// OpenMessageStore opens or creates a message store in dir. segmentSize controls
// the mmap capacity of each segment file.
func OpenMessageStore(dir string, segmentSize int64) (*MessageStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create message store dir %s: %w", dir, err)
	}

	ms := &MessageStore{
		dir:      dir,
		segSize:  segmentSize,
		segments: make(map[uint32]*MFile),
		acks:     make(map[uint32]*MFile),
		deleted:  make(map[uint32][]uint32),
		msgCount: make(map[uint32]uint32),
	}

	if err := ms.loadSegments(); err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}

	if err := ms.loadAcks(); err != nil {
		return nil, fmt.Errorf("load acks: %w", err)
	}

	ms.loadStats()

	return ms, nil
}

// Push writes a message to the current write segment and returns its position.
// The segment is rotated when the current one is full.
func (ms *MessageStore) Push(msg *Message) (SegmentPosition, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.closed {
		return SegmentPosition{}, ErrClosed
	}

	byteSize := msg.ByteSize()

	// Rotate segment if the current one cannot fit this message.
	if ms.wfile.Size()+int64(byteSize) > ms.wfile.Capacity() {
		if err := ms.rotateSegment(byteSize); err != nil {
			return SegmentPosition{}, fmt.Errorf("rotate segment: %w", err)
		}
	}

	pos := ms.wfile.Size()
	buf := make([]byte, byteSize)
	msg.MarshalTo(buf)

	if _, err := ms.wfile.Write(buf); err != nil {
		return SegmentPosition{}, fmt.Errorf("write message to segment %d: %w", ms.wfileID, err)
	}

	sp := SegmentPosition{
		Segment:  ms.wfileID,
		Position: uint32(pos),
		Size:     uint32(byteSize),
	}

	ms.msgCount[ms.wfileID]++
	ms.size++
	ms.bytesize += uint64(byteSize)

	return sp, nil
}

// Shift reads and removes the next unacked message from the store.
// It returns false when the store is empty.
func (ms *MessageStore) Shift() (*Envelope, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.closed || ms.size == 0 {
		return nil, false
	}

	for {
		if ms.rpos >= ms.rfile.Size() {
			if !ms.advanceReadSegment() {
				return nil, false
			}
			continue
		}

		seg := ms.rfileID
		pos := ms.rpos

		env, msgSize, err := ms.readMessageAt(seg, pos)
		if err != nil {
			return nil, false
		}

		ms.rpos += int64(msgSize)

		if ms.isDeleted(seg, uint32(pos)) {
			continue
		}

		ms.size--
		ms.bytesize -= uint64(msgSize)

		return env, true
	}
}

// Delete marks a message as acknowledged by appending its position to the ack file.
// If all messages in a segment are acked, the segment and ack files are removed.
func (ms *MessageStore) Delete(sp SegmentPosition) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.closed {
		return ErrClosed
	}

	ackFile, err := ms.openAckFile(sp.Segment)
	if err != nil {
		return fmt.Errorf("open ack file for segment %d: %w", sp.Segment, err)
	}

	var posBuf [ackEntrySize]byte
	binary.LittleEndian.PutUint32(posBuf[:], sp.Position)
	if _, err := ackFile.Write(posBuf[:]); err != nil {
		return fmt.Errorf("write ack for segment %d position %d: %w", sp.Segment, sp.Position, err)
	}

	// Track in the in-memory deleted set.
	del := ms.deleted[sp.Segment]
	idx := sort.Search(len(del), func(i int) bool { return del[i] >= sp.Position })
	if idx < len(del) && del[idx] == sp.Position {
		return nil // already deleted
	}
	del = append(del, 0)
	copy(del[idx+1:], del[idx:])
	del[idx] = sp.Position
	ms.deleted[sp.Segment] = del

	// Clean up fully-acked segments (not the current write segment).
	if sp.Segment != ms.wfileID {
		ackCount := uint32(ackFile.Size() / ackEntrySize)
		if ackCount >= ms.msgCount[sp.Segment] {
			ms.cleanupSegment(sp.Segment)
		}
	}

	return nil
}

// GetMessage reads a message by its segment position without advancing the read cursor.
func (ms *MessageStore) GetMessage(sp SegmentPosition) (*BytesMessage, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.closed {
		return nil, ErrClosed
	}

	seg, ok := ms.segments[sp.Segment]
	if !ok {
		return nil, fmt.Errorf("segment %d not found", sp.Segment)
	}

	var msg *BytesMessage
	err := seg.ReadFunc(int64(sp.Position), int64(sp.Size), func(buf []byte) error {
		var readErr error
		msg, readErr = ReadBytesMessage(buf)
		return readErr
	})
	if err != nil {
		return nil, fmt.Errorf("read message at %s: %w", sp, err)
	}

	return msg, nil
}

// Purge removes up to max messages from the front of the store.
func (ms *MessageStore) Purge(limit int) int {
	count := 0
	for count < limit {
		env, ok := ms.Shift()
		if !ok {
			break
		}
		if err := ms.Delete(env.SegmentPosition); err != nil {
			break
		}
		count++
	}
	return count
}

// Close flushes and closes all segment and ack files.
func (ms *MessageStore) Close() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.closed {
		return nil
	}
	ms.closed = true

	var firstErr error
	for _, seg := range ms.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, ack := range ms.acks {
		if err := ack.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Len returns the number of unacked messages.
func (ms *MessageStore) Len() uint32 {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.size
}

// Empty returns true when the store has no unacked messages.
func (ms *MessageStore) Empty() bool {
	return ms.Len() == 0
}

// ByteSize returns the total byte size of unacked messages.
func (ms *MessageStore) ByteSize() uint64 {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.bytesize
}

// --- internal methods ---

func segmentFileName(id uint32) string {
	return fmt.Sprintf("msgs.%010d", id)
}

func ackFileName(id uint32) string {
	return fmt.Sprintf("acks.%010d", id)
}

func (ms *MessageStore) loadSegments() error {
	entries, err := os.ReadDir(ms.dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", ms.dir, err)
	}

	var ids []uint32
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "msgs.") && len(name) == 15 {
			idStr := name[5:]
			id, parseErr := strconv.ParseUint(idStr, 10, 32)
			if parseErr != nil {
				continue
			}
			ids = append(ids, uint32(id))
		}
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	if len(ids) == 0 {
		// Create the first segment.
		return ms.createSegment(1)
	}

	for i, id := range ids {
		path := filepath.Join(ms.dir, segmentFileName(id))
		isLast := i == len(ids)-1

		var mf *MFile
		if isLast {
			// Last segment is writable with full capacity.
			mf, err = OpenMFile(path, ms.segSize)
		} else {
			mf, err = OpenMFileReadOnly(path)
		}
		if err != nil {
			return fmt.Errorf("open segment %d: %w", id, err)
		}

		ms.segments[id] = mf
	}

	lastID := ids[len(ids)-1]
	firstID := ids[0]

	ms.wfileID = lastID
	ms.wfile = ms.segments[lastID]
	ms.rfileID = firstID
	ms.rfile = ms.segments[firstID]
	ms.rpos = schemaSize // skip schema header

	return nil
}

func (ms *MessageStore) createSegment(id uint32) error {
	path := filepath.Join(ms.dir, segmentFileName(id))

	mf, err := OpenMFile(path, ms.segSize)
	if err != nil {
		return fmt.Errorf("create segment %d: %w", id, err)
	}

	// Write schema version header.
	var header [schemaSize]byte
	binary.LittleEndian.PutUint32(header[:], schemaVersion)
	if _, err := mf.Write(header[:]); err != nil {
		return fmt.Errorf("write schema version to segment %d: %w", id, err)
	}

	ms.segments[id] = mf
	ms.wfileID = id
	ms.wfile = mf
	ms.rfileID = id
	ms.rfile = mf
	ms.rpos = schemaSize

	return nil
}

func (ms *MessageStore) rotateSegment(nextMsgSize int) error {
	nextID := ms.wfileID + 1
	path := filepath.Join(ms.dir, segmentFileName(nextID))

	capacity := ms.segSize
	needed := int64(nextMsgSize + schemaSize)
	if needed > capacity {
		capacity = needed
	}

	mf, err := OpenMFile(path, capacity)
	if err != nil {
		return fmt.Errorf("create segment %d: %w", nextID, err)
	}

	var header [schemaSize]byte
	binary.LittleEndian.PutUint32(header[:], schemaVersion)
	if _, err := mf.Write(header[:]); err != nil {
		return fmt.Errorf("write schema header to segment %d: %w", nextID, err)
	}

	ms.segments[nextID] = mf
	ms.wfileID = nextID
	ms.wfile = mf

	return nil
}

func (ms *MessageStore) loadAcks() error {
	entries, err := os.ReadDir(ms.dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", ms.dir, err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "acks.") || len(name) != 15 {
			continue
		}

		idStr := name[5:]
		id, parseErr := strconv.ParseUint(idStr, 10, 32)
		if parseErr != nil {
			continue
		}
		segID := uint32(id)

		// Check that the corresponding segment exists.
		if _, ok := ms.segments[segID]; !ok {
			// Orphaned ack file — remove it.
			_ = os.Remove(filepath.Join(ms.dir, name))
			continue
		}

		path := filepath.Join(ms.dir, name)
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("read ack file %s: %w", path, readErr)
		}

		var positions []uint32
		for i := 0; i+ackEntrySize <= len(data); i += ackEntrySize {
			pos := binary.LittleEndian.Uint32(data[i:])
			if pos == 0 {
				break // position 0 is invalid (schema header occupies byte 0)
			}
			positions = append(positions, pos)
		}
		sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })

		if len(positions) > 0 {
			ms.deleted[segID] = positions
		}

		// Open the ack file for future appends.
		ackMF, ackErr := ms.openAckFile(segID)
		if ackErr != nil {
			return fmt.Errorf("open ack mfile for segment %d: %w", segID, ackErr)
		}
		ms.acks[segID] = ackMF
	}

	return nil
}

func (ms *MessageStore) loadStats() {
	sortedIDs := ms.sortedSegmentIDs()

	for _, segID := range sortedIDs {
		mf := ms.segments[segID]
		pos := int64(schemaSize)
		fileSize := mf.Size()
		var count uint32

		for pos < fileSize {
			var msgSize int
			remaining := fileSize - pos
			err := mf.ReadFunc(pos, remaining, func(buf []byte) error {
				// Check for zero timestamp indicating end of valid data.
				if len(buf) >= 8 && binary.LittleEndian.Uint64(buf[:8]) == 0 {
					return errEndOfData
				}
				var skipErr error
				msgSize, skipErr = SkipMessage(buf)
				return skipErr
			})
			if err != nil {
				break
			}

			count++
			if !ms.isDeleted(segID, uint32(pos)) {
				ms.size++
				ms.bytesize += uint64(msgSize)
			}

			pos += int64(msgSize)
		}

		ms.msgCount[segID] = count

		// Resize segment to last valid position to prevent reading garbage
		// after a crash (matching LavinMQ's mfile.resize(pos) behavior).
		mf.Resize(pos)
	}

	// Set the read position to the first undeleted message.
	ms.rpos = int64(schemaSize)
}

func (ms *MessageStore) readMessageAt(seg uint32, pos int64) (*Envelope, int, error) {
	mf, ok := ms.segments[seg]
	if !ok {
		return nil, 0, fmt.Errorf("segment %d not found", seg)
	}

	remaining := mf.Size() - pos
	if remaining <= 0 {
		return nil, 0, fmt.Errorf("position %d beyond segment size %d", pos, mf.Size())
	}

	var bmsg *BytesMessage
	var msgSize int
	err := mf.ReadFunc(pos, remaining, func(buf []byte) error {
		var skipErr error
		msgSize, skipErr = SkipMessage(buf)
		if skipErr != nil {
			return skipErr
		}
		var readErr error
		bmsg, readErr = ReadBytesMessage(buf[:msgSize])
		return readErr
	})
	if err != nil {
		return nil, 0, fmt.Errorf("read message at segment %d pos %d: %w", seg, pos, err)
	}

	sp := SegmentPosition{
		Segment:  seg,
		Position: uint32(pos),
		Size:     uint32(msgSize),
	}

	return &Envelope{
		SegmentPosition: sp,
		Message:         bmsg,
	}, msgSize, nil
}

func (ms *MessageStore) advanceReadSegment() bool {
	ids := ms.sortedSegmentIDs()
	for _, id := range ids {
		if id > ms.rfileID {
			ms.rfileID = id
			ms.rfile = ms.segments[id]
			ms.rpos = schemaSize
			return true
		}
	}
	return false
}

func (ms *MessageStore) isDeleted(seg uint32, pos uint32) bool {
	del, ok := ms.deleted[seg]
	if !ok {
		return false
	}
	idx := sort.Search(len(del), func(i int) bool { return del[i] >= pos })
	return idx < len(del) && del[idx] == pos
}

func (ms *MessageStore) openAckFile(segID uint32) (*MFile, error) {
	if ack, ok := ms.acks[segID]; ok {
		return ack, nil
	}

	path := filepath.Join(ms.dir, ackFileName(segID))
	// Size ack file capacity: at most one ack per minimum-size message.
	capacity := (ms.segSize / int64(MinMessageSize)) * ackEntrySize
	const minAckCapacity = 4096
	if capacity < minAckCapacity {
		capacity = minAckCapacity
	}

	ack, err := OpenMFile(path, capacity)
	if err != nil {
		return nil, err
	}
	ms.acks[segID] = ack
	return ack, nil
}

func (ms *MessageStore) cleanupSegment(segID uint32) {
	if segID == ms.rfileID {
		ms.advanceReadSegment()
	}

	if ack, ok := ms.acks[segID]; ok {
		ack.Delete()
		_ = ack.Close()
		delete(ms.acks, segID)
	}

	if seg, ok := ms.segments[segID]; ok {
		seg.Delete()
		_ = seg.Close()
		delete(ms.segments, segID)
	}

	delete(ms.msgCount, segID)
	delete(ms.deleted, segID)
}

func (ms *MessageStore) sortedSegmentIDs() []uint32 {
	ids := make([]uint32, 0, len(ms.segments))
	for id := range ms.segments {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// errEndOfData is a sentinel used internally during segment scanning.
var errEndOfData = errors.New("end of valid data")
