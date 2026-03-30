package broker

import (
	"sync"
	"sync/atomic"

	"github.com/jamesainslie/gomq/storage"
)

// streamState holds per-queue state for stream mode (x-queue-type=stream).
// Stream queues behave as append-only logs: ack is a no-op, messages are
// never deleted by consumers, and each consumer tracks its own read offset.
type streamState struct {
	// messageCount is the total number of messages ever published to the stream.
	// It serves as the monotonically increasing offset counter.
	messageCount atomic.Int64

	// positions tracks the segment position for each logical offset.
	// Index i holds the SegmentPosition of message with offset i.
	mu        sync.Mutex
	positions []storage.SegmentPosition
}

// recordPosition appends the segment position for the next logical offset.
func (ss *streamState) recordPosition(sp storage.SegmentPosition) {
	ss.mu.Lock()
	offset := int64(len(ss.positions))
	ss.positions = append(ss.positions, sp)
	ss.mu.Unlock()
	ss.messageCount.Store(offset + 1)
}

// segmentPosition returns the SegmentPosition for the given logical offset.
func (ss *streamState) segmentPosition(offset int64) (storage.SegmentPosition, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if offset < 0 || offset >= int64(len(ss.positions)) {
		return storage.SegmentPosition{}, false
	}
	return ss.positions[offset], true
}

// lastOffset returns the offset of the most recently published message.
// Returns -1 if no messages have been published.
func (ss *streamState) lastOffset() int64 {
	return ss.messageCount.Load() - 1
}

// count returns the total number of messages in the stream.
func (ss *streamState) count() int64 {
	return ss.messageCount.Load()
}

// IsStream reports whether this queue is in stream mode.
func (q *Queue) IsStream() bool {
	return q.streamMode
}

// StreamOffset resolves a stream offset specifier to a numeric offset.
// Supported values: "first" (0), "last" (last published), "next" (last+1),
// or a numeric offset (int64).
func (q *Queue) StreamOffset(spec interface{}) int64 {
	if q.stream == nil {
		return 0
	}

	switch specVal := spec.(type) {
	case string:
		switch specVal {
		case "first":
			return 0
		case "last":
			last := q.stream.lastOffset()
			if last < 0 {
				return 0
			}
			return last
		case "next":
			return q.stream.count()
		default:
			return 0
		}
	case int64:
		return specVal
	case int:
		return int64(specVal)
	default:
		return 0
	}
}

// StreamGet reads a message at the given logical offset without removing it
// from the store. Returns the envelope, the next offset to read, and whether
// a message was available. This is the primary read path for stream consumers.
func (q *Queue) StreamGet(offset int64) (*storage.Envelope, int64, bool) {
	if q.stream == nil {
		return nil, offset, false
	}

	sp, ok := q.stream.segmentPosition(offset)
	if !ok {
		return nil, offset, false
	}

	msg, err := q.store.GetMessage(sp)
	if err != nil {
		return nil, offset, false
	}

	env := &storage.Envelope{
		SegmentPosition: sp,
		Message:         msg,
	}

	return env, offset + 1, true
}
