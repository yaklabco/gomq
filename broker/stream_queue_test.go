package broker

import (
	"testing"
	"time"

	"github.com/yaklabco/gomq/storage"
)

func newStreamQueue(t *testing.T, name string) *Queue {
	t.Helper()
	args := map[string]interface{}{
		"x-queue-type": "stream",
	}
	dir := t.TempDir()
	queue, err := NewQueue(name, false, false, false, args, dir)
	if err != nil {
		t.Fatalf("NewQueue() error: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	return queue
}

func TestStreamQueue_PublishAndConsumeFromFirst(t *testing.T) {
	t.Parallel()
	queue := newStreamQueue(t, "stream-first-q")

	// Publish several messages.
	for range 5 {
		msg := &storage.Message{
			Timestamp:    time.Now().UnixMilli(),
			ExchangeName: "",
			RoutingKey:   "key",
			Properties:   storage.Properties{},
			BodySize:     uint64(len("msg")),
			Body:         []byte("msg"),
		}
		if _, err := queue.PublishSync(msg); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// Create a consumer offset starting from "first" (offset 0).
	offset := queue.StreamOffset("first")
	if offset != 0 {
		t.Errorf("StreamOffset(first) = %d, want 0", offset)
	}

	// Read all messages using the stream read path.
	count := 0
	for {
		env, nextOffset, ok := queue.StreamGet(offset)
		if !ok {
			break
		}
		if env.Message == nil {
			t.Fatal("StreamGet returned nil message")
		}
		offset = nextOffset
		count++
	}

	if count != 5 {
		t.Errorf("consumed %d messages from offset=first, want 5", count)
	}
}

func TestStreamQueue_ConsumeFromLast(t *testing.T) {
	t.Parallel()
	queue := newStreamQueue(t, "stream-last-q")

	// Publish 3 messages before consumer starts.
	for range 3 {
		msg := makeStorageMsg("", "key", "old")
		if _, err := queue.PublishSync(msg); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// "last" offset should be at the last published message.
	offset := queue.StreamOffset("last")
	if offset != 2 {
		t.Errorf("StreamOffset(last) = %d, want 2 (0-indexed, 3 messages)", offset)
	}

	// "next" should be beyond the last — only new messages after this point.
	nextOffset := queue.StreamOffset("next")

	// Publish 2 more messages after getting the offset.
	for range 2 {
		msg := makeStorageMsg("", "key", "new")
		if _, err := queue.PublishSync(msg); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// Reading from "last" includes the last existing message + new ones.
	countFromLast := 0
	readOffset := offset
	for {
		_, next, ok := queue.StreamGet(readOffset)
		if !ok {
			break
		}
		readOffset = next
		countFromLast++
	}
	if countFromLast != 3 {
		t.Errorf("consumed %d messages from offset=last, want 3 (last old + 2 new)", countFromLast)
	}

	// Reading from "next" should only get the 2 new messages.
	countFromNext := 0
	readOffset = nextOffset
	for {
		_, next, ok := queue.StreamGet(readOffset)
		if !ok {
			break
		}
		readOffset = next
		countFromNext++
	}
	if countFromNext != 2 {
		t.Errorf("consumed %d messages from offset=next, want 2", countFromNext)
	}
}

func TestStreamQueue_MultipleConsumersIndependentOffsets(t *testing.T) {
	t.Parallel()
	queue := newStreamQueue(t, "stream-multi-q")

	for range 10 {
		msg := makeStorageMsg("", "key", "data")
		if _, err := queue.PublishSync(msg); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// Consumer A reads from first.
	offsetA := queue.StreamOffset("first")
	countA := 0
	for {
		_, next, ok := queue.StreamGet(offsetA)
		if !ok {
			break
		}
		offsetA = next
		countA++
	}

	// Consumer B reads from first independently.
	offsetB := queue.StreamOffset("first")
	countB := 0
	for {
		_, next, ok := queue.StreamGet(offsetB)
		if !ok {
			break
		}
		offsetB = next
		countB++
	}

	if countA != 10 {
		t.Errorf("consumer A read %d messages, want 10", countA)
	}
	if countB != 10 {
		t.Errorf("consumer B read %d messages, want 10", countB)
	}
}

func TestStreamQueue_AckIsNoop(t *testing.T) {
	t.Parallel()
	queue := newStreamQueue(t, "stream-ack-q")

	msg := makeStorageMsg("", "key", "persistent")
	if _, err := queue.PublishSync(msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	// Read the message.
	offset := queue.StreamOffset("first")
	env, _, ok := queue.StreamGet(offset)
	if !ok {
		t.Fatal("StreamGet returned false")
	}

	// Ack should be a no-op for stream queues.
	if err := queue.Ack(env.SegmentPosition); err != nil {
		t.Fatalf("Ack() error: %v", err)
	}

	// Message should still be readable from offset=first.
	offset = queue.StreamOffset("first")
	_, _, ok = queue.StreamGet(offset)
	if !ok {
		t.Error("message not available after ack in stream queue (should be retained)")
	}
}

func TestStreamQueue_NumericOffset(t *testing.T) {
	t.Parallel()
	queue := newStreamQueue(t, "stream-num-q")

	for range 10 {
		msg := makeStorageMsg("", "key", "data")
		if _, err := queue.PublishSync(msg); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// Start reading from offset 5 (skip first 5 messages).
	offset := int64(5)
	count := 0
	for {
		_, next, ok := queue.StreamGet(offset)
		if !ok {
			break
		}
		offset = next
		count++
	}

	if count != 5 {
		t.Errorf("consumed %d messages from offset=5, want 5", count)
	}
}

func TestStreamQueue_Len(t *testing.T) {
	t.Parallel()
	queue := newStreamQueue(t, "stream-len-q")

	for range 5 {
		msg := makeStorageMsg("", "key", "data")
		if _, err := queue.PublishSync(msg); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// Len should reflect all messages (not reduced by reads).
	if queue.Len() != 5 {
		t.Errorf("Len() = %d, want 5", queue.Len())
	}

	// Reading should not decrease Len in stream mode.
	offset := queue.StreamOffset("first")
	queue.StreamGet(offset)

	if queue.Len() != 5 {
		t.Errorf("Len() after StreamGet = %d, want 5", queue.Len())
	}
}

func TestStreamQueue_IsStreamMode(t *testing.T) {
	t.Parallel()
	queue := newStreamQueue(t, "stream-mode-q")

	if !queue.IsStream() {
		t.Error("IsStream() = false, want true")
	}

	normalQueue := newTestQueue(t, "normal-q", nil)
	if normalQueue.IsStream() {
		t.Error("IsStream() = true for normal queue, want false")
	}
}
