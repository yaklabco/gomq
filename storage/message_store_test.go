package storage

import (
	"bytes"
	"testing"
)

func makeMsg(exchange, routingKey, body string) *Message {
	return &Message{
		Timestamp:    1711929600000,
		ExchangeName: exchange,
		RoutingKey:   routingKey,
		Properties:   Properties{},
		BodySize:     uint64(len(body)),
		Body:         []byte(body),
	}
}

func openTestStore(t *testing.T, segSize int64) *MessageStore {
	t.Helper()
	dir := t.TempDir()
	store, err := OpenMessageStore(dir, segSize)
	if err != nil {
		t.Fatalf("OpenMessageStore() error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestMessageStore_PushShiftRoundtrip(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	msg := makeMsg("amq.direct", "test.key", "hello world")

	sp, err := store.Push(msg)
	if err != nil {
		t.Fatalf("Push() error: %v", err)
	}
	if sp.Segment == 0 {
		t.Error("Push() returned zero segment")
	}

	if store.Len() != 1 {
		t.Errorf("Len() = %d, want 1", store.Len())
	}

	env, ok := store.Shift()
	if !ok {
		t.Fatal("Shift() returned false, want true")
	}
	if env.Message.ExchangeName != "amq.direct" {
		t.Errorf("ExchangeName = %q, want %q", env.Message.ExchangeName, "amq.direct")
	}
	if !bytes.Equal(env.Message.Body, []byte("hello world")) {
		t.Errorf("Body = %q, want %q", env.Message.Body, "hello world")
	}

	if store.Len() != 0 {
		t.Errorf("Len() after Shift = %d, want 0", store.Len())
	}
}

func TestMessageStore_FIFOOrder(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	bodies := []string{"first", "second", "third"}
	for _, body := range bodies {
		if _, err := store.Push(makeMsg("ex", "rk", body)); err != nil {
			t.Fatalf("Push(%q) error: %v", body, err)
		}
	}

	for _, want := range bodies {
		env, ok := store.Shift()
		if !ok {
			t.Fatalf("Shift() returned false, want %q", want)
		}
		if string(env.Message.Body) != want {
			t.Errorf("Shift() body = %q, want %q", env.Message.Body, want)
		}
	}

	if _, ok := store.Shift(); ok {
		t.Error("Shift() should return false on empty store")
	}
}

func TestMessageStore_DeleteSkipsMessage(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	sp1, err := store.Push(makeMsg("ex", "rk", "msg1"))
	if err != nil {
		t.Fatalf("Push() error: %v", err)
	}
	if _, err := store.Push(makeMsg("ex", "rk", "msg2")); err != nil {
		t.Fatalf("Push() error: %v", err)
	}

	if err := store.Delete(sp1); err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	env, ok := store.Shift()
	if !ok {
		t.Fatal("Shift() returned false, want true")
	}
	if string(env.Message.Body) != "msg2" {
		t.Errorf("Shift() body = %q, want %q (msg1 should be deleted)", env.Message.Body, "msg2")
	}
}

func TestMessageStore_SegmentRotation(t *testing.T) {
	t.Parallel()

	// Use a small segment size to force rotation.
	store := openTestStore(t, 256)

	positions := make([]SegmentPosition, 0, 20)
	for i := range 20 {
		msg := makeMsg("ex", "rk", "payload-"+string(rune('A'+i)))
		sp, err := store.Push(msg)
		if err != nil {
			t.Fatalf("Push() error at i=%d: %v", i, err)
		}
		positions = append(positions, sp)
	}

	// We should have multiple segments.
	segments := make(map[uint32]bool)
	for _, sp := range positions {
		segments[sp.Segment] = true
	}
	if len(segments) < 2 {
		t.Errorf("expected multiple segments, got %d", len(segments))
	}

	// All messages should still be readable in order.
	for i := range 20 {
		env, ok := store.Shift()
		if !ok {
			t.Fatalf("Shift() returned false at i=%d", i)
		}
		want := "payload-" + string(rune('A'+i))
		if string(env.Message.Body) != want {
			t.Errorf("Shift() body = %q, want %q", env.Message.Body, want)
		}
	}
}

func TestMessageStore_Reopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Open, push, close.
	store, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatalf("OpenMessageStore() error: %v", err)
	}
	if _, err := store.Push(makeMsg("ex", "rk", "persisted")); err != nil {
		t.Fatalf("Push() error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Reopen.
	store2, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatalf("OpenMessageStore() reopen error: %v", err)
	}
	defer func() { _ = store2.Close() }()

	if store2.Len() != 1 {
		t.Errorf("reopened Len() = %d, want 1", store2.Len())
	}

	env, ok := store2.Shift()
	if !ok {
		t.Fatal("Shift() on reopened store returned false")
	}
	if string(env.Message.Body) != "persisted" {
		t.Errorf("Body = %q, want %q", env.Message.Body, "persisted")
	}
}

func TestMessageStore_Purge(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	for i := range 5 {
		if _, err := store.Push(makeMsg("ex", "rk", "msg"+string(rune('0'+i)))); err != nil {
			t.Fatalf("Push() error: %v", err)
		}
	}

	purged := store.Purge(3)
	if purged != 3 {
		t.Errorf("Purge(3) = %d, want 3", purged)
	}
	if store.Len() != 2 {
		t.Errorf("Len() after Purge(3) = %d, want 2", store.Len())
	}
}

func TestMessageStore_EmptyShift(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	if _, ok := store.Shift(); ok {
		t.Error("Shift() on empty store returned true, want false")
	}
}

func TestMessageStore_GetMessage(t *testing.T) {
	t.Parallel()
	store := openTestStore(t, 4096)

	sp, err := store.Push(makeMsg("ex", "rk", "random-access"))
	if err != nil {
		t.Fatalf("Push() error: %v", err)
	}

	got, err := store.GetMessage(sp)
	if err != nil {
		t.Fatalf("GetMessage() error: %v", err)
	}
	if string(got.Body) != "random-access" {
		t.Errorf("GetMessage() body = %q, want %q", got.Body, "random-access")
	}
}
