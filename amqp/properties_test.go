package amqp

import (
	"bytes"
	"testing"
)

func TestProperties_emptyRoundTrip(t *testing.T) {
	t.Parallel()

	original := &Properties{}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &Properties{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.Flags() != 0 {
		t.Errorf("Flags() = 0x%04X, want 0", decoded.Flags())
	}
}

func TestProperties_allFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	original := &Properties{
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		Headers:         Table{"key": "val"},
		DeliveryMode:    2,
		Priority:        5,
		CorrelationID:   "corr-123",
		ReplyTo:         "reply-queue",
		Expiration:      "60000",
		MessageID:       "msg-456",
		Timestamp:       1234567890,
		Type:            "my.event",
		UserID:          "guest",
		AppID:           "gomq-test",
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &Properties{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.ContentType != original.ContentType {
		t.Errorf("ContentType = %q, want %q", decoded.ContentType, original.ContentType)
	}
	if decoded.ContentEncoding != original.ContentEncoding {
		t.Errorf("ContentEncoding = %q, want %q", decoded.ContentEncoding, original.ContentEncoding)
	}
	hval, ok := decoded.Headers["key"].(string)
	if !ok || hval != "val" {
		t.Errorf("Headers[key] = %v, want %q", decoded.Headers["key"], "val")
	}
	if decoded.DeliveryMode != original.DeliveryMode {
		t.Errorf("DeliveryMode = %d, want %d", decoded.DeliveryMode, original.DeliveryMode)
	}
	if decoded.Priority != original.Priority {
		t.Errorf("Priority = %d, want %d", decoded.Priority, original.Priority)
	}
	if decoded.CorrelationID != original.CorrelationID {
		t.Errorf("CorrelationID = %q, want %q", decoded.CorrelationID, original.CorrelationID)
	}
	if decoded.ReplyTo != original.ReplyTo {
		t.Errorf("ReplyTo = %q, want %q", decoded.ReplyTo, original.ReplyTo)
	}
	if decoded.Expiration != original.Expiration {
		t.Errorf("Expiration = %q, want %q", decoded.Expiration, original.Expiration)
	}
	if decoded.MessageID != original.MessageID {
		t.Errorf("MessageID = %q, want %q", decoded.MessageID, original.MessageID)
	}
	if decoded.Timestamp != original.Timestamp {
		t.Errorf("Timestamp = %d, want %d", decoded.Timestamp, original.Timestamp)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type = %q, want %q", decoded.Type, original.Type)
	}
	if decoded.UserID != original.UserID {
		t.Errorf("UserID = %q, want %q", decoded.UserID, original.UserID)
	}
	if decoded.AppID != original.AppID {
		t.Errorf("AppID = %q, want %q", decoded.AppID, original.AppID)
	}
}

func TestProperties_partialFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	original := &Properties{
		ContentType:  "text/plain",
		DeliveryMode: 1,
		MessageID:    "abc",
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &Properties{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.ContentType != "text/plain" {
		t.Errorf("ContentType = %q, want %q", decoded.ContentType, "text/plain")
	}
	if decoded.DeliveryMode != 1 {
		t.Errorf("DeliveryMode = %d, want 1", decoded.DeliveryMode)
	}
	if decoded.MessageID != "abc" {
		t.Errorf("MessageID = %q, want %q", decoded.MessageID, "abc")
	}

	// Fields that were not set should remain zero.
	if decoded.ContentEncoding != "" {
		t.Errorf("ContentEncoding = %q, want empty", decoded.ContentEncoding)
	}
	if decoded.Priority != 0 {
		t.Errorf("Priority = %d, want 0", decoded.Priority)
	}
	if decoded.Timestamp != 0 {
		t.Errorf("Timestamp = %d, want 0", decoded.Timestamp)
	}
}

func TestProperties_flags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		props Properties
		want  uint16
	}{
		{
			name: "content-type only",
			props: Properties{
				ContentType: "text/plain",
			},
			want: flagContentType,
		},
		{
			name: "delivery-mode only",
			props: Properties{
				DeliveryMode: 2,
			},
			want: flagDeliveryMode,
		},
		{
			name: "multiple fields",
			props: Properties{
				ContentType: "application/json",
				Headers:     Table{"x": int32(1)},
				Priority:    3,
			},
			want: flagContentType | flagHeaders | flagPriority,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.props.Flags()
			if got != tt.want {
				t.Errorf("Flags() = 0x%04X, want 0x%04X", got, tt.want)
			}
		})
	}
}

func TestProperties_byteSize(t *testing.T) {
	t.Parallel()

	// Empty properties: just 2 bytes for flags.
	empty := &Properties{}
	if sz := empty.ByteSize(); sz != 2 {
		t.Errorf("empty ByteSize() = %d, want 2", sz)
	}

	// Properties with content-type "a/b" (3 chars): 2 (flags) + 1 (shortstr len) + 3 = 6.
	withCT := &Properties{ContentType: "a/b"}
	if sz := withCT.ByteSize(); sz != 6 {
		t.Errorf("with content-type ByteSize() = %d, want 6", sz)
	}
}
