package amqp

import (
	"bytes"
	"testing"
)

func TestMethodByID_connectionStart(t *testing.T) {
	t.Parallel()

	method, err := MethodByID(MethodConnectionStart)
	if err != nil {
		t.Fatalf("MethodByID() error = %v", err)
	}
	if method.MethodID() != MethodConnectionStart {
		t.Errorf("MethodID() = 0x%08X, want 0x%08X", method.MethodID(), MethodConnectionStart)
	}
	if method.MethodName() != "connection.start" {
		t.Errorf("MethodName() = %q, want %q", method.MethodName(), "connection.start")
	}
}

func TestMethodByID_unknown(t *testing.T) {
	t.Parallel()

	_, err := MethodByID(0xFFFFFFFF)
	if err == nil {
		t.Error("expected error for unknown method ID, got nil")
	}
}

func TestMethodByID_allMethods(t *testing.T) {
	t.Parallel()

	ids := []uint32{
		MethodConnectionStart, MethodConnectionStartOk,
		MethodConnectionTune, MethodConnectionTuneOk,
		MethodConnectionOpen, MethodConnectionOpenOk,
		MethodConnectionClose, MethodConnectionCloseOk,
		MethodChannelOpen, MethodChannelOpenOk,
		MethodChannelFlow, MethodChannelFlowOk,
		MethodChannelClose, MethodChannelCloseOk,
	}
	for _, id := range ids {
		t.Run(methodName(id), func(t *testing.T) {
			t.Parallel()
			method, err := MethodByID(id)
			if err != nil {
				t.Fatalf("MethodByID(0x%08X) error = %v", id, err)
			}
			if method.MethodID() != id {
				t.Errorf("MethodID() = 0x%08X, want 0x%08X", method.MethodID(), id)
			}
		})
	}
}

func methodName(id uint32) string {
	method, err := MethodByID(id)
	if err != nil {
		return "unknown"
	}
	return method.MethodName()
}

func TestConnectionStart_roundTrip(t *testing.T) {
	t.Parallel()

	original := &ConnectionStart{
		VersionMajor:     0,
		VersionMinor:     9,
		ServerProperties: Table{"product": "gomq"},
		Mechanisms:       []byte("PLAIN"),
		Locales:          []byte("en_US"),
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &ConnectionStart{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.VersionMajor != original.VersionMajor {
		t.Errorf("VersionMajor = %d, want %d", decoded.VersionMajor, original.VersionMajor)
	}
	if decoded.VersionMinor != original.VersionMinor {
		t.Errorf("VersionMinor = %d, want %d", decoded.VersionMinor, original.VersionMinor)
	}
	if decoded.ServerProperties["product"] != "gomq" {
		t.Errorf("ServerProperties[product] = %v, want %q", decoded.ServerProperties["product"], "gomq")
	}
	if !bytes.Equal(decoded.Mechanisms, original.Mechanisms) {
		t.Errorf("Mechanisms = %q, want %q", decoded.Mechanisms, original.Mechanisms)
	}
	if !bytes.Equal(decoded.Locales, original.Locales) {
		t.Errorf("Locales = %q, want %q", decoded.Locales, original.Locales)
	}
}

func TestConnectionStartOk_roundTrip(t *testing.T) {
	t.Parallel()

	original := &ConnectionStartOk{
		ClientProperties: Table{"product": "test-client"},
		Mechanism:        "PLAIN",
		Response:         []byte("\x00guest\x00guest"),
		Locale:           "en_US",
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &ConnectionStartOk{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.Mechanism != original.Mechanism {
		t.Errorf("Mechanism = %q, want %q", decoded.Mechanism, original.Mechanism)
	}
	if decoded.Locale != original.Locale {
		t.Errorf("Locale = %q, want %q", decoded.Locale, original.Locale)
	}
	if !bytes.Equal(decoded.Response, original.Response) {
		t.Errorf("Response = %q, want %q", decoded.Response, original.Response)
	}
}

func TestConnectionTune_roundTrip(t *testing.T) {
	t.Parallel()

	original := &ConnectionTune{
		ChannelMax: 2047,
		FrameMax:   131072,
		Heartbeat:  60,
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &ConnectionTune{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.ChannelMax != original.ChannelMax {
		t.Errorf("ChannelMax = %d, want %d", decoded.ChannelMax, original.ChannelMax)
	}
	if decoded.FrameMax != original.FrameMax {
		t.Errorf("FrameMax = %d, want %d", decoded.FrameMax, original.FrameMax)
	}
	if decoded.Heartbeat != original.Heartbeat {
		t.Errorf("Heartbeat = %d, want %d", decoded.Heartbeat, original.Heartbeat)
	}
}

func TestConnectionClose_roundTrip(t *testing.T) {
	t.Parallel()

	original := &ConnectionClose{
		ReplyCode:       NotFound,
		ReplyText:       "NOT_FOUND - no exchange 'test'",
		FailingClassID:  ClassExchange,
		FailingMethodID: 10,
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &ConnectionClose{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.ReplyCode != original.ReplyCode {
		t.Errorf("ReplyCode = %d, want %d", decoded.ReplyCode, original.ReplyCode)
	}
	if decoded.ReplyText != original.ReplyText {
		t.Errorf("ReplyText = %q, want %q", decoded.ReplyText, original.ReplyText)
	}
	if decoded.FailingClassID != original.FailingClassID {
		t.Errorf("FailingClassID = %d, want %d", decoded.FailingClassID, original.FailingClassID)
	}
	if decoded.FailingMethodID != original.FailingMethodID {
		t.Errorf("FailingMethodID = %d, want %d", decoded.FailingMethodID, original.FailingMethodID)
	}
}

func TestConnectionOpen_roundTrip(t *testing.T) {
	t.Parallel()

	original := &ConnectionOpen{
		VirtualHost: "/test",
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &ConnectionOpen{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.VirtualHost != original.VirtualHost {
		t.Errorf("VirtualHost = %q, want %q", decoded.VirtualHost, original.VirtualHost)
	}
}

func TestChannelOpen_roundTrip(t *testing.T) {
	t.Parallel()

	original := &ChannelOpen{}
	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &ChannelOpen{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}
}

func TestChannelClose_roundTrip(t *testing.T) {
	t.Parallel()

	original := &ChannelClose{
		ReplyCode:       ChannelError,
		ReplyText:       "CHANNEL_ERROR - something went wrong",
		FailingClassID:  ClassBasic,
		FailingMethodID: 60,
	}

	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &ChannelClose{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.ReplyCode != original.ReplyCode {
		t.Errorf("ReplyCode = %d, want %d", decoded.ReplyCode, original.ReplyCode)
	}
	if decoded.ReplyText != original.ReplyText {
		t.Errorf("ReplyText = %q, want %q", decoded.ReplyText, original.ReplyText)
	}
}

func TestChannelFlow_roundTrip(t *testing.T) {
	t.Parallel()

	original := &ChannelFlow{Active: true}
	var buf bytes.Buffer
	if err := original.Write(&buf); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	decoded := &ChannelFlow{}
	if err := decoded.Read(&buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if decoded.Active != original.Active {
		t.Errorf("Active = %v, want %v", decoded.Active, original.Active)
	}
}

func TestMethodID_values(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   uint32
		want uint32
	}{
		{"connection.start", MethodConnectionStart, 10<<16 | 10},
		{"connection.start-ok", MethodConnectionStartOk, 10<<16 | 11},
		{"connection.tune", MethodConnectionTune, 10<<16 | 30},
		{"connection.tune-ok", MethodConnectionTuneOk, 10<<16 | 31},
		{"connection.open", MethodConnectionOpen, 10<<16 | 40},
		{"connection.open-ok", MethodConnectionOpenOk, 10<<16 | 41},
		{"connection.close", MethodConnectionClose, 10<<16 | 50},
		{"connection.close-ok", MethodConnectionCloseOk, 10<<16 | 51},
		{"channel.open", MethodChannelOpen, 20<<16 | 10},
		{"channel.open-ok", MethodChannelOpenOk, 20<<16 | 11},
		{"channel.flow", MethodChannelFlow, 20<<16 | 20},
		{"channel.flow-ok", MethodChannelFlowOk, 20<<16 | 21},
		{"channel.close", MethodChannelClose, 20<<16 | 40},
		{"channel.close-ok", MethodChannelCloseOk, 20<<16 | 41},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.id != tt.want {
				t.Errorf("Method %s ID = 0x%08X, want 0x%08X", tt.name, tt.id, tt.want)
			}
		})
	}
}
