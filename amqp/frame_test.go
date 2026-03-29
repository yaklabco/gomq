package amqp

import (
	"bytes"
	"testing"
)

func TestMethodFrame_roundTrip(t *testing.T) {
	t.Parallel()

	method := &ConnectionTune{
		ChannelMax: 2047,
		FrameMax:   131072,
		Heartbeat:  60,
	}

	var buf bytes.Buffer
	wr := NewWriter(&buf, int(FrameMinSize))

	if err := wr.WriteMethod(0, method); err != nil {
		t.Fatalf("WriteMethod() error = %v", err)
	}
	if err := wr.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	rd := NewReader(&buf, 131072)
	frame, err := rd.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}

	mf, ok := frame.(*MethodFrame)
	if !ok {
		t.Fatalf("frame type = %T, want *MethodFrame", frame)
	}
	if mf.Channel != 0 {
		t.Errorf("Channel = %d, want 0", mf.Channel)
	}

	tune, ok := mf.Method.(*ConnectionTune)
	if !ok {
		t.Fatalf("method type = %T, want *ConnectionTune", mf.Method)
	}
	if tune.ChannelMax != 2047 {
		t.Errorf("ChannelMax = %d, want 2047", tune.ChannelMax)
	}
	if tune.FrameMax != 131072 {
		t.Errorf("FrameMax = %d, want 131072", tune.FrameMax)
	}
	if tune.Heartbeat != 60 {
		t.Errorf("Heartbeat = %d, want 60", tune.Heartbeat)
	}
}

func TestHeaderFrame_roundTrip(t *testing.T) {
	t.Parallel()

	props := &Properties{
		ContentType:  "application/json",
		DeliveryMode: 2,
	}

	var buf bytes.Buffer
	wr := NewWriter(&buf, int(FrameMinSize))

	if err := wr.WriteHeader(1, ClassBasic, 256, props); err != nil {
		t.Fatalf("WriteHeader() error = %v", err)
	}
	if err := wr.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	rd := NewReader(&buf, 131072)
	frame, err := rd.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}

	hf, ok := frame.(*HeaderFrame)
	if !ok {
		t.Fatalf("frame type = %T, want *HeaderFrame", frame)
	}
	if hf.Channel != 1 {
		t.Errorf("Channel = %d, want 1", hf.Channel)
	}
	if hf.ClassID != ClassBasic {
		t.Errorf("ClassID = %d, want %d", hf.ClassID, ClassBasic)
	}
	if hf.BodySize != 256 {
		t.Errorf("BodySize = %d, want 256", hf.BodySize)
	}
	if hf.Properties.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", hf.Properties.ContentType, "application/json")
	}
	if hf.Properties.DeliveryMode != 2 {
		t.Errorf("DeliveryMode = %d, want 2", hf.Properties.DeliveryMode)
	}
}

func TestBodyFrame_roundTrip(t *testing.T) {
	t.Parallel()

	body := []byte("hello AMQP world")

	var buf bytes.Buffer
	wr := NewWriter(&buf, int(FrameMinSize))

	if err := wr.WriteBody(1, body); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	if err := wr.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	rd := NewReader(&buf, 131072)
	frame, err := rd.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}

	bf, ok := frame.(*BodyFrame)
	if !ok {
		t.Fatalf("frame type = %T, want *BodyFrame", frame)
	}
	if bf.Channel != 1 {
		t.Errorf("Channel = %d, want 1", bf.Channel)
	}
	if !bytes.Equal(bf.Body, body) {
		t.Errorf("Body = %q, want %q", bf.Body, body)
	}
}

func TestHeartbeatFrame_roundTrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	wr := NewWriter(&buf, int(FrameMinSize))

	if err := wr.WriteHeartbeat(); err != nil {
		t.Fatalf("WriteHeartbeat() error = %v", err)
	}
	if err := wr.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	rd := NewReader(&buf, 131072)
	frame, err := rd.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}

	_, ok := frame.(*HeartbeatFrame)
	if !ok {
		t.Fatalf("frame type = %T, want *HeartbeatFrame", frame)
	}
}

func TestReadFrame_badFrameEnd(t *testing.T) {
	t.Parallel()

	// Construct a valid heartbeat frame but with wrong frame-end byte.
	// Heartbeat: type=8, channel=0, size=0, frame_end=0xCE
	frame := []byte{
		FrameHeartbeat, // type
		0x00, 0x00,     // channel
		0x00, 0x00, 0x00, 0x00, // size=0
		0xFF, // WRONG frame-end (should be 0xCE)
	}

	rd := NewReader(bytes.NewReader(frame), 131072)
	_, err := rd.ReadFrame()
	if err == nil {
		t.Error("expected error for bad frame-end, got nil")
	}
}

func TestBodyFrame_largeBody_multipleFrames(t *testing.T) {
	t.Parallel()

	// Create a body larger than the frame payload size.
	// With FrameMinSize=4096, the payload area is 4096 - 8 (header+end).
	// WriteBody should split into multiple frames.
	maxPayload := int(FrameMinSize) - frameHeaderSize - 1
	body := bytes.Repeat([]byte("x"), maxPayload*2+10)

	var buf bytes.Buffer
	wr := NewWriter(&buf, int(FrameMinSize))

	if err := wr.WriteBody(1, body); err != nil {
		t.Fatalf("WriteBody() error = %v", err)
	}
	if err := wr.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Read all body frames and reassemble. We read until EOF rather than
	// checking buf.Len() because the Reader wraps the source in a
	// bufio.Reader, which reads ahead.
	rd := NewReader(&buf, 131072)
	var reassembled []byte
	frameCount := 0

	for {
		frame, err := rd.ReadFrame()
		if err != nil {
			break // EOF or read error after all data consumed
		}
		bf, ok := frame.(*BodyFrame)
		if !ok {
			t.Fatalf("frame %d: type = %T, want *BodyFrame", frameCount, frame)
		}
		reassembled = append(reassembled, bf.Body...)
		frameCount++
	}

	if frameCount < 2 {
		t.Errorf("expected multiple frames for large body, got %d", frameCount)
	}
	if !bytes.Equal(reassembled, body) {
		t.Errorf("reassembled body length = %d, want %d", len(reassembled), len(body))
	}
}

func TestWriteDelivery_roundTrip(t *testing.T) {
	t.Parallel()

	deliver := &BasicDeliver{
		ConsumerTag: "ctag-1",
		DeliveryTag: 42,
		Redelivered: false,
		Exchange:    "amq.direct",
		RoutingKey:  "test.key",
	}
	props := &Properties{ContentType: "text/plain"}
	body := []byte("hello delivery")

	var buf bytes.Buffer
	wr := NewWriter(&buf, int(FrameMinSize))

	if err := wr.WriteDelivery(1, deliver, ClassBasic, uint64(len(body)), props, body); err != nil {
		t.Fatalf("WriteDelivery() error = %v", err)
	}

	rd := NewReader(&buf, 131072)

	// Frame 1: method (BasicDeliver).
	frame, err := rd.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() method error = %v", err)
	}
	mf, ok := frame.(*MethodFrame)
	if !ok {
		t.Fatalf("frame 1 type = %T, want *MethodFrame", frame)
	}
	got, ok := mf.Method.(*BasicDeliver)
	if !ok {
		t.Fatalf("method type = %T, want *BasicDeliver", mf.Method)
	}
	if got.ConsumerTag != "ctag-1" {
		t.Errorf("ConsumerTag = %q, want %q", got.ConsumerTag, "ctag-1")
	}
	if got.DeliveryTag != 42 {
		t.Errorf("DeliveryTag = %d, want 42", got.DeliveryTag)
	}

	// Frame 2: content header.
	frame, err = rd.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() header error = %v", err)
	}
	hf, ok := frame.(*HeaderFrame)
	if !ok {
		t.Fatalf("frame 2 type = %T, want *HeaderFrame", frame)
	}
	if hf.BodySize != uint64(len(body)) {
		t.Errorf("BodySize = %d, want %d", hf.BodySize, len(body))
	}
	if hf.Properties.ContentType != "text/plain" {
		t.Errorf("ContentType = %q, want %q", hf.Properties.ContentType, "text/plain")
	}

	// Frame 3: body.
	frame, err = rd.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() body error = %v", err)
	}
	bf, ok := frame.(*BodyFrame)
	if !ok {
		t.Fatalf("frame 3 type = %T, want *BodyFrame", frame)
	}
	if !bytes.Equal(bf.Body, body) {
		t.Errorf("Body = %q, want %q", bf.Body, body)
	}
}

func TestMethodFrame_onChannel(t *testing.T) {
	t.Parallel()

	method := &BasicPublish{
		Exchange:   "test",
		RoutingKey: "key",
		Mandatory:  true,
	}

	var buf bytes.Buffer
	wr := NewWriter(&buf, int(FrameMinSize))

	if err := wr.WriteMethod(5, method); err != nil {
		t.Fatalf("WriteMethod() error = %v", err)
	}
	if err := wr.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	rd := NewReader(&buf, 131072)
	frame, err := rd.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}

	mf, ok := frame.(*MethodFrame)
	if !ok {
		t.Fatalf("frame type = %T, want *MethodFrame", frame)
	}
	if mf.Channel != 5 {
		t.Errorf("Channel = %d, want 5", mf.Channel)
	}

	pub, ok := mf.Method.(*BasicPublish)
	if !ok {
		t.Fatalf("method type = %T, want *BasicPublish", mf.Method)
	}
	if pub.Exchange != "test" {
		t.Errorf("Exchange = %q, want %q", pub.Exchange, "test")
	}
	if !pub.Mandatory {
		t.Error("Mandatory = false, want true")
	}
}
