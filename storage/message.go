package storage

import (
	"encoding/binary"
	"fmt"
)

// MinMessageSize is the byte size of a message with empty exchange, empty routing key,
// and zero-length body: timestamp(8) + exLen(1) + rkLen(1) + flags(2) + bodySize(8) = 20.
const MinMessageSize = 20

// Message represents a message from a publisher, ready to be serialized to a segment.
type Message struct {
	Timestamp    int64
	ExchangeName string
	RoutingKey   string
	Properties   Properties
	BodySize     uint64
	Body         []byte
}

// Properties holds AMQP-style property flags and optional headers.
type Properties struct {
	Flags   uint16
	Headers map[string]any
}

// BytesMessage is a deserialized message read back from a segment.
// All string fields are copies (not slices of the mmap region).
type BytesMessage struct {
	Timestamp    int64
	ExchangeName string
	RoutingKey   string
	Properties   Properties
	BodySize     uint64
	Body         []byte
}

// Envelope wraps a message with its position in the store and redelivery status.
type Envelope struct {
	SegmentPosition SegmentPosition
	Message         *BytesMessage
	Redelivered     bool
}

// ByteSize returns the total serialized size of the message in bytes.
func (msg *Message) ByteSize() int {
	// timestamp(8) + exLen(1) + exchange(N) + rkLen(1) + routingKey(N) + flags(2) + bodySize(8) + body(N)
	return 8 + 1 + len(msg.ExchangeName) + 1 + len(msg.RoutingKey) + 2 + 8 + int(msg.BodySize) //nolint:gosec // body size bounded by max message size (128MB)
}

// MarshalTo serializes the message into buf using the LavinMQ binary format.
// buf must be at least ByteSize() bytes. Returns the number of bytes written.
//
// Binary format (little-endian unless noted):
//
//	timestamp(8) | ex_name_len(1) | ex_name(N) | rk_len(1) | rk(N) | properties_flags(2 BE) | body_size(8) | body(N)
func (msg *Message) MarshalTo(buf []byte) int {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(msg.Timestamp)) //nolint:gosec // timestamp reinterpretation, not a lossy conversion
	pos += 8

	buf[pos] = byte(len(msg.ExchangeName)) //nolint:gosec // AMQP shortstr max 255
	pos++
	pos += copy(buf[pos:], msg.ExchangeName)

	buf[pos] = byte(len(msg.RoutingKey)) //nolint:gosec // AMQP shortstr max 255
	pos++
	pos += copy(buf[pos:], msg.RoutingKey)

	// Properties flags are big-endian per AMQP spec.
	binary.BigEndian.PutUint16(buf[pos:], msg.Properties.Flags)
	pos += 2

	binary.LittleEndian.PutUint64(buf[pos:], msg.BodySize)
	pos += 8

	pos += copy(buf[pos:], msg.Body)

	return pos
}

// ReadBytesMessage deserializes a BytesMessage from buf.
// It copies all variable-length data out of buf so the result does not alias the input.
func ReadBytesMessage(buf []byte) (*BytesMessage, error) {
	if len(buf) < MinMessageSize {
		return nil, fmt.Errorf("buffer too small (%d < %d): %w", len(buf), MinMessageSize, ErrBoundsCheck)
	}

	pos := 0

	timestamp := int64(binary.LittleEndian.Uint64(buf[pos:])) //nolint:gosec // timestamp reinterpretation, not a lossy conversion
	pos += 8

	exLen := int(buf[pos])
	pos++
	if pos+exLen > len(buf) {
		return nil, fmt.Errorf("exchange name length %d exceeds buffer: %w", exLen, ErrBoundsCheck)
	}
	exchangeName := string(buf[pos : pos+exLen]) // string() copies
	pos += exLen

	rkLen := int(buf[pos])
	pos++
	if pos+rkLen > len(buf) {
		return nil, fmt.Errorf("routing key length %d exceeds buffer: %w", rkLen, ErrBoundsCheck)
	}
	routingKey := string(buf[pos : pos+rkLen])
	pos += rkLen

	if pos+2 > len(buf) {
		return nil, fmt.Errorf("buffer too small for properties flags: %w", ErrBoundsCheck)
	}
	flags := binary.BigEndian.Uint16(buf[pos:])
	pos += 2

	if pos+8 > len(buf) {
		return nil, fmt.Errorf("buffer too small for body size: %w", ErrBoundsCheck)
	}
	bodySize := binary.LittleEndian.Uint64(buf[pos:])
	pos += 8

	if pos+int(bodySize) > len(buf) { //nolint:gosec // body size bounded by max message size (128MB)
		return nil, fmt.Errorf("body size %d exceeds buffer: %w", bodySize, ErrBoundsCheck)
	}

	// Copy body so the result does not alias the mmap region.
	body := make([]byte, bodySize)
	copy(body, buf[pos:pos+int(bodySize)]) //nolint:gosec // body size bounded by max message size (128MB)

	return &BytesMessage{
		Timestamp:    timestamp,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Properties:   Properties{Flags: flags},
		BodySize:     bodySize,
		Body:         body,
	}, nil
}

// ReadBytesMessageZeroCopy deserializes a BytesMessage from buf without copying
// the body. String fields (exchange, routing key) are copied out as Go strings,
// but Body aliases buf directly. The caller must not use Body after buf is
// invalidated (e.g., after an mmap read lock is released).
func ReadBytesMessageZeroCopy(buf []byte) (*BytesMessage, error) {
	if len(buf) < MinMessageSize {
		return nil, fmt.Errorf("buffer too small (%d < %d): %w", len(buf), MinMessageSize, ErrBoundsCheck)
	}

	pos := 0

	timestamp := int64(binary.LittleEndian.Uint64(buf[pos:])) //nolint:gosec // timestamp reinterpretation, not a lossy conversion
	pos += 8

	exLen := int(buf[pos])
	pos++
	if pos+exLen > len(buf) {
		return nil, fmt.Errorf("exchange name length %d exceeds buffer: %w", exLen, ErrBoundsCheck)
	}
	exchangeName := string(buf[pos : pos+exLen]) // string() copies
	pos += exLen

	rkLen := int(buf[pos])
	pos++
	if pos+rkLen > len(buf) {
		return nil, fmt.Errorf("routing key length %d exceeds buffer: %w", rkLen, ErrBoundsCheck)
	}
	routingKey := string(buf[pos : pos+rkLen])
	pos += rkLen

	if pos+2 > len(buf) {
		return nil, fmt.Errorf("buffer too small for properties flags: %w", ErrBoundsCheck)
	}
	flags := binary.BigEndian.Uint16(buf[pos:])
	pos += 2

	if pos+8 > len(buf) {
		return nil, fmt.Errorf("buffer too small for body size: %w", ErrBoundsCheck)
	}
	bodySize := binary.LittleEndian.Uint64(buf[pos:])
	pos += 8

	if pos+int(bodySize) > len(buf) { //nolint:gosec // body size bounded by max message size (128MB)
		return nil, fmt.Errorf("body size %d exceeds buffer: %w", bodySize, ErrBoundsCheck)
	}

	// Zero-copy: body aliases the input buffer directly.
	body := buf[pos : pos+int(bodySize)] //nolint:gosec // body size bounded by max message size (128MB)

	return &BytesMessage{
		Timestamp:    timestamp,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Properties:   Properties{Flags: flags},
		BodySize:     bodySize,
		Body:         body,
	}, nil
}

// SkipMessage returns the total byte size of the next message in buf without fully
// deserializing it. This is used during segment recovery to count messages.
func SkipMessage(buf []byte) (int, error) {
	if len(buf) < MinMessageSize {
		return 0, fmt.Errorf("buffer too small (%d < %d): %w", len(buf), MinMessageSize, ErrBoundsCheck)
	}

	const (
		timestampLen = 8
		flagsLen     = 2
		bodySizeLen  = 8
	)

	pos := timestampLen // skip timestamp

	exLen := int(buf[pos])
	pos += 1 + exLen

	if pos >= len(buf) {
		return 0, fmt.Errorf("unexpected end of buffer after exchange name: %w", ErrBoundsCheck)
	}
	rkLen := int(buf[pos])
	pos += 1 + rkLen

	pos += flagsLen // skip properties flags

	if pos+bodySizeLen > len(buf) {
		return 0, fmt.Errorf("buffer too small for body size at offset %d: %w", pos, ErrBoundsCheck)
	}
	bodySize := binary.LittleEndian.Uint64(buf[pos:])
	pos += bodySizeLen + int(bodySize) //nolint:gosec // body size bounded by max message size (128MB)

	return pos, nil
}
