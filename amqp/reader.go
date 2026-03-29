package amqp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// readerBufSize is the bufio.Reader buffer size. 64 KiB batches reads
// from the socket, reducing syscalls on high-throughput connections.
const readerBufSize = 65536

// Reader reads AMQP frames from an io.Reader.
// It enforces a maximum frame size and reuses a read buffer to minimize allocations.
type Reader struct {
	rd      *bufio.Reader
	hdr     [frameHeaderSize]byte
	buf     []byte // reusable payload buffer
	maxSize uint32 // max frame size (negotiated)
}

// NewReader creates a new frame reader with the given max frame size.
// Frames larger than maxSize are rejected.
func NewReader(r io.Reader, maxSize uint32) *Reader {
	if maxSize < FrameMinSize {
		maxSize = FrameMinSize
	}
	return &Reader{
		rd:      bufio.NewReaderSize(r, readerBufSize),
		buf:     make([]byte, 0, maxSize),
		maxSize: maxSize,
	}
}

// SetMaxSize updates the maximum frame size (e.g. after Tune negotiation).
func (r *Reader) SetMaxSize(maxSize uint32) {
	r.maxSize = maxSize
}

// ReadFrame reads the next complete frame from the connection.
func (r *Reader) ReadFrame() (Frame, error) {
	// Read the 7-byte frame header: type(1) + channel(2) + size(4).
	if _, err := io.ReadFull(r.rd, r.hdr[:]); err != nil {
		return nil, fmt.Errorf("read frame header: %w", err)
	}

	frameType := r.hdr[0]
	channel := binary.BigEndian.Uint16(r.hdr[1:3])
	payloadSize := binary.BigEndian.Uint32(r.hdr[3:frameHeaderSize])

	// Validate frame size before allocating.
	if payloadSize > r.maxSize {
		return nil, fmt.Errorf("frame too large: %d bytes exceeds max %d", payloadSize, r.maxSize)
	}

	// Reuse buffer, growing only when needed.
	if uint32(cap(r.buf)) < payloadSize {
		r.buf = make([]byte, payloadSize)
	}
	payload := r.buf[:payloadSize]

	if _, err := io.ReadFull(r.rd, payload); err != nil {
		return nil, fmt.Errorf("read frame payload (%d bytes): %w", payloadSize, err)
	}

	// Read and verify the frame-end marker.
	var endBuf [1]byte
	if _, err := io.ReadFull(r.rd, endBuf[:]); err != nil {
		return nil, fmt.Errorf("read frame-end: %w", err)
	}
	if endBuf[0] != FrameEnd {
		return nil, fmt.Errorf("bad frame-end marker: got 0x%02X, want 0x%02X", endBuf[0], FrameEnd)
	}

	return r.decodeFrame(frameType, channel, payload)
}

func (r *Reader) decodeFrame(frameType uint8, channel uint16, payload []byte) (Frame, error) {
	switch frameType {
	case FrameMethod:
		return r.decodeMethodFrame(channel, payload)
	case FrameHeader:
		return r.decodeHeaderFrame(channel, payload)
	case FrameBody:
		// Body frames: copy payload out since buffer is reused.
		body := make([]byte, len(payload))
		copy(body, payload)
		return &BodyFrame{Channel: channel, Body: body}, nil
	case FrameHeartbeat:
		return &HeartbeatFrame{}, nil
	default:
		return nil, fmt.Errorf("unknown frame type: %d", frameType)
	}
}

func (r *Reader) decodeMethodFrame(channel uint16, payload []byte) (Frame, error) {
	if len(payload) < sizeUint32 {
		return nil, fmt.Errorf("method frame payload too short: %d bytes", len(payload))
	}

	classID := binary.BigEndian.Uint16(payload[:sizeUint16])
	methodID := binary.BigEndian.Uint16(payload[sizeUint16:sizeUint32])
	combinedID := uint32(classID)<<classShift | uint32(methodID)

	method, err := MethodByID(combinedID)
	if err != nil {
		return nil, fmt.Errorf("decode method frame: %w", err)
	}

	methodPayload := bytes.NewReader(payload[sizeUint32:])
	if err := method.Read(methodPayload); err != nil {
		return nil, fmt.Errorf("decode method %s: %w", method.MethodName(), err)
	}

	return &MethodFrame{Channel: channel, Method: method}, nil
}

func (r *Reader) decodeHeaderFrame(channel uint16, payload []byte) (Frame, error) {
	// Header payload: class(2) + weight(2) + body_size(8) + property_flags(2) + property_fields.
	minHeaderPayload := sizeUint16 + sizeUint16 + sizeUint64 + sizeUint16
	if len(payload) < minHeaderPayload {
		return nil, fmt.Errorf("header frame payload too short: %d bytes", len(payload))
	}

	classID := binary.BigEndian.Uint16(payload[:sizeUint16])
	// weight at payload[2:4] is always 0 in AMQP 0-9-1, skip it.
	bodySize := binary.BigEndian.Uint64(payload[sizeUint32 : sizeUint32+sizeUint64])

	propsPayload := bytes.NewReader(payload[sizeUint32+sizeUint64:])
	var props Properties
	if err := props.Read(propsPayload); err != nil {
		return nil, fmt.Errorf("decode header properties: %w", err)
	}

	return &HeaderFrame{
		Channel:    channel,
		ClassID:    classID,
		BodySize:   bodySize,
		Properties: props,
	}, nil
}
