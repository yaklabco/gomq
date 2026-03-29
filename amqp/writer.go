package amqp

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// Writer writes AMQP frames to an io.Writer.
type Writer struct {
	bw           *bufio.Writer
	maxFrameSize int
	scratch      []byte
}

// NewWriter creates a new frame writer with the given maximum frame size.
func NewWriter(w io.Writer, maxFrameSize int) *Writer {
	return &Writer{
		bw:           bufio.NewWriter(w),
		maxFrameSize: maxFrameSize,
		scratch:      make([]byte, maxFrameSize),
	}
}

// WriteMethod writes a method frame for the given channel.
func (w *Writer) WriteMethod(channel uint16, method Method) error {
	// Encode the method payload: class(2) + method(2) + method_fields.
	id := method.MethodID()
	classID := uint16(id >> classShift)
	methodID := uint16(id & methodMask)

	// Write class and method IDs to scratch buffer.
	binary.BigEndian.PutUint16(w.scratch[:sizeUint16], classID)
	binary.BigEndian.PutUint16(w.scratch[sizeUint16:sizeUint32], methodID)

	// Write method fields into a temporary buffer.
	var methodBuf limitedBuffer
	if err := method.Write(&methodBuf); err != nil {
		return fmt.Errorf("encode method %s: %w", method.MethodName(), err)
	}

	payloadSize := sizeUint32 + methodBuf.Len()
	copy(w.scratch[sizeUint32:], methodBuf.Bytes())

	return w.writeFrame(FrameMethod, channel, w.scratch[:payloadSize])
}

// WriteHeader writes a content header frame.
func (w *Writer) WriteHeader(channel, classID uint16, bodySize uint64, props *Properties) error {
	// Header payload: class(2) + weight(2) + body_size(8) + properties.
	off := 0
	binary.BigEndian.PutUint16(w.scratch[off:], classID)
	off += sizeUint16
	binary.BigEndian.PutUint16(w.scratch[off:], 0) // weight, always 0
	off += sizeUint16
	binary.BigEndian.PutUint64(w.scratch[off:], bodySize)
	off += sizeUint64

	// Write properties into a temporary buffer.
	var propsBuf limitedBuffer
	if err := props.Write(&propsBuf); err != nil {
		return fmt.Errorf("encode header properties: %w", err)
	}

	copy(w.scratch[off:], propsBuf.Bytes())
	off += propsBuf.Len()

	return w.writeFrame(FrameHeader, channel, w.scratch[:off])
}

// WriteBody writes one or more body frames, splitting if the body exceeds
// the maximum frame payload size.
func (w *Writer) WriteBody(channel uint16, body []byte) error {
	maxPayload := w.maxFrameSize - frameHeaderSize - 1 // -1 for frame-end marker

	for len(body) > 0 {
		chunk := body
		if len(chunk) > maxPayload {
			chunk = body[:maxPayload]
		}

		if err := w.writeFrame(FrameBody, channel, chunk); err != nil {
			return fmt.Errorf("write body frame: %w", err)
		}
		body = body[len(chunk):]
	}
	return nil
}

// WriteHeartbeat writes a heartbeat frame (channel=0, size=0).
func (w *Writer) WriteHeartbeat() error {
	return w.writeFrame(FrameHeartbeat, 0, nil)
}

// Flush flushes the buffered writer.
func (w *Writer) Flush() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("flush frame writer: %w", err)
	}
	return nil
}

// writeFrame writes a complete frame: header + payload + frame-end.
func (w *Writer) writeFrame(frameType uint8, channel uint16, payload []byte) error {
	// Frame header: type(1) + channel(2) + size(4).
	var hdr [frameHeaderSize]byte
	hdr[0] = frameType
	binary.BigEndian.PutUint16(hdr[1:3], channel)
	binary.BigEndian.PutUint32(hdr[3:frameHeaderSize], uint32(len(payload)))

	if _, err := w.bw.Write(hdr[:]); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := w.bw.Write(payload); err != nil {
			return fmt.Errorf("write frame payload: %w", err)
		}
	}
	if err := w.bw.WriteByte(FrameEnd); err != nil {
		return fmt.Errorf("write frame-end: %w", err)
	}
	return nil
}

// limitedBuffer is a simple byte buffer used for intermediate encoding.
type limitedBuffer struct {
	buf []byte
}

func (lb *limitedBuffer) Write(p []byte) (int, error) {
	lb.buf = append(lb.buf, p...)
	return len(p), nil
}

func (lb *limitedBuffer) Bytes() []byte { return lb.buf }
func (lb *limitedBuffer) Len() int      { return len(lb.buf) }
