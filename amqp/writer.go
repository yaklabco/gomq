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
	methodBuf    []byte // reusable buffer for method/property encoding
}

// sliceWriter appends writes to a caller-owned byte slice, avoiding
// intermediate allocations.
type sliceWriter struct {
	buf *[]byte
}

func (sw *sliceWriter) Write(p []byte) (int, error) {
	*sw.buf = append(*sw.buf, p...)
	return len(p), nil
}

// writerBufSize is the bufio.Writer buffer size. 64 KiB reduces write
// syscalls on high-throughput connections compared to the default 4 KiB.
const writerBufSize = 65536

// NewWriter creates a new frame writer with the given maximum frame size.
func NewWriter(w io.Writer, maxFrameSize int) *Writer {
	return &Writer{
		bw:           bufio.NewWriterSize(w, writerBufSize),
		maxFrameSize: maxFrameSize,
		scratch:      make([]byte, maxFrameSize),
		methodBuf:    make([]byte, 0, maxFrameSize),
	}
}

// WriteMethod writes a method frame for the given channel.
func (w *Writer) WriteMethod(channel uint16, method Method) error {
	// Encode the method payload: class(2) + method(2) + method_fields.
	id := method.MethodID()
	binary.BigEndian.PutUint16(w.scratch[:sizeUint16], uint16(id>>classShift))
	binary.BigEndian.PutUint16(w.scratch[sizeUint16:sizeUint32], uint16(id&methodMask))

	// Reuse methodBuf to avoid per-call heap allocation.
	w.methodBuf = w.methodBuf[:0]
	sw := &sliceWriter{buf: &w.methodBuf}
	if err := method.Write(sw); err != nil {
		return fmt.Errorf("encode method %s: %w", method.MethodName(), err)
	}

	payloadSize := sizeUint32 + len(w.methodBuf)
	copy(w.scratch[sizeUint32:], w.methodBuf)

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

	// Reuse methodBuf to avoid per-call heap allocation for properties.
	w.methodBuf = w.methodBuf[:0]
	sw := &sliceWriter{buf: &w.methodBuf}
	if err := props.Write(sw); err != nil {
		return fmt.Errorf("encode header properties: %w", err)
	}

	copy(w.scratch[off:], w.methodBuf)
	off += len(w.methodBuf)

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

// WriteDelivery writes a complete BasicDeliver + content header + body as a
// single batch with one Flush. Method and header frames are encoded directly
// into scratch, avoiding intermediate encoding buffers. This is the hot-path
// writer for consumer delivery.
func (w *Writer) WriteDelivery(channel uint16, deliver *BasicDeliver, classID uint16, bodySize uint64, props *Properties, body []byte) error {
	// 1. Write the method frame (BasicDeliver).
	if err := w.WriteMethod(channel, deliver); err != nil {
		return fmt.Errorf("write delivery method: %w", err)
	}

	// 2. Write the content header frame.
	if err := w.WriteHeader(channel, classID, bodySize, props); err != nil {
		return fmt.Errorf("write delivery header: %w", err)
	}

	// 3. Write body frame(s).
	if err := w.WriteBody(channel, body); err != nil {
		return fmt.Errorf("write delivery body: %w", err)
	}

	// 4. Single flush for the entire delivery.
	return w.Flush()
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
	binary.BigEndian.PutUint32(hdr[3:frameHeaderSize], uint32(len(payload))) //nolint:gosec // payload bounded by maxFrameSize

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
