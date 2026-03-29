package amqp

import (
	"encoding/binary"
	"fmt"
	"io"
)

// wireReader wraps an io.Reader with helpers for reading AMQP wire primitives.
type wireReader struct {
	rd io.Reader
}

func (wr *wireReader) readUint8() (uint8, error) {
	var buf [1]byte
	if _, err := io.ReadFull(wr.rd, buf[:]); err != nil {
		return 0, fmt.Errorf("read uint8: %w", err)
	}
	return buf[0], nil
}

func (wr *wireReader) readUint16() (uint16, error) {
	var buf [sizeUint16]byte
	if _, err := io.ReadFull(wr.rd, buf[:]); err != nil {
		return 0, fmt.Errorf("read uint16: %w", err)
	}
	return binary.BigEndian.Uint16(buf[:]), nil
}

func (wr *wireReader) readUint32() (uint32, error) {
	var buf [sizeUint32]byte
	if _, err := io.ReadFull(wr.rd, buf[:]); err != nil {
		return 0, fmt.Errorf("read uint32: %w", err)
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

func (wr *wireReader) readUint64() (uint64, error) {
	var buf [sizeUint64]byte
	if _, err := io.ReadFull(wr.rd, buf[:]); err != nil {
		return 0, fmt.Errorf("read uint64: %w", err)
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

// readShortstr reads a 1-byte length prefixed string.
func (wr *wireReader) readShortstr() (string, error) {
	slen, err := wr.readUint8()
	if err != nil {
		return "", fmt.Errorf("shortstr length: %w", err)
	}
	buf := make([]byte, slen)
	if _, err := io.ReadFull(wr.rd, buf); err != nil {
		return "", fmt.Errorf("shortstr payload: %w", err)
	}
	return string(buf), nil
}

// readLongstr reads a 4-byte length prefixed byte slice.
func (wr *wireReader) readLongstr() ([]byte, error) {
	slen, err := wr.readUint32()
	if err != nil {
		return nil, fmt.Errorf("longstr length: %w", err)
	}
	buf := make([]byte, slen)
	if _, err := io.ReadFull(wr.rd, buf); err != nil {
		return nil, fmt.Errorf("longstr payload: %w", err)
	}
	return buf, nil
}

// readTable reads a field table from the wire.
func (wr *wireReader) readTable() (Table, error) {
	data, err := wr.readLongstr()
	if err != nil {
		return nil, fmt.Errorf("table data: %w", err)
	}
	if len(data) == 0 {
		return make(Table), nil
	}
	// Prepend the 4-byte length so unmarshalTable can parse it.
	prefixed := make([]byte, sizeLongLen+len(data))
	binary.BigEndian.PutUint32(prefixed[:sizeLongLen], uint32(len(data)))
	copy(prefixed[sizeLongLen:], data)
	tbl, _, err := unmarshalTable(prefixed)
	if err != nil {
		return nil, fmt.Errorf("decode table: %w", err)
	}
	return tbl, nil
}

// wireWriter wraps an io.Writer with helpers for writing AMQP wire primitives.
type wireWriter struct {
	wt io.Writer
}

func (ww *wireWriter) writeUint8(val uint8) error {
	_, err := ww.wt.Write([]byte{val})
	if err != nil {
		return fmt.Errorf("write uint8: %w", err)
	}
	return nil
}

func (ww *wireWriter) writeUint16(val uint16) error {
	var buf [sizeUint16]byte
	binary.BigEndian.PutUint16(buf[:], val)
	if _, err := ww.wt.Write(buf[:]); err != nil {
		return fmt.Errorf("write uint16: %w", err)
	}
	return nil
}

func (ww *wireWriter) writeUint32(val uint32) error {
	var buf [sizeUint32]byte
	binary.BigEndian.PutUint32(buf[:], val)
	if _, err := ww.wt.Write(buf[:]); err != nil {
		return fmt.Errorf("write uint32: %w", err)
	}
	return nil
}

func (ww *wireWriter) writeUint64(val uint64) error {
	var buf [sizeUint64]byte
	binary.BigEndian.PutUint64(buf[:], val)
	if _, err := ww.wt.Write(buf[:]); err != nil {
		return fmt.Errorf("write uint64: %w", err)
	}
	return nil
}

// writeShortstr writes a 1-byte length prefixed string.
func (ww *wireWriter) writeShortstr(str string) error {
	if len(str) > maxShortStr {
		return fmt.Errorf("shortstr too long: %d bytes (max %d)", len(str), maxShortStr)
	}
	if err := ww.writeUint8(uint8(len(str))); err != nil {
		return err
	}
	if _, err := ww.wt.Write([]byte(str)); err != nil {
		return fmt.Errorf("write shortstr payload: %w", err)
	}
	return nil
}

// writeLongstr writes a 4-byte length prefixed byte slice.
func (ww *wireWriter) writeLongstr(data []byte) error {
	if err := ww.writeUint32(uint32(len(data))); err != nil {
		return err
	}
	if _, err := ww.wt.Write(data); err != nil {
		return fmt.Errorf("write longstr payload: %w", err)
	}
	return nil
}

// writeTable writes a field table to the wire.
func (ww *wireWriter) writeTable(tbl Table) error {
	// Marshal the table entries (without the 4-byte length prefix that marshalTable writes).
	buf := make([]byte, tableBufferSize)
	wrote, err := marshalTable(buf, tbl)
	if err != nil {
		return fmt.Errorf("encode table: %w", err)
	}
	// marshalTable writes: [4-byte length][entries...]
	// We want to write the full thing (length + entries) as the wire format.
	if _, err := ww.wt.Write(buf[:wrote]); err != nil {
		return fmt.Errorf("write table: %w", err)
	}
	return nil
}

const tableBufferSize = 65536 // generous buffer for table encoding
