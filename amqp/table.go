package amqp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

// Table represents an AMQP field table — a map of string keys to typed values.
type Table map[string]any

// Timestamp is a distinct type for AMQP timestamps (uint64 seconds since epoch),
// allowing field table encoding to distinguish timestamps from plain uint64 values.
type Timestamp uint64

// Wire format sizes for AMQP primitive types (tag byte excluded).
const (
	sizeUint8   = 1
	sizeUint16  = 2
	sizeUint32  = 4
	sizeUint64  = 8
	sizeLongLen = 4 // 4-byte length prefix for tables, longstr, arrays
	maxShortStr = 255
)

// Sentinel errors for table codec failures.
var (
	errBufferTooSmall = errors.New("buffer too small")
	errBufferTooShort = errors.New("buffer too short")
)

// marshalTable writes a field table to buf in AMQP wire format.
// Returns the number of bytes written.
func marshalTable(buf []byte, tbl Table) (int, error) {
	if len(buf) < sizeLongLen {
		return 0, errBufferTooSmall
	}

	// Reserve 4 bytes for the table length prefix; write entries after.
	off := sizeLongLen

	// Sort keys for deterministic output.
	keys := make([]string, 0, len(tbl))
	for key := range tbl {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		wrote, err := marshalFieldKey(buf[off:], key)
		if err != nil {
			return 0, fmt.Errorf("marshal key %q: %w", key, err)
		}
		off += wrote

		wrote, err = marshalFieldValue(buf[off:], tbl[key])
		if err != nil {
			return 0, fmt.Errorf("marshal value for key %q: %w", key, err)
		}
		off += wrote
	}

	// Write the table payload length (excludes the 4-byte prefix itself).
	binary.BigEndian.PutUint32(buf[:sizeLongLen], uint32(off-sizeLongLen))

	return off, nil
}

// unmarshalTable reads a field table from buf.
// Returns the decoded table and the total number of bytes consumed (including length prefix).
func unmarshalTable(buf []byte) (Table, int, error) {
	if len(buf) < sizeLongLen {
		return nil, 0, fmt.Errorf("table length prefix: %w", errBufferTooShort)
	}

	tableLen := int(binary.BigEndian.Uint32(buf[:sizeLongLen]))
	if len(buf) < sizeLongLen+tableLen {
		return nil, 0, fmt.Errorf("table payload (%d bytes): %w", tableLen, errBufferTooShort)
	}

	tbl := make(Table)
	off := sizeLongLen
	end := sizeLongLen + tableLen

	for off < end {
		key, keySize, err := unmarshalFieldKey(buf[off:])
		if err != nil {
			return nil, 0, fmt.Errorf("unmarshal field key at offset %d: %w", off, err)
		}
		off += keySize

		val, valSize, err := unmarshalFieldValue(buf[off:])
		if err != nil {
			return nil, 0, fmt.Errorf("unmarshal field value for key %q: %w", key, err)
		}
		off += valSize

		tbl[key] = val
	}

	return tbl, end, nil
}

func marshalFieldKey(buf []byte, key string) (int, error) {
	if len(key) > maxShortStr {
		return 0, fmt.Errorf("field key too long: %d bytes", len(key))
	}
	needed := 1 + len(key)
	if len(buf) < needed {
		return 0, fmt.Errorf("field key: %w", errBufferTooSmall)
	}
	buf[0] = byte(len(key))
	copy(buf[1:], key)
	return needed, nil
}

func unmarshalFieldKey(buf []byte) (string, int, error) {
	if len(buf) < 1 {
		return "", 0, fmt.Errorf("field key length: %w", errBufferTooShort)
	}
	keyLen := int(buf[0])
	if len(buf) < 1+keyLen {
		return "", 0, fmt.Errorf("field key payload (%d bytes): %w", keyLen, errBufferTooShort)
	}
	return string(buf[1 : 1+keyLen]), 1 + keyLen, nil
}

func marshalFieldValue(buf []byte, val any) (int, error) {
	if len(buf) < 1 {
		return 0, fmt.Errorf("type tag: %w", errBufferTooSmall)
	}

	switch tv := val.(type) {
	case nil:
		buf[0] = 'V'
		return 1, nil
	case bool:
		if len(buf) < 1+sizeUint8 {
			return 0, fmt.Errorf("bool: %w", errBufferTooSmall)
		}
		buf[0] = 't'
		if tv {
			buf[1] = 1
		} else {
			buf[1] = 0
		}
		return 1 + sizeUint8, nil
	case int8:
		if len(buf) < 1+sizeUint8 {
			return 0, fmt.Errorf("int8: %w", errBufferTooSmall)
		}
		buf[0] = 'b'
		buf[1] = byte(tv)
		return 1 + sizeUint8, nil
	case uint8:
		if len(buf) < 1+sizeUint8 {
			return 0, fmt.Errorf("uint8: %w", errBufferTooSmall)
		}
		buf[0] = 'B'
		buf[1] = tv
		return 1 + sizeUint8, nil
	case int16:
		if len(buf) < 1+sizeUint16 {
			return 0, fmt.Errorf("int16: %w", errBufferTooSmall)
		}
		buf[0] = 'U'
		binary.BigEndian.PutUint16(buf[1:], uint16(tv))
		return 1 + sizeUint16, nil
	case uint16:
		if len(buf) < 1+sizeUint16 {
			return 0, fmt.Errorf("uint16: %w", errBufferTooSmall)
		}
		buf[0] = 'u'
		binary.BigEndian.PutUint16(buf[1:], tv)
		return 1 + sizeUint16, nil
	case int32:
		if len(buf) < 1+sizeUint32 {
			return 0, fmt.Errorf("int32: %w", errBufferTooSmall)
		}
		buf[0] = 'I'
		binary.BigEndian.PutUint32(buf[1:], uint32(tv))
		return 1 + sizeUint32, nil
	case uint32:
		if len(buf) < 1+sizeUint32 {
			return 0, fmt.Errorf("uint32: %w", errBufferTooSmall)
		}
		buf[0] = 'i'
		binary.BigEndian.PutUint32(buf[1:], tv)
		return 1 + sizeUint32, nil
	case int64:
		if len(buf) < 1+sizeUint64 {
			return 0, fmt.Errorf("int64: %w", errBufferTooSmall)
		}
		buf[0] = 'L'
		binary.BigEndian.PutUint64(buf[1:], uint64(tv))
		return 1 + sizeUint64, nil
	case uint64:
		if len(buf) < 1+sizeUint64 {
			return 0, fmt.Errorf("uint64: %w", errBufferTooSmall)
		}
		buf[0] = 'l'
		binary.BigEndian.PutUint64(buf[1:], tv)
		return 1 + sizeUint64, nil
	case Timestamp:
		if len(buf) < 1+sizeUint64 {
			return 0, fmt.Errorf("timestamp: %w", errBufferTooSmall)
		}
		buf[0] = 'T'
		binary.BigEndian.PutUint64(buf[1:], uint64(tv))
		return 1 + sizeUint64, nil
	case float32:
		if len(buf) < 1+sizeUint32 {
			return 0, fmt.Errorf("float32: %w", errBufferTooSmall)
		}
		buf[0] = 'f'
		binary.BigEndian.PutUint32(buf[1:], math.Float32bits(tv))
		return 1 + sizeUint32, nil
	case float64:
		if len(buf) < 1+sizeUint64 {
			return 0, fmt.Errorf("float64: %w", errBufferTooSmall)
		}
		buf[0] = 'd'
		binary.BigEndian.PutUint64(buf[1:], math.Float64bits(tv))
		return 1 + sizeUint64, nil
	case string:
		return marshalLongstr(buf, []byte(tv))
	case []byte:
		return marshalByteArray(buf, tv)
	case Table:
		return marshalNestedTable(buf, tv)
	case []any:
		return marshalArray(buf, tv)
	default:
		return 0, fmt.Errorf("unsupported field value type: %T", val)
	}
}

func marshalLongstr(buf []byte, data []byte) (int, error) {
	needed := 1 + sizeLongLen + len(data)
	if len(buf) < needed {
		return 0, fmt.Errorf("longstr: %w", errBufferTooSmall)
	}
	buf[0] = 'S'
	binary.BigEndian.PutUint32(buf[1:], uint32(len(data)))
	copy(buf[1+sizeLongLen:], data)
	return needed, nil
}

func marshalByteArray(buf []byte, data []byte) (int, error) {
	needed := 1 + sizeLongLen + len(data)
	if len(buf) < needed {
		return 0, fmt.Errorf("byte array: %w", errBufferTooSmall)
	}
	buf[0] = 'x'
	binary.BigEndian.PutUint32(buf[1:], uint32(len(data)))
	copy(buf[1+sizeLongLen:], data)
	return needed, nil
}

func marshalNestedTable(buf []byte, tbl Table) (int, error) {
	if len(buf) < 1 {
		return 0, fmt.Errorf("nested table tag: %w", errBufferTooSmall)
	}
	buf[0] = 'F'
	wrote, err := marshalTable(buf[1:], tbl)
	if err != nil {
		return 0, fmt.Errorf("marshal nested table: %w", err)
	}
	return 1 + wrote, nil
}

func marshalArray(buf []byte, arr []any) (int, error) {
	headerSize := 1 + sizeLongLen
	if len(buf) < headerSize {
		return 0, fmt.Errorf("array header: %w", errBufferTooSmall)
	}
	buf[0] = 'A'
	// Reserve 4 bytes for array payload length.
	off := headerSize
	for idx, item := range arr {
		wrote, err := marshalFieldValue(buf[off:], item)
		if err != nil {
			return 0, fmt.Errorf("marshal array element %d: %w", idx, err)
		}
		off += wrote
	}
	binary.BigEndian.PutUint32(buf[1:headerSize], uint32(off-headerSize))
	return off, nil
}

func unmarshalFieldValue(buf []byte) (any, int, error) {
	if len(buf) < 1 {
		return nil, 0, fmt.Errorf("type tag: %w", errBufferTooShort)
	}

	tag := buf[0]
	payload := buf[1:]

	switch tag {
	case 'V':
		return nil, 1, nil
	case 't':
		if len(payload) < sizeUint8 {
			return nil, 0, fmt.Errorf("bool value: %w", errBufferTooShort)
		}
		return payload[0] != 0, 1 + sizeUint8, nil
	case 'b':
		if len(payload) < sizeUint8 {
			return nil, 0, fmt.Errorf("int8 value: %w", errBufferTooShort)
		}
		return int8(payload[0]), 1 + sizeUint8, nil
	case 'B':
		if len(payload) < sizeUint8 {
			return nil, 0, fmt.Errorf("uint8 value: %w", errBufferTooShort)
		}
		return payload[0], 1 + sizeUint8, nil
	case 'U':
		if len(payload) < sizeUint16 {
			return nil, 0, fmt.Errorf("int16 value: %w", errBufferTooShort)
		}
		return int16(binary.BigEndian.Uint16(payload[:sizeUint16])), 1 + sizeUint16, nil
	case 'u':
		if len(payload) < sizeUint16 {
			return nil, 0, fmt.Errorf("uint16 value: %w", errBufferTooShort)
		}
		return binary.BigEndian.Uint16(payload[:sizeUint16]), 1 + sizeUint16, nil
	case 'I':
		if len(payload) < sizeUint32 {
			return nil, 0, fmt.Errorf("int32 value: %w", errBufferTooShort)
		}
		return int32(binary.BigEndian.Uint32(payload[:sizeUint32])), 1 + sizeUint32, nil
	case 'i':
		if len(payload) < sizeUint32 {
			return nil, 0, fmt.Errorf("uint32 value: %w", errBufferTooShort)
		}
		return binary.BigEndian.Uint32(payload[:sizeUint32]), 1 + sizeUint32, nil
	case 'L':
		if len(payload) < sizeUint64 {
			return nil, 0, fmt.Errorf("int64 value: %w", errBufferTooShort)
		}
		return int64(binary.BigEndian.Uint64(payload[:sizeUint64])), 1 + sizeUint64, nil
	case 'l':
		if len(payload) < sizeUint64 {
			return nil, 0, fmt.Errorf("uint64 value: %w", errBufferTooShort)
		}
		return binary.BigEndian.Uint64(payload[:sizeUint64]), 1 + sizeUint64, nil
	case 'T':
		if len(payload) < sizeUint64 {
			return nil, 0, fmt.Errorf("timestamp value: %w", errBufferTooShort)
		}
		return Timestamp(binary.BigEndian.Uint64(payload[:sizeUint64])), 1 + sizeUint64, nil
	case 'f':
		if len(payload) < sizeUint32 {
			return nil, 0, fmt.Errorf("float32 value: %w", errBufferTooShort)
		}
		return math.Float32frombits(binary.BigEndian.Uint32(payload[:sizeUint32])), 1 + sizeUint32, nil
	case 'd':
		if len(payload) < sizeUint64 {
			return nil, 0, fmt.Errorf("float64 value: %w", errBufferTooShort)
		}
		return math.Float64frombits(binary.BigEndian.Uint64(payload[:sizeUint64])), 1 + sizeUint64, nil
	case 's':
		return unmarshalShortstr(payload)
	case 'S':
		return unmarshalLongstr(payload)
	case 'x':
		return unmarshalByteArr(payload)
	case 'F':
		return unmarshalNestedTable(payload)
	case 'A':
		return unmarshalArray(payload)
	default:
		return nil, 0, fmt.Errorf("unknown field value type tag: %c (0x%02x)", tag, tag)
	}
}

func unmarshalShortstr(payload []byte) (any, int, error) {
	if len(payload) < 1 {
		return nil, 0, fmt.Errorf("shortstr length: %w", errBufferTooShort)
	}
	slen := int(payload[0])
	if len(payload) < 1+slen {
		return nil, 0, fmt.Errorf("shortstr payload (%d bytes): %w", slen, errBufferTooShort)
	}
	// +1 for tag (already consumed by caller), +1 for length byte, +slen for string.
	return string(payload[1 : 1+slen]), 1 + 1 + slen, nil
}

func unmarshalLongstr(payload []byte) (any, int, error) {
	if len(payload) < sizeLongLen {
		return nil, 0, fmt.Errorf("longstr length: %w", errBufferTooShort)
	}
	slen := int(binary.BigEndian.Uint32(payload[:sizeLongLen]))
	if len(payload) < sizeLongLen+slen {
		return nil, 0, fmt.Errorf("longstr payload (%d bytes): %w", slen, errBufferTooShort)
	}
	// +1 for tag, +4 for length, +slen for payload.
	return string(payload[sizeLongLen : sizeLongLen+slen]), 1 + sizeLongLen + slen, nil
}

func unmarshalByteArr(payload []byte) (any, int, error) {
	if len(payload) < sizeLongLen {
		return nil, 0, fmt.Errorf("byte array length: %w", errBufferTooShort)
	}
	blen := int(binary.BigEndian.Uint32(payload[:sizeLongLen]))
	if len(payload) < sizeLongLen+blen {
		return nil, 0, fmt.Errorf("byte array payload (%d bytes): %w", blen, errBufferTooShort)
	}
	data := make([]byte, blen)
	copy(data, payload[sizeLongLen:sizeLongLen+blen])
	// +1 for tag, +4 for length, +blen for payload.
	return data, 1 + sizeLongLen + blen, nil
}

func unmarshalNestedTable(payload []byte) (any, int, error) {
	tbl, consumed, err := unmarshalTable(payload)
	if err != nil {
		return nil, 0, fmt.Errorf("unmarshal nested table: %w", err)
	}
	// +1 for the 'F' tag already consumed.
	return tbl, 1 + consumed, nil
}

func unmarshalArray(payload []byte) (any, int, error) {
	if len(payload) < sizeLongLen {
		return nil, 0, fmt.Errorf("array length: %w", errBufferTooShort)
	}
	arrLen := int(binary.BigEndian.Uint32(payload[:sizeLongLen]))
	if len(payload) < sizeLongLen+arrLen {
		return nil, 0, fmt.Errorf("array payload (%d bytes): %w", arrLen, errBufferTooShort)
	}

	var arr []any
	off := sizeLongLen
	end := sizeLongLen + arrLen

	for off < end {
		val, consumed, err := unmarshalFieldValue(payload[off:])
		if err != nil {
			return nil, 0, fmt.Errorf("unmarshal array element: %w", err)
		}
		arr = append(arr, val)
		off += consumed
	}

	// +1 for the 'A' tag already consumed.
	return arr, 1 + off, nil
}
