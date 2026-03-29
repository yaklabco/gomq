package amqp

import (
	"bytes"
	"math"
	"testing"
)

func TestMarshalTable_empty(t *testing.T) {
	t.Parallel()

	buf := make([]byte, 4096)
	wrote, err := marshalTable(buf, Table{})
	if err != nil {
		t.Fatalf("marshalTable() error = %v", err)
	}
	// Empty table: 4-byte length prefix with value 0.
	if wrote != 4 {
		t.Errorf("marshalTable() wrote %d bytes, want 4", wrote)
	}
	// The 4-byte length prefix should be zero.
	length := uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3])
	if length != 0 {
		t.Errorf("table length = %d, want 0", length)
	}
}

func TestMarshalTable_nilTable(t *testing.T) {
	t.Parallel()

	buf := make([]byte, 4096)
	wrote, err := marshalTable(buf, nil)
	if err != nil {
		t.Fatalf("marshalTable() error = %v", err)
	}
	if wrote != 4 {
		t.Errorf("marshalTable() wrote %d bytes, want 4", wrote)
	}
}

func TestRoundTrip_stringValue(t *testing.T) {
	t.Parallel()

	original := Table{
		"key": "hello",
	}

	buf := make([]byte, 4096)
	wrote, err := marshalTable(buf, original)
	if err != nil {
		t.Fatalf("marshalTable() error = %v", err)
	}

	decoded, consumed, err := unmarshalTable(buf[:wrote])
	if err != nil {
		t.Fatalf("unmarshalTable() error = %v", err)
	}
	if consumed != wrote {
		t.Errorf("unmarshalTable() consumed %d bytes, want %d", consumed, wrote)
	}
	val, ok := decoded["key"]
	if !ok {
		t.Fatal("missing key 'key' in decoded table")
	}
	str, ok := val.(string)
	if !ok {
		t.Fatalf("value type = %T, want string", val)
	}
	if str != "hello" {
		t.Errorf("value = %q, want %q", str, "hello")
	}
}

func TestRoundTrip_integerTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		val  any
		want any
	}{
		{"int8", int8(-42), int8(-42)},
		{"uint8", uint8(200), uint8(200)},
		{"int16", int16(-1000), int16(-1000)},
		{"uint16", uint16(50000), uint16(50000)},
		{"int32", int32(-100000), int32(-100000)},
		{"uint32", uint32(3000000000), uint32(3000000000)},
		{"int64", int64(-9000000000000), int64(-9000000000000)},
		{"uint64", uint64(math.MaxUint64), uint64(math.MaxUint64)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := Table{"num": tt.val}
			buf := make([]byte, 4096)
			wrote, err := marshalTable(buf, original)
			if err != nil {
				t.Fatalf("marshalTable() error = %v", err)
			}

			decoded, _, err := unmarshalTable(buf[:wrote])
			if err != nil {
				t.Fatalf("unmarshalTable() error = %v", err)
			}

			got := decoded["num"]
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestRoundTrip_bool(t *testing.T) {
	t.Parallel()

	for _, val := range []bool{true, false} {
		t.Run(boolName(val), func(t *testing.T) {
			t.Parallel()

			original := Table{"flag": val}
			buf := make([]byte, 4096)
			wrote, err := marshalTable(buf, original)
			if err != nil {
				t.Fatalf("marshalTable() error = %v", err)
			}

			decoded, _, err := unmarshalTable(buf[:wrote])
			if err != nil {
				t.Fatalf("unmarshalTable() error = %v", err)
			}

			got, ok := decoded["flag"].(bool)
			if !ok {
				t.Fatalf("value type = %T, want bool", decoded["flag"])
			}
			if got != val {
				t.Errorf("got %v, want %v", got, val)
			}
		})
	}
}

func boolName(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func TestRoundTrip_nil(t *testing.T) {
	t.Parallel()

	original := Table{"empty": nil}
	buf := make([]byte, 4096)
	wrote, err := marshalTable(buf, original)
	if err != nil {
		t.Fatalf("marshalTable() error = %v", err)
	}

	decoded, _, err := unmarshalTable(buf[:wrote])
	if err != nil {
		t.Fatalf("unmarshalTable() error = %v", err)
	}

	if decoded["empty"] != nil {
		t.Errorf("got %v, want nil", decoded["empty"])
	}
}

func TestRoundTrip_nestedTable(t *testing.T) {
	t.Parallel()

	original := Table{
		"outer": Table{
			"inner": "value",
		},
	}

	buf := make([]byte, 4096)
	wrote, err := marshalTable(buf, original)
	if err != nil {
		t.Fatalf("marshalTable() error = %v", err)
	}

	decoded, _, err := unmarshalTable(buf[:wrote])
	if err != nil {
		t.Fatalf("unmarshalTable() error = %v", err)
	}

	outerRaw, ok := decoded["outer"]
	if !ok {
		t.Fatal("missing key 'outer'")
	}
	outer, ok := outerRaw.(Table)
	if !ok {
		t.Fatalf("outer type = %T, want Table", outerRaw)
	}
	inner, ok := outer["inner"].(string)
	if !ok {
		t.Fatalf("inner type = %T, want string", outer["inner"])
	}
	if inner != "value" {
		t.Errorf("inner = %q, want %q", inner, "value")
	}
}

func TestRoundTrip_array(t *testing.T) {
	t.Parallel()

	original := Table{
		"list": []any{int32(1), "two", true},
	}

	buf := make([]byte, 4096)
	wrote, err := marshalTable(buf, original)
	if err != nil {
		t.Fatalf("marshalTable() error = %v", err)
	}

	decoded, _, err := unmarshalTable(buf[:wrote])
	if err != nil {
		t.Fatalf("unmarshalTable() error = %v", err)
	}

	listRaw, ok := decoded["list"]
	if !ok {
		t.Fatal("missing key 'list'")
	}
	list, ok := listRaw.([]any)
	if !ok {
		t.Fatalf("list type = %T, want []any", listRaw)
	}
	if len(list) != 3 {
		t.Fatalf("list length = %d, want 3", len(list))
	}
	if list[0] != int32(1) {
		t.Errorf("list[0] = %v (%T), want int32(1)", list[0], list[0])
	}
	if list[1] != "two" {
		t.Errorf("list[1] = %v, want %q", list[1], "two")
	}
	bval, ok := list[2].(bool)
	if !ok || !bval {
		t.Errorf("list[2] = %v, want true", list[2])
	}
}

func TestRoundTrip_floats(t *testing.T) {
	t.Parallel()

	original := Table{
		"f32": float32(3.14),
		"f64": float64(2.71828),
	}

	buf := make([]byte, 4096)
	wrote, err := marshalTable(buf, original)
	if err != nil {
		t.Fatalf("marshalTable() error = %v", err)
	}

	decoded, _, err := unmarshalTable(buf[:wrote])
	if err != nil {
		t.Fatalf("unmarshalTable() error = %v", err)
	}

	f32, ok := decoded["f32"].(float32)
	if !ok {
		t.Fatalf("f32 type = %T, want float32", decoded["f32"])
	}
	if f32 != float32(3.14) {
		t.Errorf("f32 = %v, want %v", f32, float32(3.14))
	}

	f64, ok := decoded["f64"].(float64)
	if !ok {
		t.Fatalf("f64 type = %T, want float64", decoded["f64"])
	}
	if f64 != 2.71828 {
		t.Errorf("f64 = %v, want %v", f64, 2.71828)
	}
}

func TestRoundTrip_timestamp(t *testing.T) {
	t.Parallel()

	// Timestamps are encoded as uint64 with tag 'T'.
	// We use a special wrapper to distinguish from plain uint64.
	original := Table{
		"ts": Timestamp(1234567890),
	}

	buf := make([]byte, 4096)
	wrote, err := marshalTable(buf, original)
	if err != nil {
		t.Fatalf("marshalTable() error = %v", err)
	}

	decoded, _, err := unmarshalTable(buf[:wrote])
	if err != nil {
		t.Fatalf("unmarshalTable() error = %v", err)
	}

	ts, ok := decoded["ts"].(Timestamp)
	if !ok {
		t.Fatalf("ts type = %T, want Timestamp", decoded["ts"])
	}
	if ts != Timestamp(1234567890) {
		t.Errorf("ts = %v, want %v", ts, Timestamp(1234567890))
	}
}

func TestRoundTrip_byteArray(t *testing.T) {
	t.Parallel()

	original := Table{
		"data": []byte{0xDE, 0xAD, 0xBE, 0xEF},
	}

	buf := make([]byte, 4096)
	wrote, err := marshalTable(buf, original)
	if err != nil {
		t.Fatalf("marshalTable() error = %v", err)
	}

	decoded, _, err := unmarshalTable(buf[:wrote])
	if err != nil {
		t.Fatalf("unmarshalTable() error = %v", err)
	}

	data, ok := decoded["data"].([]byte)
	if !ok {
		t.Fatalf("data type = %T, want []byte", decoded["data"])
	}
	if !bytes.Equal(data, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Errorf("data = %x, want deadbeef", data)
	}
}

func TestRoundTrip_multipleKeys(t *testing.T) {
	t.Parallel()

	original := Table{
		"product":  "gomq",
		"version":  "0.1.0",
		"durable":  true,
		"priority": int32(5),
	}

	buf := make([]byte, 4096)
	wrote, err := marshalTable(buf, original)
	if err != nil {
		t.Fatalf("marshalTable() error = %v", err)
	}

	decoded, readBack, err := unmarshalTable(buf[:wrote])
	if err != nil {
		t.Fatalf("unmarshalTable() error = %v", err)
	}
	if readBack != wrote {
		t.Errorf("consumed %d, want %d", readBack, wrote)
	}

	if decoded["product"] != "gomq" {
		t.Errorf("product = %v, want %q", decoded["product"], "gomq")
	}
	if decoded["version"] != "0.1.0" {
		t.Errorf("version = %v, want %q", decoded["version"], "0.1.0")
	}
	dur, ok := decoded["durable"].(bool)
	if !ok || !dur {
		t.Errorf("durable = %v, want true", decoded["durable"])
	}
	if decoded["priority"] != int32(5) {
		t.Errorf("priority = %v, want int32(5)", decoded["priority"])
	}
}

func TestUnmarshalTable_truncatedLength(t *testing.T) {
	t.Parallel()

	// Buffer too short for 4-byte length prefix.
	_, _, err := unmarshalTable([]byte{0x00, 0x00})
	if err == nil {
		t.Error("expected error for truncated buffer, got nil")
	}
}

func TestUnmarshalTable_truncatedPayload(t *testing.T) {
	t.Parallel()

	// Length says 100 bytes but buffer only has 4 (the length prefix).
	buf := []byte{0x00, 0x00, 0x00, 0x64}
	_, _, err := unmarshalTable(buf)
	if err == nil {
		t.Error("expected error for truncated payload, got nil")
	}
}

func TestMarshalTable_unsupportedType(t *testing.T) {
	t.Parallel()

	buf := make([]byte, 4096)
	_, err := marshalTable(buf, Table{"bad": struct{}{}})
	if err == nil {
		t.Error("expected error for unsupported type, got nil")
	}
}
