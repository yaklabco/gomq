package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ActionType identifies the kind of replication action.
type ActionType byte

const (
	// ActionAppend appends data to an existing file.
	ActionAppend ActionType = 1
	// ActionReplace replaces or creates a file with the given data.
	ActionReplace ActionType = 2
	// ActionDelete removes a file.
	ActionDelete ActionType = 3
)

// Action represents a single file-level replication operation.
type Action struct {
	Type ActionType
	Path string
	Data []byte // nil for delete
}

// Wire format per action:
//   [1 byte]  ActionType
//   [4 bytes] path length (little-endian uint32)
//   [N bytes] path
//   [4 bytes] data length (little-endian uint32), 0 for delete
//   [M bytes] data (omitted when data length is 0)

// actionCountSize is the byte size of the uint32 action count header.
const actionCountSize = 4

// EncodeActions serialises a slice of actions into a binary format.
// The output is prefixed with a 4-byte little-endian action count.
func EncodeActions(actions []Action) ([]byte, error) {
	// Pre-size the buffer: count header + estimated per-action overhead.
	buf := make([]byte, 0, actionCountSize+len(actions)*32)

	// Action count.
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(actions)))

	for _, action := range actions {
		buf = append(buf, byte(action.Type))

		// Path.
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(action.Path)))
		buf = append(buf, action.Path...)

		// Data.
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(action.Data)))
		if len(action.Data) > 0 {
			buf = append(buf, action.Data...)
		}
	}

	return buf, nil
}

// DecodeActions deserialises actions from the binary format produced by
// EncodeActions. Returns an error if the data is truncated or malformed.
func DecodeActions(data []byte) ([]Action, error) {
	if len(data) < actionCountSize {
		return nil, fmt.Errorf("decode actions: %w", io.ErrUnexpectedEOF)
	}

	count := binary.LittleEndian.Uint32(data[:4])
	off := actionCountSize

	actions := make([]Action, 0, count)

	for idx := range count {
		// Type.
		if off >= len(data) {
			return nil, fmt.Errorf("decode action %d type: %w", idx, io.ErrUnexpectedEOF)
		}
		aType := ActionType(data[off])
		off++

		// Path length.
		if off+4 > len(data) {
			return nil, fmt.Errorf("decode action %d path length: %w", idx, io.ErrUnexpectedEOF)
		}
		pathLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4

		// Path.
		if off+pathLen > len(data) {
			return nil, fmt.Errorf("decode action %d path: %w", idx, io.ErrUnexpectedEOF)
		}
		path := string(data[off : off+pathLen])
		off += pathLen

		// Data length.
		if off+4 > len(data) {
			return nil, fmt.Errorf("decode action %d data length: %w", idx, io.ErrUnexpectedEOF)
		}
		dataLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4

		var actionData []byte
		if dataLen > 0 {
			if off+dataLen > len(data) {
				return nil, fmt.Errorf("decode action %d data: %w", idx, io.ErrUnexpectedEOF)
			}
			actionData = make([]byte, dataLen)
			copy(actionData, data[off:off+dataLen])
			off += dataLen
		}

		actions = append(actions, Action{
			Type: aType,
			Path: path,
			Data: actionData,
		})
	}

	return actions, nil
}
