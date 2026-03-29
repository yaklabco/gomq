package amqp

// Frame is the interface implemented by all AMQP frame types.
type Frame interface {
	frameType() uint8
}

// MethodFrame carries an AMQP method payload.
type MethodFrame struct {
	Channel uint16
	Method  Method
}

func (*MethodFrame) frameType() uint8 { return FrameMethod }

// HeaderFrame carries content header properties.
type HeaderFrame struct {
	Channel    uint16
	ClassID    uint16
	BodySize   uint64
	Properties Properties
}

func (*HeaderFrame) frameType() uint8 { return FrameHeader }

// BodyFrame carries content body data.
type BodyFrame struct {
	Channel uint16
	Body    []byte
}

func (*BodyFrame) frameType() uint8 { return FrameBody }

// HeartbeatFrame is a connection keepalive.
type HeartbeatFrame struct{}

func (*HeartbeatFrame) frameType() uint8 { return FrameHeartbeat }

// frameHeaderSize is the number of bytes in a frame header:
// type(1) + channel(2) + size(4) = 7.
const frameHeaderSize = 7

// classShift is the bit shift to combine/extract class ID in a 32-bit method ID.
const classShift = 16

// methodMask extracts the method ID from a combined 32-bit method ID.
const methodMask = 0xFFFF
