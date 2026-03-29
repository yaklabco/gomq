package amqp

import (
	"fmt"
	"io"
)

// Property presence flags (bit positions within the 16-bit flags field).
// Bit 15 is the most significant, bit 0 is the least significant.
const (
	flagContentType     uint16 = 1 << 15
	flagContentEncoding uint16 = 1 << 14
	flagHeaders         uint16 = 1 << 13
	flagDeliveryMode    uint16 = 1 << 12
	flagPriority        uint16 = 1 << 11
	flagCorrelationID   uint16 = 1 << 10
	flagReplyTo         uint16 = 1 << 9
	flagExpiration      uint16 = 1 << 8
	flagMessageID       uint16 = 1 << 7
	flagTimestamp       uint16 = 1 << 6
	flagType            uint16 = 1 << 5
	flagUserID          uint16 = 1 << 4
	flagAppID           uint16 = 1 << 3
	flagReserved        uint16 = 1 << 2
)

// Properties represents AMQP basic content header properties.
type Properties struct {
	ContentType     string // shortstr, bit 15
	ContentEncoding string // shortstr, bit 14
	Headers         Table  // table, bit 13
	DeliveryMode    uint8  // octet, bit 12
	Priority        uint8  // octet, bit 11
	CorrelationID   string // shortstr, bit 10
	ReplyTo         string // shortstr, bit 9
	Expiration      string // shortstr, bit 8
	MessageID       string // shortstr, bit 7
	Timestamp       uint64 // uint64, bit 6
	Type            string // shortstr, bit 5
	UserID          string // shortstr, bit 4
	AppID           string // shortstr, bit 3
	Reserved        string // shortstr, bit 2
}

// Flags computes the presence bitfield for the properties.
func (p *Properties) Flags() uint16 {
	var flags uint16
	if p.ContentType != "" {
		flags |= flagContentType
	}
	if p.ContentEncoding != "" {
		flags |= flagContentEncoding
	}
	if len(p.Headers) > 0 {
		flags |= flagHeaders
	}
	if p.DeliveryMode != 0 {
		flags |= flagDeliveryMode
	}
	if p.Priority != 0 {
		flags |= flagPriority
	}
	if p.CorrelationID != "" {
		flags |= flagCorrelationID
	}
	if p.ReplyTo != "" {
		flags |= flagReplyTo
	}
	if p.Expiration != "" {
		flags |= flagExpiration
	}
	if p.MessageID != "" {
		flags |= flagMessageID
	}
	if p.Timestamp != 0 {
		flags |= flagTimestamp
	}
	if p.Type != "" {
		flags |= flagType
	}
	if p.UserID != "" {
		flags |= flagUserID
	}
	if p.AppID != "" {
		flags |= flagAppID
	}
	if p.Reserved != "" {
		flags |= flagReserved
	}
	return flags
}

// Write encodes the properties to the writer (flags + present fields).
func (p *Properties) Write(w io.Writer) error {
	wt := &wireWriter{wt: w}
	flags := p.Flags()

	if err := wt.writeUint16(flags); err != nil {
		return fmt.Errorf("properties write flags: %w", err)
	}
	if flags&flagContentType != 0 {
		if err := wt.writeShortstr(p.ContentType); err != nil {
			return fmt.Errorf("properties write content-type: %w", err)
		}
	}
	if flags&flagContentEncoding != 0 {
		if err := wt.writeShortstr(p.ContentEncoding); err != nil {
			return fmt.Errorf("properties write content-encoding: %w", err)
		}
	}
	if flags&flagHeaders != 0 {
		if err := wt.writeTable(p.Headers); err != nil {
			return fmt.Errorf("properties write headers: %w", err)
		}
	}
	if flags&flagDeliveryMode != 0 {
		if err := wt.writeUint8(p.DeliveryMode); err != nil {
			return fmt.Errorf("properties write delivery-mode: %w", err)
		}
	}
	if flags&flagPriority != 0 {
		if err := wt.writeUint8(p.Priority); err != nil {
			return fmt.Errorf("properties write priority: %w", err)
		}
	}
	if flags&flagCorrelationID != 0 {
		if err := wt.writeShortstr(p.CorrelationID); err != nil {
			return fmt.Errorf("properties write correlation-id: %w", err)
		}
	}
	if flags&flagReplyTo != 0 {
		if err := wt.writeShortstr(p.ReplyTo); err != nil {
			return fmt.Errorf("properties write reply-to: %w", err)
		}
	}
	if flags&flagExpiration != 0 {
		if err := wt.writeShortstr(p.Expiration); err != nil {
			return fmt.Errorf("properties write expiration: %w", err)
		}
	}
	if flags&flagMessageID != 0 {
		if err := wt.writeShortstr(p.MessageID); err != nil {
			return fmt.Errorf("properties write message-id: %w", err)
		}
	}
	if flags&flagTimestamp != 0 {
		if err := wt.writeUint64(p.Timestamp); err != nil {
			return fmt.Errorf("properties write timestamp: %w", err)
		}
	}
	if flags&flagType != 0 {
		if err := wt.writeShortstr(p.Type); err != nil {
			return fmt.Errorf("properties write type: %w", err)
		}
	}
	if flags&flagUserID != 0 {
		if err := wt.writeShortstr(p.UserID); err != nil {
			return fmt.Errorf("properties write user-id: %w", err)
		}
	}
	if flags&flagAppID != 0 {
		if err := wt.writeShortstr(p.AppID); err != nil {
			return fmt.Errorf("properties write app-id: %w", err)
		}
	}
	if flags&flagReserved != 0 {
		if err := wt.writeShortstr(p.Reserved); err != nil {
			return fmt.Errorf("properties write reserved: %w", err)
		}
	}
	return nil
}

// Read decodes the properties from the reader (flags + present fields).
func (p *Properties) Read(r io.Reader) error {
	rd := &wireReader{rd: r}

	flags, err := rd.readUint16()
	if err != nil {
		return fmt.Errorf("properties read flags: %w", err)
	}
	if flags&flagContentType != 0 {
		if p.ContentType, err = rd.readShortstr(); err != nil {
			return fmt.Errorf("properties read content-type: %w", err)
		}
	}
	if flags&flagContentEncoding != 0 {
		if p.ContentEncoding, err = rd.readShortstr(); err != nil {
			return fmt.Errorf("properties read content-encoding: %w", err)
		}
	}
	if flags&flagHeaders != 0 {
		if p.Headers, err = rd.readTable(); err != nil {
			return fmt.Errorf("properties read headers: %w", err)
		}
	}
	if flags&flagDeliveryMode != 0 {
		if p.DeliveryMode, err = rd.readUint8(); err != nil {
			return fmt.Errorf("properties read delivery-mode: %w", err)
		}
	}
	if flags&flagPriority != 0 {
		if p.Priority, err = rd.readUint8(); err != nil {
			return fmt.Errorf("properties read priority: %w", err)
		}
	}
	if flags&flagCorrelationID != 0 {
		if p.CorrelationID, err = rd.readShortstr(); err != nil {
			return fmt.Errorf("properties read correlation-id: %w", err)
		}
	}
	if flags&flagReplyTo != 0 {
		if p.ReplyTo, err = rd.readShortstr(); err != nil {
			return fmt.Errorf("properties read reply-to: %w", err)
		}
	}
	if flags&flagExpiration != 0 {
		if p.Expiration, err = rd.readShortstr(); err != nil {
			return fmt.Errorf("properties read expiration: %w", err)
		}
	}
	if flags&flagMessageID != 0 {
		if p.MessageID, err = rd.readShortstr(); err != nil {
			return fmt.Errorf("properties read message-id: %w", err)
		}
	}
	if flags&flagTimestamp != 0 {
		if p.Timestamp, err = rd.readUint64(); err != nil {
			return fmt.Errorf("properties read timestamp: %w", err)
		}
	}
	if flags&flagType != 0 {
		if p.Type, err = rd.readShortstr(); err != nil {
			return fmt.Errorf("properties read type: %w", err)
		}
	}
	if flags&flagUserID != 0 {
		if p.UserID, err = rd.readShortstr(); err != nil {
			return fmt.Errorf("properties read user-id: %w", err)
		}
	}
	if flags&flagAppID != 0 {
		if p.AppID, err = rd.readShortstr(); err != nil {
			return fmt.Errorf("properties read app-id: %w", err)
		}
	}
	if flags&flagReserved != 0 {
		if p.Reserved, err = rd.readShortstr(); err != nil {
			return fmt.Errorf("properties read reserved: %w", err)
		}
	}
	return nil
}

// ByteSize returns the encoded size in bytes (flags + present fields).
func (p *Properties) ByteSize() int {
	size := sizeUint16 // flags
	flags := p.Flags()

	if flags&flagContentType != 0 {
		size += 1 + len(p.ContentType)
	}
	if flags&flagContentEncoding != 0 {
		size += 1 + len(p.ContentEncoding)
	}
	if flags&flagHeaders != 0 {
		// Estimate: marshal to scratch buffer.
		buf := make([]byte, tableBufferSize)
		wrote, err := marshalTable(buf, p.Headers)
		if err == nil {
			size += wrote
		}
	}
	if flags&flagDeliveryMode != 0 {
		size += sizeUint8
	}
	if flags&flagPriority != 0 {
		size += sizeUint8
	}
	if flags&flagCorrelationID != 0 {
		size += 1 + len(p.CorrelationID)
	}
	if flags&flagReplyTo != 0 {
		size += 1 + len(p.ReplyTo)
	}
	if flags&flagExpiration != 0 {
		size += 1 + len(p.Expiration)
	}
	if flags&flagMessageID != 0 {
		size += 1 + len(p.MessageID)
	}
	if flags&flagTimestamp != 0 {
		size += sizeUint64
	}
	if flags&flagType != 0 {
		size += 1 + len(p.Type)
	}
	if flags&flagUserID != 0 {
		size += 1 + len(p.UserID)
	}
	if flags&flagAppID != 0 {
		size += 1 + len(p.AppID)
	}
	if flags&flagReserved != 0 {
		size += 1 + len(p.Reserved)
	}
	return size
}
