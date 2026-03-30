// Package mqtt implements the MQTT 3.1.1 protocol, bridging MQTT clients
// to the AMQP broker via the amq.topic exchange.
package mqtt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

// MQTT 3.1.1 packet type constants (upper 4 bits of the fixed header).
const (
	TypeConnect     byte = 1
	TypeConnack     byte = 2
	TypePublish     byte = 3
	TypePuback      byte = 4
	TypePubrec      byte = 5
	TypePubrel      byte = 6
	TypePubcomp     byte = 7
	TypeSubscribe   byte = 8
	TypeSuback      byte = 9
	TypeUnsubscribe byte = 10
	TypeUnsuback    byte = 11
	TypePingreq     byte = 12
	TypePingresp    byte = 13
	TypeDisconnect  byte = 14
)

// CONNACK return codes.
const (
	ConnackAccepted          byte = 0
	ConnackUnacceptableProto byte = 1
	ConnackIDRejected        byte = 2
	ConnackServerUnavailable byte = 3
	ConnackBadCredentials    byte = 4
	ConnackNotAuthorized     byte = 5
)

// Fixed header bit layout constants.
const (
	fixedHeaderTypeShift = 4    // packet type is in upper 4 bits
	fixedHeaderFlagsMask = 0x0F // lower 4 bits are flags
	remainLenContBit     = 0x80 // continuation bit in remaining length encoding
	remainLenValueMask   = 0x7F // value bits in remaining length encoding
	remainLenMaxShift    = 28   // 4 bytes * 7 bits each
	packetIDLen          = 2    // packet identifier is 2 bytes
	reservedFlags02      = 0x02 // reserved flags value for SUBSCRIBE/UNSUBSCRIBE/PUBREL
	qosMask              = 0x03 // QoS is 2 bits
	publishDUPFlag       = 0x08 // DUP flag in PUBLISH fixed header
	publishRetainFlag    = 0x01 // RETAIN flag in PUBLISH fixed header
	publishQoSShift      = 1    // QoS shift in PUBLISH fixed header flags
	connackPayloadLen    = 2    // CONNACK variable header is always 2 bytes
	sessionPresentFlag   = 0x01 // session present flag in CONNACK
	connectCleanSession  = 0x02 // clean session flag in CONNECT
	connectWillFlag      = 0x04 // will flag in CONNECT
	connectWillQoSShift  = 3    // will QoS shift in CONNECT flags
	connectWillRetain    = 0x20 // will retain flag in CONNECT
	connectPasswordFlag  = 0x40 // password flag in CONNECT
	connectUsernameFlag  = 0x80 // username flag in CONNECT
)

// Sentinel errors for packet parsing.
var (
	ErrMalformedPacket    = errors.New("malformed packet")
	ErrProtocolViolation  = errors.New("protocol violation")
	ErrInvalidPacketType  = errors.New("invalid packet type")
	ErrRemainingLengthMax = errors.New("remaining length exceeds maximum")
)

// Packet is the interface implemented by all MQTT 3.1.1 control packets.
type Packet interface {
	Type() byte
	Encode(writer io.Writer) error
}

// TopicSubscription pairs a topic filter with a requested QoS level.
type TopicSubscription struct {
	Filter string
	QoS    byte
}

// ReadPacket reads one MQTT packet from reader.
func ReadPacket(reader io.Reader) (Packet, error) {
	// Read fixed header byte.
	var hdr [1]byte
	if _, err := io.ReadFull(reader, hdr[:]); err != nil {
		return nil, fmt.Errorf("read fixed header: %w", err)
	}

	pktType := hdr[0] >> fixedHeaderTypeShift
	flags := hdr[0] & fixedHeaderFlagsMask

	remainLen, err := decodeRemainingLength(reader)
	if err != nil {
		return nil, fmt.Errorf("decode remaining length: %w", err)
	}

	// Read the variable header + payload.
	payload := make([]byte, remainLen)
	if remainLen > 0 {
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("read payload (%d bytes): %w", remainLen, err)
		}
	}

	return decodePacket(pktType, flags, payload)
}

// decodePacket dispatches to the appropriate packet decoder based on type.
func decodePacket(pktType, flags byte, payload []byte) (Packet, error) {
	switch pktType {
	case TypeConnect:
		return decodeConnect(payload)
	case TypeConnack:
		return decodeConnack(payload)
	case TypePublish:
		return decodePublish(flags, payload)
	case TypePuback:
		return decodePuback(payload)
	case TypePubrec:
		return decodePubrec(payload)
	case TypePubrel:
		return decodePubrel(payload)
	case TypePubcomp:
		return decodePubcomp(payload)
	case TypeSubscribe:
		return decodeSubscribe(payload)
	case TypeSuback:
		return decodeSuback(payload)
	case TypeUnsubscribe:
		return decodeUnsubscribe(payload)
	case TypeUnsuback:
		return decodeUnsuback(payload)
	case TypePingreq:
		return &PingreqPacket{}, nil
	case TypePingresp:
		return &PingrespPacket{}, nil
	case TypeDisconnect:
		return &DisconnectPacket{}, nil
	default:
		return nil, fmt.Errorf("type %d: %w", pktType, ErrInvalidPacketType)
	}
}

// --- Variable-length encoding ---

// encodeRemainingLength encodes a value as the MQTT variable-length encoding.
func encodeRemainingLength(value uint32) []byte {
	var buf [4]byte
	idx := 0
	for {
		encoded := byte(value & remainLenValueMask) //nolint:gosec // intentional mask, no overflow
		value >>= 7
		if value > 0 {
			encoded |= remainLenContBit
		}
		buf[idx] = encoded
		idx++
		if value == 0 {
			break
		}
	}
	return buf[:idx]
}

// decodeRemainingLength reads the MQTT variable-length remaining length field.
func decodeRemainingLength(reader io.Reader) (uint32, error) {
	var value uint32
	var oneByte [1]byte
	for shift := 0; shift < remainLenMaxShift; shift += 7 {
		if _, err := io.ReadFull(reader, oneByte[:]); err != nil {
			return 0, fmt.Errorf("read byte at shift %d: %w", shift, err)
		}
		value |= uint32(oneByte[0]&remainLenValueMask) << shift
		if oneByte[0]&remainLenContBit == 0 {
			return value, nil
		}
	}
	return 0, ErrRemainingLengthMax
}

// writeFixedHeader writes the fixed header (type+flags byte + remaining length).
func writeFixedHeader(writer io.Writer, pktType, flags byte, remainLen uint32) error {
	hdr := (pktType << fixedHeaderTypeShift) | (flags & fixedHeaderFlagsMask)
	rl := encodeRemainingLength(remainLen)
	buf := make([]byte, 1+len(rl))
	buf[0] = hdr
	copy(buf[1:], rl)
	_, err := writer.Write(buf)
	if err != nil {
		return fmt.Errorf("write fixed header: %w", err)
	}
	return nil
}

// --- UTF-8 string helpers ---

func encodeMQTTString(str string) []byte {
	buf := make([]byte, packetIDLen+len(str))
	binary.BigEndian.PutUint16(buf, uint16(len(str))) //nolint:gosec // MQTT string max 65535
	copy(buf[packetIDLen:], str)
	return buf
}

func decodeMQTTString(data []byte, offset int) (string, int, error) {
	if offset+packetIDLen > len(data) {
		return "", 0, fmt.Errorf("string length at offset %d: %w", offset, ErrMalformedPacket)
	}
	sLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += packetIDLen
	if offset+sLen > len(data) {
		return "", 0, fmt.Errorf("string data at offset %d (len %d): %w", offset, sLen, ErrMalformedPacket)
	}
	str := string(data[offset : offset+sLen])
	return str, offset + sLen, nil
}

func decodeMQTTBytes(data []byte, offset int) ([]byte, int, error) {
	if offset+packetIDLen > len(data) {
		return nil, 0, fmt.Errorf("bytes length at offset %d: %w", offset, ErrMalformedPacket)
	}
	bLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += packetIDLen
	if offset+bLen > len(data) {
		return nil, 0, fmt.Errorf("bytes data at offset %d (len %d): %w", offset, bLen, ErrMalformedPacket)
	}
	result := make([]byte, bLen)
	copy(result, data[offset:offset+bLen])
	return result, offset + bLen, nil
}

// --- CONNECT ---

// ConnectPacket represents an MQTT CONNECT packet.
type ConnectPacket struct {
	ProtocolName  string
	ProtocolLevel byte
	CleanSession  bool
	KeepAlive     uint16
	ClientID      string
	WillTopic     string
	WillMessage   []byte
	WillQoS       byte
	WillRetain    bool
	Username      string
	Password      []byte
}

// Type returns the MQTT packet type for CONNECT.
func (p *ConnectPacket) Type() byte { return TypeConnect }

// Encode serializes the CONNECT packet to writer.
func (p *ConnectPacket) Encode(writer io.Writer) error {
	var payload []byte

	// Variable header: protocol name + level + flags + keepalive.
	payload = append(payload, encodeMQTTString(p.ProtocolName)...)
	payload = append(payload, p.ProtocolLevel)

	var flags byte
	if p.CleanSession {
		flags |= connectCleanSession
	}
	hasWill := p.WillTopic != ""
	if hasWill {
		flags |= connectWillFlag
		flags |= (p.WillQoS & qosMask) << connectWillQoSShift
		if p.WillRetain {
			flags |= connectWillRetain
		}
	}
	if len(p.Password) > 0 {
		flags |= connectPasswordFlag
	}
	if p.Username != "" {
		flags |= connectUsernameFlag
	}
	payload = append(payload, flags)

	var ka [packetIDLen]byte
	binary.BigEndian.PutUint16(ka[:], p.KeepAlive)
	payload = append(payload, ka[:]...)

	// Payload: client ID, will topic, will message, username, password.
	payload = append(payload, encodeMQTTString(p.ClientID)...)
	if hasWill {
		payload = append(payload, encodeMQTTString(p.WillTopic)...)
		willBytes := make([]byte, packetIDLen+len(p.WillMessage))
		binary.BigEndian.PutUint16(willBytes, uint16(len(p.WillMessage))) //nolint:gosec // MQTT string max 65535
		copy(willBytes[packetIDLen:], p.WillMessage)
		payload = append(payload, willBytes...)
	}
	if p.Username != "" {
		payload = append(payload, encodeMQTTString(p.Username)...)
	}
	if len(p.Password) > 0 {
		pwBytes := make([]byte, packetIDLen+len(p.Password))
		binary.BigEndian.PutUint16(pwBytes, uint16(len(p.Password))) //nolint:gosec // MQTT binary max 65535
		copy(pwBytes[packetIDLen:], p.Password)
		payload = append(payload, pwBytes...)
	}

	if err := writeFixedHeader(writer, TypeConnect, 0, uint32(len(payload))); err != nil { //nolint:gosec // payload bounded
		return err
	}
	_, err := writer.Write(payload)
	if err != nil {
		return fmt.Errorf("write connect payload: %w", err)
	}
	return nil
}

func decodeConnect(data []byte) (*ConnectPacket, error) {
	pkt := &ConnectPacket{}
	offset := 0

	var err error
	pkt.ProtocolName, offset, err = decodeMQTTString(data, offset)
	if err != nil {
		return nil, fmt.Errorf("decode connect protocol name: %w", err)
	}

	if offset >= len(data) {
		return nil, fmt.Errorf("decode connect protocol level: %w", ErrMalformedPacket)
	}
	pkt.ProtocolLevel = data[offset]
	offset++

	if offset >= len(data) {
		return nil, fmt.Errorf("decode connect flags: %w", ErrMalformedPacket)
	}
	flags := data[offset]
	offset++

	pkt.CleanSession = flags&connectCleanSession != 0
	hasWill := flags&connectWillFlag != 0
	pkt.WillQoS = (flags >> connectWillQoSShift) & qosMask
	pkt.WillRetain = flags&connectWillRetain != 0
	hasPassword := flags&connectPasswordFlag != 0
	hasUsername := flags&connectUsernameFlag != 0

	if offset+packetIDLen > len(data) {
		return nil, fmt.Errorf("decode connect keepalive: %w", ErrMalformedPacket)
	}
	pkt.KeepAlive = binary.BigEndian.Uint16(data[offset:])
	offset += packetIDLen

	pkt.ClientID, offset, err = decodeMQTTString(data, offset)
	if err != nil {
		return nil, fmt.Errorf("decode connect client ID: %w", err)
	}

	if hasWill {
		pkt.WillTopic, offset, err = decodeMQTTString(data, offset)
		if err != nil {
			return nil, fmt.Errorf("decode connect will topic: %w", err)
		}
		pkt.WillMessage, offset, err = decodeMQTTBytes(data, offset)
		if err != nil {
			return nil, fmt.Errorf("decode connect will message: %w", err)
		}
	}

	if hasUsername {
		pkt.Username, offset, err = decodeMQTTString(data, offset)
		if err != nil {
			return nil, fmt.Errorf("decode connect username: %w", err)
		}
	}

	if hasPassword {
		pkt.Password, _, err = decodeMQTTBytes(data, offset)
		if err != nil {
			return nil, fmt.Errorf("decode connect password: %w", err)
		}
	}

	return pkt, nil
}

// --- CONNACK ---

// ConnackPacket represents an MQTT CONNACK packet.
type ConnackPacket struct {
	SessionPresent bool
	ReturnCode     byte
}

// Type returns the MQTT packet type for CONNACK.
func (p *ConnackPacket) Type() byte { return TypeConnack }

// Encode serializes the CONNACK packet to writer.
func (p *ConnackPacket) Encode(writer io.Writer) error {
	if err := writeFixedHeader(writer, TypeConnack, 0, connackPayloadLen); err != nil {
		return err
	}
	var flags byte
	if p.SessionPresent {
		flags = sessionPresentFlag
	}
	_, err := writer.Write([]byte{flags, p.ReturnCode})
	if err != nil {
		return fmt.Errorf("write connack payload: %w", err)
	}
	return nil
}

func decodeConnack(data []byte) (*ConnackPacket, error) {
	if len(data) < connackPayloadLen {
		return nil, fmt.Errorf("connack too short (%d bytes): %w", len(data), ErrMalformedPacket)
	}
	return &ConnackPacket{
		SessionPresent: data[0]&sessionPresentFlag != 0,
		ReturnCode:     data[1],
	}, nil
}

// --- PUBLISH ---

// PublishPacket represents an MQTT PUBLISH packet.
type PublishPacket struct {
	TopicName string
	PacketID  uint16
	Payload   []byte
	QoS       byte
	Retain    bool
	DUP       bool
}

// Type returns the MQTT packet type for PUBLISH.
func (p *PublishPacket) Type() byte { return TypePublish }

// Encode serializes the PUBLISH packet to writer.
func (p *PublishPacket) Encode(writer io.Writer) error {
	varHeader := encodeMQTTString(p.TopicName)
	if p.QoS > 0 {
		var pid [packetIDLen]byte
		binary.BigEndian.PutUint16(pid[:], p.PacketID)
		varHeader = append(varHeader, pid[:]...)
	}

	fullPayload := make([]byte, 0, len(varHeader)+len(p.Payload))
	fullPayload = append(fullPayload, varHeader...)
	fullPayload = append(fullPayload, p.Payload...)

	var flags byte
	if p.DUP {
		flags |= publishDUPFlag
	}
	flags |= (p.QoS & qosMask) << publishQoSShift
	if p.Retain {
		flags |= publishRetainFlag
	}

	if err := writeFixedHeader(writer, TypePublish, flags, uint32(len(fullPayload))); err != nil { //nolint:gosec // payload bounded
		return err
	}
	_, err := writer.Write(fullPayload)
	if err != nil {
		return fmt.Errorf("write publish payload: %w", err)
	}
	return nil
}

func decodePublish(flags byte, data []byte) (*PublishPacket, error) {
	pkt := &PublishPacket{
		DUP:    flags&publishDUPFlag != 0,
		QoS:    (flags >> publishQoSShift) & qosMask,
		Retain: flags&publishRetainFlag != 0,
	}

	offset := 0
	var err error
	pkt.TopicName, offset, err = decodeMQTTString(data, offset)
	if err != nil {
		return nil, fmt.Errorf("decode publish topic: %w", err)
	}

	if pkt.QoS > 0 {
		if offset+packetIDLen > len(data) {
			return nil, fmt.Errorf("decode publish packet ID: %w", ErrMalformedPacket)
		}
		pkt.PacketID = binary.BigEndian.Uint16(data[offset:])
		offset += packetIDLen
	}

	if offset < len(data) {
		pkt.Payload = make([]byte, len(data)-offset)
		copy(pkt.Payload, data[offset:])
	}

	return pkt, nil
}

// --- PUBACK ---

// PubackPacket represents an MQTT PUBACK packet (QoS 1 acknowledgement).
type PubackPacket struct {
	PacketID uint16
}

// Type returns the MQTT packet type for PUBACK.
func (p *PubackPacket) Type() byte { return TypePuback }

// Encode serializes the PUBACK packet to writer.
func (p *PubackPacket) Encode(writer io.Writer) error {
	return encodePacketIDOnly(writer, TypePuback, 0, p.PacketID)
}

func decodePuback(data []byte) (*PubackPacket, error) {
	id, err := decodePacketID(data)
	if err != nil {
		return nil, fmt.Errorf("decode puback: %w", err)
	}
	return &PubackPacket{PacketID: id}, nil
}

// --- PUBREC ---

// PubrecPacket represents an MQTT PUBREC packet (QoS 2, step 1).
type PubrecPacket struct {
	PacketID uint16
}

// Type returns the MQTT packet type for PUBREC.
func (p *PubrecPacket) Type() byte { return TypePubrec }

// Encode serializes the PUBREC packet to writer.
func (p *PubrecPacket) Encode(writer io.Writer) error {
	return encodePacketIDOnly(writer, TypePubrec, 0, p.PacketID)
}

func decodePubrec(data []byte) (*PubrecPacket, error) {
	id, err := decodePacketID(data)
	if err != nil {
		return nil, fmt.Errorf("decode pubrec: %w", err)
	}
	return &PubrecPacket{PacketID: id}, nil
}

// --- PUBREL ---

// PubrelPacket represents an MQTT PUBREL packet (QoS 2, step 2).
type PubrelPacket struct {
	PacketID uint16
}

// Type returns the MQTT packet type for PUBREL.
func (p *PubrelPacket) Type() byte { return TypePubrel }

// Encode serializes the PUBREL packet to writer.
func (p *PubrelPacket) Encode(writer io.Writer) error {
	// PUBREL has reserved flags 0x02 per MQTT 3.1.1 spec.
	return encodePacketIDOnly(writer, TypePubrel, reservedFlags02, p.PacketID)
}

func decodePubrel(data []byte) (*PubrelPacket, error) {
	id, err := decodePacketID(data)
	if err != nil {
		return nil, fmt.Errorf("decode pubrel: %w", err)
	}
	return &PubrelPacket{PacketID: id}, nil
}

// --- PUBCOMP ---

// PubcompPacket represents an MQTT PUBCOMP packet (QoS 2, step 3).
type PubcompPacket struct {
	PacketID uint16
}

// Type returns the MQTT packet type for PUBCOMP.
func (p *PubcompPacket) Type() byte { return TypePubcomp }

// Encode serializes the PUBCOMP packet to writer.
func (p *PubcompPacket) Encode(writer io.Writer) error {
	return encodePacketIDOnly(writer, TypePubcomp, 0, p.PacketID)
}

func decodePubcomp(data []byte) (*PubcompPacket, error) {
	id, err := decodePacketID(data)
	if err != nil {
		return nil, fmt.Errorf("decode pubcomp: %w", err)
	}
	return &PubcompPacket{PacketID: id}, nil
}

// --- SUBSCRIBE ---

// SubscribePacket represents an MQTT SUBSCRIBE packet.
type SubscribePacket struct {
	PacketID uint16
	Topics   []TopicSubscription
}

// Type returns the MQTT packet type for SUBSCRIBE.
func (p *SubscribePacket) Type() byte { return TypeSubscribe }

// Encode serializes the SUBSCRIBE packet to writer.
func (p *SubscribePacket) Encode(writer io.Writer) error {
	// Pre-calculate payload size: 2 (packet ID) + sum of (2 + len(filter) + 1) per topic.
	payloadSize := packetIDLen
	for _, ts := range p.Topics {
		payloadSize += packetIDLen + len(ts.Filter) + 1
	}
	payload := make([]byte, 0, payloadSize)

	var pid [packetIDLen]byte
	binary.BigEndian.PutUint16(pid[:], p.PacketID)
	payload = append(payload, pid[:]...)

	for _, ts := range p.Topics {
		payload = append(payload, encodeMQTTString(ts.Filter)...)
		payload = append(payload, ts.QoS)
	}

	// SUBSCRIBE has reserved flags 0x02 per MQTT 3.1.1 spec.
	if err := writeFixedHeader(writer, TypeSubscribe, reservedFlags02, uint32(len(payload))); err != nil { //nolint:gosec // payload bounded
		return err
	}
	_, err := writer.Write(payload)
	if err != nil {
		return fmt.Errorf("write subscribe payload: %w", err)
	}
	return nil
}

func decodeSubscribe(data []byte) (*SubscribePacket, error) {
	if len(data) < packetIDLen {
		return nil, fmt.Errorf("subscribe too short (%d bytes): %w", len(data), ErrMalformedPacket)
	}

	pkt := &SubscribePacket{
		PacketID: binary.BigEndian.Uint16(data),
	}
	offset := packetIDLen

	for offset < len(data) {
		var filter string
		var err error
		filter, offset, err = decodeMQTTString(data, offset)
		if err != nil {
			return nil, fmt.Errorf("decode subscribe topic filter: %w", err)
		}
		if offset >= len(data) {
			return nil, fmt.Errorf("decode subscribe QoS: %w", ErrMalformedPacket)
		}
		qos := data[offset]
		offset++
		pkt.Topics = append(pkt.Topics, TopicSubscription{Filter: filter, QoS: qos})
	}

	return pkt, nil
}

// --- SUBACK ---

// SubackPacket represents an MQTT SUBACK packet.
type SubackPacket struct {
	PacketID    uint16
	ReturnCodes []byte
}

// Type returns the MQTT packet type for SUBACK.
func (p *SubackPacket) Type() byte { return TypeSuback }

// Encode serializes the SUBACK packet to writer.
func (p *SubackPacket) Encode(writer io.Writer) error {
	payload := make([]byte, packetIDLen+len(p.ReturnCodes))
	binary.BigEndian.PutUint16(payload, p.PacketID)
	copy(payload[packetIDLen:], p.ReturnCodes)

	if err := writeFixedHeader(writer, TypeSuback, 0, uint32(len(payload))); err != nil { //nolint:gosec // payload bounded
		return err
	}
	_, err := writer.Write(payload)
	if err != nil {
		return fmt.Errorf("write suback payload: %w", err)
	}
	return nil
}

func decodeSuback(data []byte) (*SubackPacket, error) {
	if len(data) < packetIDLen {
		return nil, fmt.Errorf("suback too short (%d bytes): %w", len(data), ErrMalformedPacket)
	}
	pkt := &SubackPacket{
		PacketID:    binary.BigEndian.Uint16(data),
		ReturnCodes: make([]byte, len(data)-packetIDLen),
	}
	copy(pkt.ReturnCodes, data[packetIDLen:])
	return pkt, nil
}

// --- UNSUBSCRIBE ---

// UnsubscribePacket represents an MQTT UNSUBSCRIBE packet.
type UnsubscribePacket struct {
	PacketID uint16
	Topics   []string
}

// Type returns the MQTT packet type for UNSUBSCRIBE.
func (p *UnsubscribePacket) Type() byte { return TypeUnsubscribe }

// Encode serializes the UNSUBSCRIBE packet to writer.
func (p *UnsubscribePacket) Encode(writer io.Writer) error {
	payloadSize := packetIDLen
	for _, topic := range p.Topics {
		payloadSize += packetIDLen + len(topic)
	}
	payload := make([]byte, 0, payloadSize)

	var pid [packetIDLen]byte
	binary.BigEndian.PutUint16(pid[:], p.PacketID)
	payload = append(payload, pid[:]...)

	for _, topic := range p.Topics {
		payload = append(payload, encodeMQTTString(topic)...)
	}

	// UNSUBSCRIBE has reserved flags 0x02 per MQTT 3.1.1 spec.
	if err := writeFixedHeader(writer, TypeUnsubscribe, reservedFlags02, uint32(len(payload))); err != nil { //nolint:gosec // payload bounded
		return err
	}
	_, err := writer.Write(payload)
	if err != nil {
		return fmt.Errorf("write unsubscribe payload: %w", err)
	}
	return nil
}

func decodeUnsubscribe(data []byte) (*UnsubscribePacket, error) {
	if len(data) < packetIDLen {
		return nil, fmt.Errorf("unsubscribe too short (%d bytes): %w", len(data), ErrMalformedPacket)
	}

	pkt := &UnsubscribePacket{
		PacketID: binary.BigEndian.Uint16(data),
	}
	offset := packetIDLen

	for offset < len(data) {
		var topic string
		var err error
		topic, offset, err = decodeMQTTString(data, offset)
		if err != nil {
			return nil, fmt.Errorf("decode unsubscribe topic: %w", err)
		}
		pkt.Topics = append(pkt.Topics, topic)
	}

	return pkt, nil
}

// --- UNSUBACK ---

// UnsubackPacket represents an MQTT UNSUBACK packet.
type UnsubackPacket struct {
	PacketID uint16
}

// Type returns the MQTT packet type for UNSUBACK.
func (p *UnsubackPacket) Type() byte { return TypeUnsuback }

// Encode serializes the UNSUBACK packet to writer.
func (p *UnsubackPacket) Encode(writer io.Writer) error {
	return encodePacketIDOnly(writer, TypeUnsuback, 0, p.PacketID)
}

func decodeUnsuback(data []byte) (*UnsubackPacket, error) {
	id, err := decodePacketID(data)
	if err != nil {
		return nil, fmt.Errorf("decode unsuback: %w", err)
	}
	return &UnsubackPacket{PacketID: id}, nil
}

// --- PINGREQ ---

// PingreqPacket represents an MQTT PINGREQ packet.
type PingreqPacket struct{}

// Type returns the MQTT packet type for PINGREQ.
func (p *PingreqPacket) Type() byte { return TypePingreq }

// Encode serializes the PINGREQ packet to writer.
func (p *PingreqPacket) Encode(writer io.Writer) error {
	return writeFixedHeader(writer, TypePingreq, 0, 0)
}

// --- PINGRESP ---

// PingrespPacket represents an MQTT PINGRESP packet.
type PingrespPacket struct{}

// Type returns the MQTT packet type for PINGRESP.
func (p *PingrespPacket) Type() byte { return TypePingresp }

// Encode serializes the PINGRESP packet to writer.
func (p *PingrespPacket) Encode(writer io.Writer) error {
	return writeFixedHeader(writer, TypePingresp, 0, 0)
}

// --- DISCONNECT ---

// DisconnectPacket represents an MQTT DISCONNECT packet.
type DisconnectPacket struct{}

// Type returns the MQTT packet type for DISCONNECT.
func (p *DisconnectPacket) Type() byte { return TypeDisconnect }

// Encode serializes the DISCONNECT packet to writer.
func (p *DisconnectPacket) Encode(writer io.Writer) error {
	return writeFixedHeader(writer, TypeDisconnect, 0, 0)
}

// --- Helpers ---

// encodePacketIDOnly encodes a packet that contains only a packet ID.
func encodePacketIDOnly(writer io.Writer, pktType, flags byte, packetID uint16) error {
	if err := writeFixedHeader(writer, pktType, flags, packetIDLen); err != nil {
		return err
	}
	var pid [packetIDLen]byte
	binary.BigEndian.PutUint16(pid[:], packetID)
	_, err := writer.Write(pid[:])
	if err != nil {
		return fmt.Errorf("write packet ID: %w", err)
	}
	return nil
}

// decodePacketID reads a 2-byte packet identifier from data.
func decodePacketID(data []byte) (uint16, error) {
	if len(data) < packetIDLen {
		return 0, fmt.Errorf("packet ID too short (%d bytes): %w", len(data), ErrMalformedPacket)
	}
	return binary.BigEndian.Uint16(data), nil
}

// --- Topic conversion ---

// MQTTTopicToAMQP converts an MQTT topic or filter to an AMQP routing key.
// MQTT '/' becomes '.', '+' becomes '*', '#' stays '#'.
func MQTTTopicToAMQP(topic string) string {
	replacer := strings.NewReplacer("/", ".", "+", "*")
	return replacer.Replace(topic)
}

// AMQPTopicToMQTT converts an AMQP routing key to an MQTT topic.
// AMQP '.' becomes '/', '*' becomes '+', '#' stays '#'.
func AMQPTopicToMQTT(key string) string {
	replacer := strings.NewReplacer(".", "/", "*", "+")
	return replacer.Replace(key)
}
