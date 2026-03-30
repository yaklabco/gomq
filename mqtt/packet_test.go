package mqtt

import (
	"bytes"
	"testing"
)

func TestEncodeRemainingLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value uint32
		want  []byte
	}{
		{name: "zero", value: 0, want: []byte{0x00}},
		{name: "single byte max", value: 127, want: []byte{0x7F}},
		{name: "two bytes min", value: 128, want: []byte{0x80, 0x01}},
		{name: "two bytes max", value: 16383, want: []byte{0xFF, 0x7F}},
		{name: "three bytes min", value: 16384, want: []byte{0x80, 0x80, 0x01}},
		{name: "three bytes max", value: 2097151, want: []byte{0xFF, 0xFF, 0x7F}},
		{name: "four bytes min", value: 2097152, want: []byte{0x80, 0x80, 0x80, 0x01}},
		{name: "four bytes max", value: 268435455, want: []byte{0xFF, 0xFF, 0xFF, 0x7F}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := encodeRemainingLength(tt.value)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("encodeRemainingLength(%d) = %x, want %x", tt.value, got, tt.want)
			}
		})
	}
}

func TestDecodeRemainingLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   []byte
		want    uint32
		wantErr bool
	}{
		{name: "zero", input: []byte{0x00}, want: 0},
		{name: "single byte max", input: []byte{0x7F}, want: 127},
		{name: "two bytes", input: []byte{0x80, 0x01}, want: 128},
		{name: "four bytes max", input: []byte{0xFF, 0xFF, 0xFF, 0x7F}, want: 268435455},
		{name: "malformed too many continuation bytes", input: []byte{0x80, 0x80, 0x80, 0x80, 0x01}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reader := bytes.NewReader(tt.input)
			got, err := decodeRemainingLength(reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeRemainingLength() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("decodeRemainingLength() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestConnectPacketRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pkt  *ConnectPacket
	}{
		{
			name: "minimal",
			pkt: &ConnectPacket{
				ProtocolName:  "MQTT",
				ProtocolLevel: 4,
				ClientID:      "test-client",
				KeepAlive:     60,
			},
		},
		{
			name: "with credentials",
			pkt: &ConnectPacket{
				ProtocolName:  "MQTT",
				ProtocolLevel: 4,
				ClientID:      "client-1",
				KeepAlive:     120,
				Username:      "user",
				Password:      []byte("pass"),
				CleanSession:  true,
			},
		},
		{
			name: "with will",
			pkt: &ConnectPacket{
				ProtocolName:  "MQTT",
				ProtocolLevel: 4,
				ClientID:      "client-2",
				KeepAlive:     30,
				WillTopic:     "will/topic",
				WillMessage:   []byte("goodbye"),
				WillQoS:       1,
				WillRetain:    true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			if err := tt.pkt.Encode(&buf); err != nil {
				t.Fatalf("Encode() error: %v", err)
			}

			pkt, err := ReadPacket(&buf)
			if err != nil {
				t.Fatalf("ReadPacket() error: %v", err)
			}

			got, ok := pkt.(*ConnectPacket)
			if !ok {
				t.Fatalf("ReadPacket() returned %T, want *ConnectPacket", pkt)
			}

			if got.ClientID != tt.pkt.ClientID {
				t.Errorf("ClientID = %q, want %q", got.ClientID, tt.pkt.ClientID)
			}
			if got.KeepAlive != tt.pkt.KeepAlive {
				t.Errorf("KeepAlive = %d, want %d", got.KeepAlive, tt.pkt.KeepAlive)
			}
			if got.CleanSession != tt.pkt.CleanSession {
				t.Errorf("CleanSession = %v, want %v", got.CleanSession, tt.pkt.CleanSession)
			}
			if got.Username != tt.pkt.Username {
				t.Errorf("Username = %q, want %q", got.Username, tt.pkt.Username)
			}
			if !bytes.Equal(got.Password, tt.pkt.Password) {
				t.Errorf("Password = %q, want %q", got.Password, tt.pkt.Password)
			}
			if got.WillTopic != tt.pkt.WillTopic {
				t.Errorf("WillTopic = %q, want %q", got.WillTopic, tt.pkt.WillTopic)
			}
			if !bytes.Equal(got.WillMessage, tt.pkt.WillMessage) {
				t.Errorf("WillMessage = %q, want %q", got.WillMessage, tt.pkt.WillMessage)
			}
			if got.WillQoS != tt.pkt.WillQoS {
				t.Errorf("WillQoS = %d, want %d", got.WillQoS, tt.pkt.WillQoS)
			}
			if got.WillRetain != tt.pkt.WillRetain {
				t.Errorf("WillRetain = %v, want %v", got.WillRetain, tt.pkt.WillRetain)
			}
		})
	}
}

func TestConnackPacketRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pkt  *ConnackPacket
	}{
		{name: "accepted", pkt: &ConnackPacket{SessionPresent: false, ReturnCode: ConnackAccepted}},
		{name: "bad credentials", pkt: &ConnackPacket{SessionPresent: false, ReturnCode: ConnackBadCredentials}},
		{name: "session present", pkt: &ConnackPacket{SessionPresent: true, ReturnCode: ConnackAccepted}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertRoundtrip(t, tt.pkt, func(pkt Packet) {
				got := requireType[*ConnackPacket](t, pkt)
				if got.SessionPresent != tt.pkt.SessionPresent {
					t.Errorf("SessionPresent = %v, want %v", got.SessionPresent, tt.pkt.SessionPresent)
				}
				if got.ReturnCode != tt.pkt.ReturnCode {
					t.Errorf("ReturnCode = %d, want %d", got.ReturnCode, tt.pkt.ReturnCode)
				}
			})
		})
	}
}

func TestPublishPacketRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pkt  *PublishPacket
	}{
		{
			name: "qos 0",
			pkt: &PublishPacket{
				TopicName: "test/topic",
				Payload:   []byte("hello"),
				QoS:       0,
			},
		},
		{
			name: "qos 1",
			pkt: &PublishPacket{
				TopicName: "a/b/c",
				PacketID:  42,
				Payload:   []byte("world"),
				QoS:       1,
			},
		},
		{
			name: "qos 2 with retain and dup",
			pkt: &PublishPacket{
				TopicName: "sensors/temp",
				PacketID:  1000,
				Payload:   []byte{0x01, 0x02, 0x03},
				QoS:       2,
				Retain:    true,
				DUP:       true,
			},
		},
		{
			name: "empty payload",
			pkt: &PublishPacket{
				TopicName: "empty",
				QoS:       0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertRoundtrip(t, tt.pkt, func(pkt Packet) {
				got := requireType[*PublishPacket](t, pkt)
				if got.TopicName != tt.pkt.TopicName {
					t.Errorf("TopicName = %q, want %q", got.TopicName, tt.pkt.TopicName)
				}
				if got.QoS != tt.pkt.QoS {
					t.Errorf("QoS = %d, want %d", got.QoS, tt.pkt.QoS)
				}
				if got.PacketID != tt.pkt.PacketID {
					t.Errorf("PacketID = %d, want %d", got.PacketID, tt.pkt.PacketID)
				}
				if !bytes.Equal(got.Payload, tt.pkt.Payload) {
					t.Errorf("Payload = %x, want %x", got.Payload, tt.pkt.Payload)
				}
				if got.Retain != tt.pkt.Retain {
					t.Errorf("Retain = %v, want %v", got.Retain, tt.pkt.Retain)
				}
				if got.DUP != tt.pkt.DUP {
					t.Errorf("DUP = %v, want %v", got.DUP, tt.pkt.DUP)
				}
			})
		})
	}
}

func TestPubackPacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &PubackPacket{PacketID: 12345}
	assertRoundtrip(t, pkt, func(p Packet) {
		got := requireType[*PubackPacket](t, p)
		if got.PacketID != pkt.PacketID {
			t.Errorf("PacketID = %d, want %d", got.PacketID, pkt.PacketID)
		}
	})
}

func TestPubrecPacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &PubrecPacket{PacketID: 100}
	assertRoundtrip(t, pkt, func(p Packet) {
		got := requireType[*PubrecPacket](t, p)
		if got.PacketID != pkt.PacketID {
			t.Errorf("PacketID = %d, want %d", got.PacketID, pkt.PacketID)
		}
	})
}

func TestPubrelPacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &PubrelPacket{PacketID: 200}
	assertRoundtrip(t, pkt, func(p Packet) {
		got := requireType[*PubrelPacket](t, p)
		if got.PacketID != pkt.PacketID {
			t.Errorf("PacketID = %d, want %d", got.PacketID, pkt.PacketID)
		}
	})
}

func TestPubcompPacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &PubcompPacket{PacketID: 300}
	assertRoundtrip(t, pkt, func(p Packet) {
		got := requireType[*PubcompPacket](t, p)
		if got.PacketID != pkt.PacketID {
			t.Errorf("PacketID = %d, want %d", got.PacketID, pkt.PacketID)
		}
	})
}

func TestSubscribePacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &SubscribePacket{
		PacketID: 1,
		Topics: []TopicSubscription{
			{Filter: "a/b/c", QoS: 0},
			{Filter: "d/+/f", QoS: 1},
			{Filter: "#", QoS: 2},
		},
	}

	assertRoundtrip(t, pkt, func(p Packet) {
		got := requireType[*SubscribePacket](t, p)
		if got.PacketID != pkt.PacketID {
			t.Errorf("PacketID = %d, want %d", got.PacketID, pkt.PacketID)
		}
		if len(got.Topics) != len(pkt.Topics) {
			t.Fatalf("Topics count = %d, want %d", len(got.Topics), len(pkt.Topics))
		}
		for i, ts := range got.Topics {
			if ts.Filter != pkt.Topics[i].Filter {
				t.Errorf("Topics[%d].Filter = %q, want %q", i, ts.Filter, pkt.Topics[i].Filter)
			}
			if ts.QoS != pkt.Topics[i].QoS {
				t.Errorf("Topics[%d].QoS = %d, want %d", i, ts.QoS, pkt.Topics[i].QoS)
			}
		}
	})
}

func TestSubackPacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &SubackPacket{
		PacketID:    1,
		ReturnCodes: []byte{0, 1, 2, 0x80},
	}

	assertRoundtrip(t, pkt, func(p Packet) {
		got := requireType[*SubackPacket](t, p)
		if got.PacketID != pkt.PacketID {
			t.Errorf("PacketID = %d, want %d", got.PacketID, pkt.PacketID)
		}
		if !bytes.Equal(got.ReturnCodes, pkt.ReturnCodes) {
			t.Errorf("ReturnCodes = %v, want %v", got.ReturnCodes, pkt.ReturnCodes)
		}
	})
}

func TestUnsubscribePacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &UnsubscribePacket{
		PacketID: 5,
		Topics:   []string{"a/b", "c/d"},
	}

	assertRoundtrip(t, pkt, func(p Packet) {
		got := requireType[*UnsubscribePacket](t, p)
		if got.PacketID != pkt.PacketID {
			t.Errorf("PacketID = %d, want %d", got.PacketID, pkt.PacketID)
		}
		if len(got.Topics) != len(pkt.Topics) {
			t.Fatalf("Topics count = %d, want %d", len(got.Topics), len(pkt.Topics))
		}
		for i, topic := range got.Topics {
			if topic != pkt.Topics[i] {
				t.Errorf("Topics[%d] = %q, want %q", i, topic, pkt.Topics[i])
			}
		}
	})
}

func TestUnsubackPacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &UnsubackPacket{PacketID: 7}
	assertRoundtrip(t, pkt, func(p Packet) {
		got := requireType[*UnsubackPacket](t, p)
		if got.PacketID != pkt.PacketID {
			t.Errorf("PacketID = %d, want %d", got.PacketID, pkt.PacketID)
		}
	})
}

func TestPingreqPacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &PingreqPacket{}
	assertRoundtrip(t, pkt, func(p Packet) {
		requireType[*PingreqPacket](t, p)
	})
}

func TestPingrespPacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &PingrespPacket{}
	assertRoundtrip(t, pkt, func(p Packet) {
		requireType[*PingrespPacket](t, p)
	})
}

func TestDisconnectPacketRoundtrip(t *testing.T) {
	t.Parallel()

	pkt := &DisconnectPacket{}
	assertRoundtrip(t, pkt, func(p Packet) {
		requireType[*DisconnectPacket](t, p)
	})
}

func TestReadPacketInvalidType(t *testing.T) {
	t.Parallel()

	// Type 0 is reserved and invalid.
	buf := bytes.NewBuffer([]byte{0x00, 0x00})
	_, err := ReadPacket(buf)
	if err == nil {
		t.Fatal("ReadPacket() expected error for invalid packet type")
	}
}

func TestTopicConversion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mqttTopic string
		amqpKey   string
	}{
		{name: "simple", mqttTopic: "a/b/c", amqpKey: "a.b.c"},
		{name: "single level wildcard", mqttTopic: "sensors/+/temperature", amqpKey: "sensors.*.temperature"},
		{name: "multi level wildcard", mqttTopic: "#", amqpKey: "#"},
		{name: "mixed wildcards", mqttTopic: "a/+/c/#", amqpKey: "a.*.c.#"},
		{name: "no separators", mqttTopic: "simple", amqpKey: "simple"},
		{name: "reverse simple", mqttTopic: "a/b/c", amqpKey: "a.b.c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MQTTTopicToAMQP(tt.mqttTopic)
			if got != tt.amqpKey {
				t.Errorf("MQTTTopicToAMQP(%q) = %q, want %q", tt.mqttTopic, got, tt.amqpKey)
			}

			back := AMQPTopicToMQTT(tt.amqpKey)
			if back != tt.mqttTopic {
				t.Errorf("AMQPTopicToMQTT(%q) = %q, want %q", tt.amqpKey, back, tt.mqttTopic)
			}
		})
	}
}

// requireType asserts that pkt is of type T and returns it, failing the test if not.
func requireType[T any](t *testing.T, pkt Packet) T {
	t.Helper()
	got, ok := pkt.(T)
	if !ok {
		t.Fatalf("got %T, want %T", pkt, got)
	}
	return got
}

// assertRoundtrip encodes a packet, decodes it, and calls check to verify the result.
func assertRoundtrip(t *testing.T, pkt Packet, check func(Packet)) {
	t.Helper()

	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("Encode() error: %v", err)
	}

	decoded, err := ReadPacket(&buf)
	if err != nil {
		t.Fatalf("ReadPacket() error: %v", err)
	}

	if decoded.Type() != pkt.Type() {
		t.Fatalf("Type() = %d, want %d", decoded.Type(), pkt.Type())
	}

	check(decoded)
}
