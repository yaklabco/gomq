package mqtt_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/yaklabco/gomq/auth"
	"github.com/yaklabco/gomq/broker"
	"github.com/yaklabco/gomq/mqtt"
)

// testTimeout is the maximum time to wait for async operations in tests.
const testTimeout = 5 * time.Second

// newTestBroker creates a Broker backed by a temporary VHost and UserStore.
func newTestBroker(t *testing.T) *mqtt.Broker {
	t.Helper()

	dataDir := t.TempDir()

	vh, err := broker.NewVHost("/", dataDir)
	if err != nil {
		t.Fatalf("NewVHost: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := vh.Close(); closeErr != nil {
			t.Errorf("close vhost: %v", closeErr)
		}
	})

	users, err := auth.NewUserStore(dataDir)
	if err != nil {
		t.Fatalf("NewUserStore: %v", err)
	}

	return mqtt.NewBroker(vh, users)
}

// dialBroker creates a paired TCP connection for testing. It returns the
// "client" side connection and starts serving the "server" side on the broker.
func dialBroker(t *testing.T, brk *mqtt.Broker) net.Conn {
	t.Helper()

	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	go brk.HandleConn(server)

	return client
}

// mqttConnect performs an MQTT CONNECT handshake with guest/guest credentials.
func mqttConnect(t *testing.T, conn net.Conn, clientID string, clean bool) *mqtt.ConnackPacket {
	t.Helper()

	pkt := &mqtt.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		CleanSession:  clean,
		KeepAlive:     60,
		ClientID:      clientID,
		Username:      "guest",
		Password:      []byte("guest"),
	}

	if err := pkt.Encode(conn); err != nil {
		t.Fatalf("encode CONNECT: %v", err)
	}

	resp, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read CONNACK: %v", err)
	}

	connack, ok := resp.(*mqtt.ConnackPacket)
	if !ok {
		t.Fatalf("expected CONNACK, got %T", resp)
	}

	return connack
}

// mqttConnectWithCreds performs an MQTT CONNECT handshake with custom credentials.
func mqttConnectWithCreds(t *testing.T, conn net.Conn, clientID, username, password string, clean bool) *mqtt.ConnackPacket {
	t.Helper()

	pkt := &mqtt.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		CleanSession:  clean,
		KeepAlive:     60,
		ClientID:      clientID,
		Username:      username,
		Password:      []byte(password),
	}

	if err := pkt.Encode(conn); err != nil {
		t.Fatalf("encode CONNECT: %v", err)
	}

	resp, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read CONNACK: %v", err)
	}

	connack, ok := resp.(*mqtt.ConnackPacket)
	if !ok {
		t.Fatalf("expected CONNACK, got %T", resp)
	}

	return connack
}

// mqttSubscribe sends a SUBSCRIBE with packet ID 1 and reads the SUBACK.
func mqttSubscribe(t *testing.T, conn net.Conn, topics []mqtt.TopicSubscription) *mqtt.SubackPacket {
	t.Helper()

	sub := &mqtt.SubscribePacket{
		PacketID: 1,
		Topics:   topics,
	}
	if err := sub.Encode(conn); err != nil {
		t.Fatalf("encode SUBSCRIBE: %v", err)
	}

	resp, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read SUBACK: %v", err)
	}

	suback, ok := resp.(*mqtt.SubackPacket)
	if !ok {
		t.Fatalf("expected SUBACK, got %T", resp)
	}

	return suback
}

// readPublish reads a PUBLISH packet from conn with a timeout.
func readPublish(t *testing.T, conn net.Conn) *mqtt.PublishPacket {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			t.Fatalf("clear read deadline: %v", err)
		}
	}()

	pkt, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read PUBLISH: %v", err)
	}

	pub, ok := pkt.(*mqtt.PublishPacket)
	if !ok {
		t.Fatalf("expected PUBLISH, got %T", pkt)
	}

	return pub
}

func TestBroker_ConnectAccepted(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)
	conn := dialBroker(t, brk)

	connack := mqttConnect(t, conn, "client-1", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Errorf("ReturnCode = %d, want %d", connack.ReturnCode, mqtt.ConnackAccepted)
	}
}

func TestBroker_ConnectBadCredentials(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)
	conn := dialBroker(t, brk)

	connack := mqttConnectWithCreds(t, conn, "client-1", "guest", "wrong", true)
	if connack.ReturnCode != mqtt.ConnackBadCredentials {
		t.Errorf("ReturnCode = %d, want %d", connack.ReturnCode, mqtt.ConnackBadCredentials)
	}
}

func TestBroker_SubscribeSuback(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)
	conn := dialBroker(t, brk)

	connack := mqttConnect(t, conn, "sub-client", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("CONNECT failed: %d", connack.ReturnCode)
	}

	suback := mqttSubscribe(t, conn, []mqtt.TopicSubscription{
		{Filter: "a/b/c", QoS: 0},
		{Filter: "d/+/f", QoS: 1},
	})

	if suback.PacketID != 1 {
		t.Errorf("SUBACK PacketID = %d, want 1", suback.PacketID)
	}
	if len(suback.ReturnCodes) != 2 {
		t.Fatalf("SUBACK ReturnCodes len = %d, want 2", len(suback.ReturnCodes))
	}
	if suback.ReturnCodes[0] != 0 {
		t.Errorf("ReturnCodes[0] = %d, want 0", suback.ReturnCodes[0])
	}
	if suback.ReturnCodes[1] != 1 {
		t.Errorf("ReturnCodes[1] = %d, want 1", suback.ReturnCodes[1])
	}
}

func TestBroker_PublishQoS0(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)

	// Subscriber.
	sub := dialBroker(t, brk)
	connack := mqttConnect(t, sub, "sub-client", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("sub CONNECT failed: %d", connack.ReturnCode)
	}
	mqttSubscribe(t, sub, []mqtt.TopicSubscription{
		{Filter: "test/topic", QoS: 0},
	})

	// Publisher.
	pub := dialBroker(t, brk)
	connack = mqttConnect(t, pub, "pub-client", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("pub CONNECT failed: %d", connack.ReturnCode)
	}

	// Publish a QoS 0 message.
	pubPkt := &mqtt.PublishPacket{
		TopicName: "test/topic",
		QoS:       0,
		Payload:   []byte("hello"),
	}
	if err := pubPkt.Encode(pub); err != nil {
		t.Fatalf("encode PUBLISH: %v", err)
	}

	// Read the forwarded message on the subscriber.
	received := readPublish(t, sub)
	if received.TopicName != "test/topic" {
		t.Errorf("TopicName = %q, want %q", received.TopicName, "test/topic")
	}
	if !bytes.Equal(received.Payload, []byte("hello")) {
		t.Errorf("Payload = %q, want %q", received.Payload, "hello")
	}
}

func TestBroker_PublishQoS1WithPuback(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)

	// Subscriber.
	sub := dialBroker(t, brk)
	connack := mqttConnect(t, sub, "sub-qos1", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("sub CONNECT failed: %d", connack.ReturnCode)
	}
	mqttSubscribe(t, sub, []mqtt.TopicSubscription{
		{Filter: "qos1/topic", QoS: 1},
	})

	// Publisher.
	pub := dialBroker(t, brk)
	connack = mqttConnect(t, pub, "pub-qos1", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("pub CONNECT failed: %d", connack.ReturnCode)
	}

	// Publish a QoS 1 message.
	pubPkt := &mqtt.PublishPacket{
		TopicName: "qos1/topic",
		QoS:       1,
		PacketID:  10,
		Payload:   []byte("ack me"),
	}
	if err := pubPkt.Encode(pub); err != nil {
		t.Fatalf("encode PUBLISH: %v", err)
	}

	// Publisher should receive PUBACK.
	if err := pub.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	ackPkt, err := mqtt.ReadPacket(pub)
	if err != nil {
		t.Fatalf("read PUBACK: %v", err)
	}
	puback, ok := ackPkt.(*mqtt.PubackPacket)
	if !ok {
		t.Fatalf("expected PUBACK, got %T", ackPkt)
	}
	if puback.PacketID != 10 {
		t.Errorf("PUBACK PacketID = %d, want 10", puback.PacketID)
	}

	// Subscriber should receive the published message.
	received := readPublish(t, sub)
	if !bytes.Equal(received.Payload, []byte("ack me")) {
		t.Errorf("Payload = %q, want %q", received.Payload, "ack me")
	}
}

func TestBroker_RetainMessage(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)

	// Publisher publishes a retained message.
	pub := dialBroker(t, brk)
	connack := mqttConnect(t, pub, "retain-pub", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("CONNECT failed: %d", connack.ReturnCode)
	}

	retainPkt := &mqtt.PublishPacket{
		TopicName: "retain/topic",
		QoS:       0,
		Retain:    true,
		Payload:   []byte("retained"),
	}
	if err := retainPkt.Encode(pub); err != nil {
		t.Fatalf("encode PUBLISH: %v", err)
	}

	// Brief wait for message to be processed before subscribing.
	time.Sleep(50 * time.Millisecond)

	// New subscriber should receive the retained message.
	sub := dialBroker(t, brk)
	connack = mqttConnect(t, sub, "retain-sub", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("CONNECT failed: %d", connack.ReturnCode)
	}

	mqttSubscribe(t, sub, []mqtt.TopicSubscription{
		{Filter: "retain/topic", QoS: 0},
	})

	received := readPublish(t, sub)
	if !bytes.Equal(received.Payload, []byte("retained")) {
		t.Errorf("Payload = %q, want %q", received.Payload, "retained")
	}
	if !received.Retain {
		t.Error("expected retained flag to be set")
	}
}

func TestBroker_WildcardSubscription(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)

	// Subscriber subscribes to wildcard.
	sub := dialBroker(t, brk)
	connack := mqttConnect(t, sub, "wild-sub", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("CONNECT failed: %d", connack.ReturnCode)
	}
	mqttSubscribe(t, sub, []mqtt.TopicSubscription{
		{Filter: "sensors/+/temp", QoS: 0},
	})

	// Publisher publishes to a matching topic.
	pub := dialBroker(t, brk)
	connack = mqttConnect(t, pub, "wild-pub", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("CONNECT failed: %d", connack.ReturnCode)
	}

	pubPkt := &mqtt.PublishPacket{
		TopicName: "sensors/room1/temp",
		QoS:       0,
		Payload:   []byte("22.5"),
	}
	if err := pubPkt.Encode(pub); err != nil {
		t.Fatalf("encode PUBLISH: %v", err)
	}

	received := readPublish(t, sub)
	if received.TopicName != "sensors/room1/temp" {
		t.Errorf("TopicName = %q, want %q", received.TopicName, "sensors/room1/temp")
	}
}

func TestBroker_CleanSession(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)

	// First connection with clean=false.
	conn1 := dialBroker(t, brk)
	connack := mqttConnect(t, conn1, "persist-client", false)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("CONNECT failed: %d", connack.ReturnCode)
	}
	if connack.SessionPresent {
		t.Error("first connection should not have session present")
	}

	mqttSubscribe(t, conn1, []mqtt.TopicSubscription{
		{Filter: "persist/topic", QoS: 0},
	})

	// Disconnect gracefully.
	disc := &mqtt.DisconnectPacket{}
	if err := disc.Encode(conn1); err != nil {
		t.Fatalf("encode DISCONNECT: %v", err)
	}
	_ = conn1.Close()

	// Brief wait for disconnect processing.
	time.Sleep(50 * time.Millisecond)

	// Reconnect with clean=false; session should be present.
	conn2 := dialBroker(t, brk)
	connack = mqttConnect(t, conn2, "persist-client", false)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("CONNECT failed: %d", connack.ReturnCode)
	}
	if !connack.SessionPresent {
		t.Error("reconnection should have session present")
	}
}

func TestBroker_Pingreq(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)
	conn := dialBroker(t, brk)

	connack := mqttConnect(t, conn, "ping-client", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("CONNECT failed: %d", connack.ReturnCode)
	}

	ping := &mqtt.PingreqPacket{}
	if err := ping.Encode(conn); err != nil {
		t.Fatalf("encode PINGREQ: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	resp, err := mqtt.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read PINGRESP: %v", err)
	}
	if _, ok := resp.(*mqtt.PingrespPacket); !ok {
		t.Fatalf("expected PINGRESP, got %T", resp)
	}
}

func TestBroker_MultipleSubscribers(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)

	const numSubs = 3
	subs := make([]net.Conn, numSubs)
	for idx := range subs {
		subs[idx] = dialBroker(t, brk)
		connack := mqttConnect(t, subs[idx], clientID(idx), true)
		if connack.ReturnCode != mqtt.ConnackAccepted {
			t.Fatalf("sub %d CONNECT failed: %d", idx, connack.ReturnCode)
		}
		mqttSubscribe(t, subs[idx], []mqtt.TopicSubscription{
			{Filter: "broadcast", QoS: 0},
		})
	}

	// Publisher.
	pub := dialBroker(t, brk)
	connack := mqttConnect(t, pub, "multi-pub", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("pub CONNECT failed: %d", connack.ReturnCode)
	}

	pubPkt := &mqtt.PublishPacket{
		TopicName: "broadcast",
		QoS:       0,
		Payload:   []byte("fanout"),
	}
	if err := pubPkt.Encode(pub); err != nil {
		t.Fatalf("encode PUBLISH: %v", err)
	}

	// All subscribers should receive the message.
	var wg sync.WaitGroup
	for idx, subConn := range subs {
		wg.Add(1)
		go func(subIdx int, conn net.Conn) {
			defer wg.Done()
			received := readPublish(t, conn)
			if !bytes.Equal(received.Payload, []byte("fanout")) {
				t.Errorf("sub %d: Payload = %q, want %q", subIdx, received.Payload, "fanout")
			}
		}(idx, subConn)
	}
	wg.Wait()
}

// clientID generates a unique client ID for tests using multiple subscribers.
func clientID(idx int) string {
	return fmt.Sprintf("sub-%d", idx)
}

func TestBroker_QoS2Flow(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)

	// Subscriber.
	sub := dialBroker(t, brk)
	connack := mqttConnect(t, sub, "qos2-sub", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("sub CONNECT failed: %d", connack.ReturnCode)
	}
	mqttSubscribe(t, sub, []mqtt.TopicSubscription{
		{Filter: "qos2/topic", QoS: 2},
	})

	// Publisher.
	pub := dialBroker(t, brk)
	connack = mqttConnect(t, pub, "qos2-pub", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("pub CONNECT failed: %d", connack.ReturnCode)
	}

	// Publish QoS 2 message.
	pubPkt := &mqtt.PublishPacket{
		TopicName: "qos2/topic",
		QoS:       2,
		PacketID:  20,
		Payload:   []byte("exactly once"),
	}
	if err := pubPkt.Encode(pub); err != nil {
		t.Fatalf("encode PUBLISH: %v", err)
	}

	// Publisher should receive PUBREC.
	if err := pub.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	recPkt, err := mqtt.ReadPacket(pub)
	if err != nil {
		t.Fatalf("read PUBREC: %v", err)
	}
	pubrec, ok := recPkt.(*mqtt.PubrecPacket)
	if !ok {
		t.Fatalf("expected PUBREC, got %T", recPkt)
	}
	if pubrec.PacketID != 20 {
		t.Errorf("PUBREC PacketID = %d, want 20", pubrec.PacketID)
	}

	// Publisher sends PUBREL.
	pubrel := &mqtt.PubrelPacket{PacketID: 20}
	if err := pubrel.Encode(pub); err != nil {
		t.Fatalf("encode PUBREL: %v", err)
	}

	// Publisher should receive PUBCOMP.
	compPkt, err := mqtt.ReadPacket(pub)
	if err != nil {
		t.Fatalf("read PUBCOMP: %v", err)
	}
	pubcomp, ok := compPkt.(*mqtt.PubcompPacket)
	if !ok {
		t.Fatalf("expected PUBCOMP, got %T", compPkt)
	}
	if pubcomp.PacketID != 20 {
		t.Errorf("PUBCOMP PacketID = %d, want 20", pubcomp.PacketID)
	}

	// Subscriber should receive the message.
	received := readPublish(t, sub)
	if !bytes.Equal(received.Payload, []byte("exactly once")) {
		t.Errorf("Payload = %q, want %q", received.Payload, "exactly once")
	}
}

func TestBroker_WillMessage(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)

	// Subscriber listens on the will topic.
	sub := dialBroker(t, brk)
	connack := mqttConnect(t, sub, "will-sub", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("sub CONNECT failed: %d", connack.ReturnCode)
	}
	mqttSubscribe(t, sub, []mqtt.TopicSubscription{
		{Filter: "will/topic", QoS: 0},
	})

	// Client connects with a will message, then disconnects ungracefully.
	willConn, willServer := net.Pipe()
	t.Cleanup(func() {
		_ = willConn.Close()
		_ = willServer.Close()
	})

	go brk.HandleConn(willServer)

	willPkt := &mqtt.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		CleanSession:  true,
		KeepAlive:     60,
		ClientID:      "will-client",
		Username:      "guest",
		Password:      []byte("guest"),
		WillTopic:     "will/topic",
		WillMessage:   []byte("I died"),
		WillQoS:       0,
	}

	if err := willPkt.Encode(willConn); err != nil {
		t.Fatalf("encode will CONNECT: %v", err)
	}

	willConnack, err := mqtt.ReadPacket(willConn)
	if err != nil {
		t.Fatalf("read will CONNACK: %v", err)
	}
	if wc, ok := willConnack.(*mqtt.ConnackPacket); !ok || wc.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("will CONNECT failed")
	}

	// Ungraceful disconnect (close without DISCONNECT packet).
	_ = willConn.Close()

	// Subscriber should receive the will message.
	received := readPublish(t, sub)
	if !bytes.Equal(received.Payload, []byte("I died")) {
		t.Errorf("Will Payload = %q, want %q", received.Payload, "I died")
	}
}

// TestBroker_NoPublishAfterDisconnect verifies that a graceful DISCONNECT
// suppresses the will message.
func TestBroker_NoWillOnGracefulDisconnect(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)

	// Subscriber listens on the will topic.
	sub := dialBroker(t, brk)
	connack := mqttConnect(t, sub, "will-sub-graceful", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("sub CONNECT failed: %d", connack.ReturnCode)
	}
	mqttSubscribe(t, sub, []mqtt.TopicSubscription{
		{Filter: "will/graceful", QoS: 0},
	})

	// Client connects with a will message, then disconnects gracefully.
	willConn, willServer := net.Pipe()
	t.Cleanup(func() {
		_ = willConn.Close()
		_ = willServer.Close()
	})

	go brk.HandleConn(willServer)

	willPkt := &mqtt.ConnectPacket{
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		CleanSession:  true,
		KeepAlive:     60,
		ClientID:      "will-graceful",
		Username:      "guest",
		Password:      []byte("guest"),
		WillTopic:     "will/graceful",
		WillMessage:   []byte("should not arrive"),
		WillQoS:       0,
	}

	if err := willPkt.Encode(willConn); err != nil {
		t.Fatalf("encode will CONNECT: %v", err)
	}

	willConnack, err := mqtt.ReadPacket(willConn)
	if err != nil {
		t.Fatalf("read will CONNACK: %v", err)
	}
	if wc, ok := willConnack.(*mqtt.ConnackPacket); !ok || wc.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("will CONNECT failed")
	}

	// Graceful disconnect.
	disc := &mqtt.DisconnectPacket{}
	if err := disc.Encode(willConn); err != nil {
		t.Fatalf("encode DISCONNECT: %v", err)
	}
	_ = willConn.Close()

	// Wait and verify no message arrives.
	if err := sub.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, readErr := mqtt.ReadPacket(sub)
	if readErr == nil {
		t.Error("expected no message after graceful disconnect, but got one")
	}
}

func TestBroker_Unsubscribe(t *testing.T) {
	t.Parallel()

	brk := newTestBroker(t)

	// Subscriber.
	sub := dialBroker(t, brk)
	connack := mqttConnect(t, sub, "unsub-client", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("CONNECT failed: %d", connack.ReturnCode)
	}
	mqttSubscribe(t, sub, []mqtt.TopicSubscription{
		{Filter: "unsub/topic", QoS: 0},
	})

	// Unsubscribe.
	unsubPkt := &mqtt.UnsubscribePacket{
		PacketID: 2,
		Topics:   []string{"unsub/topic"},
	}
	if err := unsubPkt.Encode(sub); err != nil {
		t.Fatalf("encode UNSUBSCRIBE: %v", err)
	}

	if err := sub.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	unsubackPkt, err := mqtt.ReadPacket(sub)
	if err != nil {
		t.Fatalf("read UNSUBACK: %v", err)
	}
	unsuback, ok := unsubackPkt.(*mqtt.UnsubackPacket)
	if !ok {
		t.Fatalf("expected UNSUBACK, got %T", unsubackPkt)
	}
	if unsuback.PacketID != 2 {
		t.Errorf("UNSUBACK PacketID = %d, want 2", unsuback.PacketID)
	}

	// Now publish; subscriber should NOT receive it.
	pub := dialBroker(t, brk)
	connack = mqttConnect(t, pub, "unsub-pub", true)
	if connack.ReturnCode != mqtt.ConnackAccepted {
		t.Fatalf("pub CONNECT failed: %d", connack.ReturnCode)
	}
	pubPkt := &mqtt.PublishPacket{
		TopicName: "unsub/topic",
		QoS:       0,
		Payload:   []byte("should not arrive"),
	}
	if err := pubPkt.Encode(pub); err != nil {
		t.Fatalf("encode PUBLISH: %v", err)
	}

	if err := sub.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, readErr := mqtt.ReadPacket(sub)
	// We expect a timeout (no message).
	if readErr == nil {
		t.Error("expected no message after unsubscribe, but got one")
	}
	// Verify it's a deadline error, not something else.
	var netErr net.Error
	if !errors.As(readErr, &netErr) {
		// Could be io.EOF from pipe close or timeout - both are acceptable.
		if !errors.Is(readErr, io.EOF) {
			t.Errorf("unexpected error type: %v", readErr)
		}
	}
}
