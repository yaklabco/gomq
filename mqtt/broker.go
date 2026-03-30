package mqtt

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yaklabco/gomq/auth"
	"github.com/yaklabco/gomq/broker"
	"github.com/yaklabco/gomq/storage"
)

// amqpTopicExchange is the AMQP exchange name used for MQTT message routing.
const amqpTopicExchange = "amq.topic"

// mqttQueuePrefix is prepended to the client ID to form the AMQP queue name.
const mqttQueuePrefix = "mqtt."

// deliveryLoopPoll is how often the delivery loop checks for new messages.
const deliveryLoopPoll = 50 * time.Millisecond

// subFailure is the SUBACK return code indicating subscription failure.
const subFailure = 0x80

// Broker bridges MQTT clients to the AMQP broker engine. It manages
// sessions, retained messages, and per-client queues bound to amq.topic.
type Broker struct {
	vhost    *broker.VHost
	users    *auth.UserStore
	sessions map[string]*Session
	retains  map[string]*PublishPacket
	mu       sync.RWMutex
}

// NewBroker creates an MQTT broker backed by the given VHost and UserStore.
func NewBroker(vhost *broker.VHost, users *auth.UserStore) *Broker {
	return &Broker{
		vhost:    vhost,
		users:    users,
		sessions: make(map[string]*Session),
		retains:  make(map[string]*PublishPacket),
	}
}

// ListenAndServe binds a TCP listener on addr and accepts MQTT connections
// until the context is cancelled.
func (b *Broker) ListenAndServe(ctx context.Context, addr string) (net.Addr, error) {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mqtt listen on %s: %w", addr, err)
	}

	go func() {
		<-ctx.Done()
		_ = ln.Close() //nolint:errcheck // closing listener on context cancel is best-effort
	}()

	go b.acceptLoop(ln) //nolint:contextcheck // accept loop manages its own lifecycle via listener close

	return ln.Addr(), nil
}

// acceptLoop accepts connections from the listener until it is closed.
func (b *Broker) acceptLoop(ln net.Listener) {
	for {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return // listener closed
		}
		go b.HandleConn(conn)
	}
}

// HandleConn processes a single MQTT client connection. It reads the
// CONNECT packet, authenticates, and enters the read loop.
func (b *Broker) HandleConn(conn net.Conn) {
	defer func() {
		_ = conn.Close() //nolint:errcheck // closing connection at end of handler
	}()

	// Read the first packet; must be CONNECT.
	pkt, err := ReadPacket(conn)
	if err != nil {
		return
	}

	connectPkt, ok := pkt.(*ConnectPacket)
	if !ok {
		sendBestEffort(conn, &ConnackPacket{ReturnCode: ConnackUnacceptableProto})
		return
	}

	// Authenticate.
	_, authErr := b.users.Authenticate(connectPkt.Username, string(connectPkt.Password))
	if authErr != nil {
		sendBestEffort(conn, &ConnackPacket{ReturnCode: ConnackBadCredentials})
		return
	}

	session, sessionPresent := b.getOrCreateSession(connectPkt)

	// Send CONNACK.
	connack := &ConnackPacket{
		ReturnCode:     ConnackAccepted,
		SessionPresent: sessionPresent && !connectPkt.CleanSession,
	}
	if encodeErr := connack.Encode(conn); encodeErr != nil {
		return
	}

	// Bind the connection to the session.
	session.bind(
		conn,
		connectPkt.WillTopic,
		connectPkt.WillMessage,
		connectPkt.WillQoS,
		connectPkt.WillRetain,
	)

	// Ensure the per-client AMQP queue exists and re-bind existing subscriptions.
	queueName := mqttQueuePrefix + connectPkt.ClientID
	b.ensureQueue(queueName, connectPkt.CleanSession)
	b.rebindSubscriptions(session, queueName)

	// Start delivery goroutine.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.deliverLoop(ctx, session, queueName)

	// Read loop.
	b.readLoop(session, conn, queueName)

	// Post-disconnect cleanup.
	if !session.isGraceful() {
		b.publishWill(session)
	}

	session.unbind()

	if session.clean {
		b.removeSession(connectPkt.ClientID)
		b.cleanupQueue(queueName)
	}
}

// sendBestEffort writes a packet to a connection, ignoring errors.
// Used for error responses where the connection is about to close anyway.
func sendBestEffort(conn net.Conn, pkt Packet) {
	_ = pkt.Encode(conn) //nolint:errcheck // best-effort error response before close
}

// getOrCreateSession retrieves or creates a session for the CONNECT packet.
func (b *Broker) getOrCreateSession(pkt *ConnectPacket) (*Session, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	existing, found := b.sessions[pkt.ClientID]

	if pkt.CleanSession {
		// Clean session: always create fresh.
		sess := newSession(pkt.ClientID, true)
		b.sessions[pkt.ClientID] = sess
		return sess, false
	}

	if found && !existing.clean {
		// Persistent session exists: reuse.
		return existing, true
	}

	// No persistent session: create new.
	sess := newSession(pkt.ClientID, false)
	b.sessions[pkt.ClientID] = sess
	return sess, false
}

// removeSession removes a session from the broker.
func (b *Broker) removeSession(clientID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.sessions, clientID)
}

// ensureQueue creates the MQTT per-client queue in the VHost if it does not exist.
func (b *Broker) ensureQueue(queueName string, clean bool) {
	//nolint:errcheck // queue may already exist; DeclareQueue handles idempotently
	_, _ = b.vhost.DeclareQueue(queueName, !clean, false, clean, nil)
}

// cleanupQueue deletes the MQTT queue for a clean session disconnect.
func (b *Broker) cleanupQueue(queueName string) {
	//nolint:errcheck // queue may already be gone; best-effort cleanup
	_, _ = b.vhost.DeleteQueue(queueName, false, false)
}

// rebindSubscriptions restores bindings for a persistent session's subscriptions.
func (b *Broker) rebindSubscriptions(sess *Session, queueName string) {
	for filter := range sess.filters() {
		amqpKey := MQTTTopicToAMQP(filter)
		//nolint:errcheck // best-effort rebind; subscription may already exist
		_ = b.vhost.BindQueue(queueName, amqpTopicExchange, amqpKey, nil)
	}
}

// readLoop processes packets from the client until the connection closes.
func (b *Broker) readLoop(sess *Session, conn net.Conn, queueName string) {
	for {
		pkt, err := ReadPacket(conn)
		if err != nil {
			return // connection closed or read error
		}

		switch typed := pkt.(type) {
		case *PublishPacket:
			b.handlePublish(sess, typed)
		case *PubackPacket:
			// QoS 1 ack from subscriber; currently fire-and-forget delivery.
		case *PubrecPacket:
			// QoS 2 step 1 response: we sent PUBLISH, client sends PUBREC.
			// Not used in our broker (we are the receiving side for QoS 2 from publishers).
		case *PubrelPacket:
			b.handlePubrel(sess, typed)
		case *PubcompPacket:
			// QoS 2 final ack; message delivery complete.
		case *SubscribePacket:
			b.handleSubscribe(sess, typed, queueName)
		case *UnsubscribePacket:
			b.handleUnsubscribe(sess, typed, queueName)
		case *PingreqPacket:
			_ = sess.send(&PingrespPacket{}) //nolint:errcheck // best-effort ping response
		case *DisconnectPacket:
			sess.markGraceful()
			return
		}
	}
}

// handlePublish processes an incoming PUBLISH from a client.
func (b *Broker) handlePublish(sess *Session, pkt *PublishPacket) {
	// Store retained message.
	if pkt.Retain {
		b.storeRetain(pkt)
	}

	// Convert MQTT topic to AMQP routing key and publish.
	amqpKey := MQTTTopicToAMQP(pkt.TopicName)
	msg := &storage.Message{
		Timestamp:    time.Now().UnixMilli(),
		ExchangeName: amqpTopicExchange,
		RoutingKey:   amqpKey,
		BodySize:     uint64(len(pkt.Payload)), //nolint:gosec // payload length is bounded
		Body:         pkt.Payload,
	}

	_ = b.vhost.Publish(amqpTopicExchange, amqpKey, false, msg) //nolint:errcheck // best-effort publish

	// QoS 1: send PUBACK.
	if pkt.QoS == 1 {
		_ = sess.send(&PubackPacket{PacketID: pkt.PacketID}) //nolint:errcheck // best-effort ack
	}

	// QoS 2: send PUBREC, track pending.
	if pkt.QoS == 2 {
		sess.addPendingQoS2(pkt.PacketID)
		_ = sess.send(&PubrecPacket{PacketID: pkt.PacketID}) //nolint:errcheck // best-effort QoS 2 flow
	}
}

// handlePubrel processes a PUBREL from a publisher (QoS 2 step 2).
func (b *Broker) handlePubrel(sess *Session, pkt *PubrelPacket) {
	sess.removePendingQoS2(pkt.PacketID)
	_ = sess.send(&PubcompPacket{PacketID: pkt.PacketID}) //nolint:errcheck // best-effort QoS 2 completion
}

// retainEntry pairs a topic filter with its QoS for deferred retained delivery.
type retainEntry struct {
	filter string
	qos    byte
}

// handleSubscribe processes a SUBSCRIBE packet.
func (b *Broker) handleSubscribe(sess *Session, pkt *SubscribePacket, queueName string) {
	returnCodes := make([]byte, len(pkt.Topics))
	retainDeliveries := make([]retainEntry, 0, len(pkt.Topics))

	for idx, ts := range pkt.Topics {
		sess.subscribe(ts.Filter, ts.QoS)

		amqpKey := MQTTTopicToAMQP(ts.Filter)
		if err := b.vhost.BindQueue(queueName, amqpTopicExchange, amqpKey, nil); err != nil {
			returnCodes[idx] = subFailure
			continue
		}

		returnCodes[idx] = ts.QoS
		retainDeliveries = append(retainDeliveries, retainEntry{filter: ts.Filter, qos: ts.QoS})
	}

	// Send SUBACK before retained messages per MQTT 3.1.1 spec.
	_ = sess.send(&SubackPacket{ //nolint:errcheck // best-effort suback
		PacketID:    pkt.PacketID,
		ReturnCodes: returnCodes,
	})

	// Deliver retained messages after SUBACK.
	for _, entry := range retainDeliveries {
		b.deliverRetained(sess, entry.filter, entry.qos)
	}
}

// handleUnsubscribe processes an UNSUBSCRIBE packet.
func (b *Broker) handleUnsubscribe(sess *Session, pkt *UnsubscribePacket, queueName string) {
	for _, topic := range pkt.Topics {
		sess.unsubscribe(topic)

		amqpKey := MQTTTopicToAMQP(topic)
		_ = b.vhost.UnbindQueue(queueName, amqpTopicExchange, amqpKey, nil) //nolint:errcheck // best-effort unbind
	}

	_ = sess.send(&UnsubackPacket{PacketID: pkt.PacketID}) //nolint:errcheck // best-effort unsuback
}

// storeRetain stores or clears a retained message.
func (b *Broker) storeRetain(pkt *PublishPacket) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(pkt.Payload) == 0 {
		delete(b.retains, pkt.TopicName)
		return
	}

	b.retains[pkt.TopicName] = pkt
}

// deliverRetained sends any matching retained messages to the session.
func (b *Broker) deliverRetained(sess *Session, filter string, qos byte) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for topic, pkt := range b.retains {
		if topicMatchesFilter(topic, filter) {
			deliverQoS := pkt.QoS
			if qos < deliverQoS {
				deliverQoS = qos
			}
			_ = sess.send(&PublishPacket{ //nolint:errcheck // best-effort retained delivery
				TopicName: pkt.TopicName,
				Payload:   pkt.Payload,
				QoS:       deliverQoS,
				Retain:    true,
			})
		}
	}
}

// publishWill publishes the Last Will and Testament message if configured.
func (b *Broker) publishWill(sess *Session) {
	sess.mu.Lock()
	willTopic := sess.willTopic
	willMessage := sess.willMessage
	willQoS := sess.willQoS
	willRetain := sess.willRetain
	sess.mu.Unlock()

	if willTopic == "" {
		return
	}

	willPub := &PublishPacket{
		TopicName: willTopic,
		Payload:   willMessage,
		QoS:       willQoS,
		Retain:    willRetain,
	}

	if willRetain {
		b.storeRetain(willPub)
	}

	amqpKey := MQTTTopicToAMQP(willTopic)
	msg := &storage.Message{
		Timestamp:    time.Now().UnixMilli(),
		ExchangeName: amqpTopicExchange,
		RoutingKey:   amqpKey,
		BodySize:     uint64(len(willMessage)), //nolint:gosec // payload length bounded
		Body:         willMessage,
	}

	_ = b.vhost.Publish(amqpTopicExchange, amqpKey, false, msg) //nolint:errcheck // best-effort will publish
}

// deliverLoop polls the per-client AMQP queue and forwards messages to the
// MQTT session as PUBLISH packets.
func (b *Broker) deliverLoop(ctx context.Context, sess *Session, queueName string) {
	ticker := time.NewTicker(deliveryLoopPoll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.drainQueue(sess, queueName)
		}
	}
}

// drainQueue reads all available messages from the named queue and sends
// them to the session as PUBLISH packets.
func (b *Broker) drainQueue(sess *Session, queueName string) {
	queue, ok := b.vhost.GetQueue(queueName)
	if !ok {
		return
	}

	for {
		env, found := queue.Get(true)
		if !found || env == nil {
			return
		}

		mqttTopic := AMQPTopicToMQTT(env.Message.RoutingKey)
		pub := &PublishPacket{
			TopicName: mqttTopic,
			Payload:   env.Message.Body,
			QoS:       0, // deliver at QoS 0 for simplicity; subscription QoS handled by broker
		}

		if err := sess.send(pub); err != nil {
			return
		}
	}
}

// topicMatchesFilter checks if an MQTT topic matches a subscription filter.
// Supports '+' (single-level) and '#' (multi-level) wildcards.
func topicMatchesFilter(topic, filter string) bool {
	// Exact match fast path.
	if topic == filter {
		return true
	}

	return matchSegments(splitTopic(topic), splitTopic(filter))
}

// splitTopic splits an MQTT topic by '/'.
func splitTopic(topic string) []string {
	if topic == "" {
		return nil
	}
	result := make([]string, 0, 4) //nolint:mnd // reasonable pre-alloc for typical MQTT topics
	start := 0
	for idx := range len(topic) {
		if topic[idx] == '/' {
			result = append(result, topic[start:idx])
			start = idx + 1
		}
	}
	result = append(result, topic[start:])
	return result
}

// matchSegments performs segment-by-segment matching of topic against filter.
func matchSegments(topicParts, filterParts []string) bool {
	ti, fi := 0, 0
	for fi < len(filterParts) {
		if filterParts[fi] == "#" {
			return true // '#' matches everything remaining
		}
		if ti >= len(topicParts) {
			return false // filter has more segments than topic
		}
		if filterParts[fi] != "+" && filterParts[fi] != topicParts[ti] {
			return false // segment mismatch
		}
		ti++
		fi++
	}
	return ti == len(topicParts)
}

// VHost returns the underlying VHost for cross-protocol integration.
func (b *Broker) VHost() *broker.VHost {
	return b.vhost
}
